use super::super::std::sync::{Arc, Mutex};
use super::super::std::sync::atomic::AtomicU8;
use super::super::std::sync::atomic::AtomicU32;
use super::super::std::sync::atomic::AtomicUsize;
use super::super::std::sync::atomic::Ordering;
use super::super::volatile_cell::VolatileCell;
use super::super::monitor::Monitor;

// lock-free bitfield where each bit is the emptyness status of a queue
// by storing them in a single atomic bitfield, the collective emptiness status of the queues
// can be observed efficiently and lock-free
struct StatusField {
    seperate: [VolatileCell<bool>; 32],
    compound: AtomicU32
}
impl StatusField {
    fn new() -> StatusField {
        StatusField {
            seperate: [VolatileCell::new(false); 32],
            compound: AtomicU32::new(0x00000000)
        }
    }

    fn set_bit(&self, i: usize, b: bool) {
        let before = self.seperate[i].get();
        if before ^ b {
            self.seperate[i].set(b);
            if b {
                let mask = 0x1 << i;
                // TODO: there are literally single instructions for this
                loop {
                    let prev = self.compound.load(Ordering::Acquire);
                    let next = prev | mask;
                    if self.compound.compare_and_swap(prev, next, Ordering::SeqCst) == prev {
                        break;
                    }
                }
            } else {
                let mask = !(0x1 << i);
                loop {
                    let prev = self.compound.load(Ordering::Acquire);
                    let next = prev & mask;
                    if self.compound.compare_and_swap(prev, next, Ordering::SeqCst) == prev {
                        break;
                    }
                }
            }
        }
    }

    fn get_field(&self) -> u32 {
        self.compound.load(Ordering::Acquire)
    }
}

pub mod smart_pool {
    use super::*;
    use ::threading::queue::*;
    use ::threading::queue::QueueOut;
    use ::std::thread;
    use ::std::boxed::*;
    use ::std::usize;
    use ::threading::profile::*;
    use ::threading::profile::ThreadStateLogger;
    use ::time::Duration;

    pub type Runnable = Box<FnBox() -> () + Send + Sync + 'static>;

    pub struct Config {
        pub thread_count: u32,
        //pub main_count: usize,
        //pub crit_count: usize,
    }
    // TODO: implement default for config

    const STATUS_UNSTARTED: u8 = 0;
    const STATUS_RUNNING: u8 = 1;
    const STATUS_CLOSED: u8 = 2;

    #[derive(Clone)]
    #[allow(non_camel_case_types)]
    pub enum Exec {
        Foreground(Arc<Pool>),
        Main(Arc<Pool>),
        Crit(Arc<Pool>),
        Cheap,
    }
    impl Exec {
        pub fn exec(&self, task: Runnable) {
            match self.clone() {
                Exec::Foreground(pool) => {
                    pool.foreground.input.push(task, &mut ThreadStateIgnorer);
                    pool.after_add();
                },
                Exec::Main(pool) => {
                    pool.in_main_seq.push(task, &mut ThreadStateIgnorer);
                    pool.after_add();
                },
                Exec::Crit(pool) => {
                    pool.in_crit_seq.push(task, &mut ThreadStateIgnorer);
                    pool.after_add();
                },
                Exec::Cheap => task.call_box(()),
            }
        }
    }

    pub struct Pool {
        config: Config,

        lifetime_status: Arc<AtomicU8>,

        index: Arc<AtomicUsize>, // TODO: rename to poll_index
        status: Arc<StatusField>,
        notif: Arc<AtomicU32>,
        monitor: Arc<Monitor<()>>,

        foreground: Arc<LinearQueue<Runnable>>,

        in_main_seq: Arc<QueueIn<Runnable>>,
        in_crit_seq: Arc<QueueIn<Runnable>>,

        out: Vec<Box<QueueOut<Runnable>>>,

        profilers: Vec<Arc<ThreadStateProfiler>>,

        workers: Mutex<Vec<thread::JoinHandle<()>>>
    }

    impl Pool {
        pub fn start_new(config: Config) -> Arc<Pool> {
            let mut pool = Pool::new(config);
            pool.start();
            Arc::new(pool)
        }

        fn new(config: Config) -> Pool {
            // check parameters
            assert!(config.thread_count >= 1, "pool must have at least 1 thread");

            // create fields
            let lifetime_status = Arc::new(AtomicU8::new(STATUS_UNSTARTED));

            let status = Arc::new(StatusField::new());
            fn set_status(status: &Arc<StatusField>, i: usize) -> impl Fn(bool) -> () + Send + Sync + 'static {
                let status = status.clone();
                move |b: bool| {
                    status.set_bit(i, b);
                }
            }

            let index = Arc::new(AtomicUsize::new(0));
            let notif = Arc::new(AtomicU32::new(0));
            let monitor = Arc::new(Monitor::new(()));

            let foreground = LinearQueue::<Runnable>::new(set_status(&status, 0));

            let main_seq = LinearQueue::<Runnable>::new(set_status(&status, 1));
            let crit_seq = LinearQueue::<Runnable>::new(set_status(&status, 4));

            let out: Vec<Box<QueueOut<Runnable>>> = vec!(
                Box::new(main_seq.output.clone()),
                Box::new(crit_seq.output.clone()),
            );

            // construct pool
            Pool {
                config,
                lifetime_status,
                index,
                status,
                notif,
                monitor,
                foreground: Arc::new(foreground),
                in_main_seq: Arc::new(main_seq.input.clone()),
                in_crit_seq: Arc::new(crit_seq.input.clone()),
                out,
                profilers: Vec::new(),
                workers: Mutex::new(Vec::new())
            }
        }

        fn start(&mut self) {
            // set the status and assert that we're only starting once
            let status_before = self.lifetime_status.compare_and_swap(STATUS_UNSTARTED, STATUS_RUNNING, Ordering::SeqCst);
            assert_eq!(status_before, STATUS_UNSTARTED, "starting pool requires it to be in unstarted state");

            let profile_dur = Duration::seconds(5);

            // lock to the worker vector
            let mut workers = self.workers.lock().unwrap();

            for _ in 0usize..(self.config.thread_count as usize) {
                // clone data to move into new thread
                let lifetime_status = self.lifetime_status.clone();
                let foreground_out = self.foreground.output.clone();
                let index = self.index.clone();
                let out = self.out.iter()
                    .map(|queue| queue.clone_box())
                    .collect::<Vec<Box<QueueOut<Runnable>>>>();
                let status = self.status.clone();
                let monitor = self.monitor.clone();
                let notif = self.notif.clone();
                let profiler = Arc::new(ThreadStateProfiler::start_new(
                    ThreadState::Overhead,
                    profile_dur
                ));
                self.profilers.push(profiler.clone());

                // create new thread
                let handle = thread::spawn(move || {
                    // until pool closed
                    'work_loop: while lifetime_status.load(Ordering::SeqCst) == STATUS_RUNNING {
                        // first, run foreground queue
                        loop {
                            match foreground_out.poll(&*profiler) {
                                Some(task) => {
                                    profiler.enter(ThreadState::Work);
                                    task.call_box(());
                                    profiler.enter(ThreadState::Overhead);
                                },
                                None => break
                            };
                        }

                        // then, run background queues
                        // get the rolling index
                        let i = index.fetch_add(1, Ordering::SeqCst);
                        // cycle index around if it's too high
                        if i > usize::MAX / 2 {
                            index.store(0, Ordering::SeqCst);
                        }
                        // poll that queue and handle result
                        match out[i % out.len()].poll(&*profiler) {
                            Some(task) => {
                                profiler.enter(ThreadState::Work);
                                task.call_box(());
                                profiler.enter(ThreadState::Overhead);
                            },
                            None => {
                                // handle case that all queues are empty
                                if status.get_field() == 0 {
                                    // raise notif counter before checking again
                                    notif.fetch_add(1, Ordering::SeqCst);
                                    if status.get_field() == 0 {
                                        // wait until the pool is closed or a queue is non-empty
                                        // using the monitor
                                        // we raised the notif counter and rechecked to ensure
                                        // we will get notified and avoid deadlock
                                        profiler.enter(ThreadState::Sleep);
                                        monitor.with_lock(|mut guard| {
                                            while lifetime_status.load(Ordering::SeqCst) == STATUS_RUNNING &&
                                                status.get_field() == 0 {
                                                guard.wait();
                                            }
                                        });
                                        profiler.enter(ThreadState::Overhead);
                                    }
                                    // if the pool is closed, break the loop
                                    if lifetime_status.load(Ordering::SeqCst) == STATUS_CLOSED {
                                        break 'work_loop;
                                    }
                                    // once that's handled, lower notif counter
                                    notif.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                        };
                    }
                });
                // add the worker vector
                workers.push(handle);
            }
        }

        fn after_add(&self) {
            // after adding to a queue, if a thread is wanting to be notified
            // due to being asleep due to queues being empty
            // notify the monitor
            if self.notif.load(Ordering::Acquire) > 0 {
                self.monitor.with_lock(|guard| guard.notify_all());
            }
        }

        pub fn close(&self) {
            // set status and assert that we're only closing once
            // this will cause threads to die, but not wake them up
            let status_before = self.lifetime_status.compare_and_swap(STATUS_RUNNING, STATUS_CLOSED, Ordering::SeqCst);
            assert_eq!(status_before, STATUS_RUNNING, "closing pool requires it to be in running state");

            // notify the monitor, which will wake up sleeping threads
            // then, they will die
            self.monitor.with_lock(|guard| {
                guard.notify_all();
            });

            // join all worker threads
            let mut workers = self.workers.lock().unwrap();
            while !workers.is_empty() {
                workers.swap_remove(0).join().expect("pool thread failed");
            }
        }
    }

    pub trait ArcPool {
        fn foreground(&self) -> Exec;
        fn exec(&self) -> Exec;
        fn crit(&self) -> Exec;
    }

    impl ArcPool for Arc<Pool> {
        fn foreground(&self) -> Exec {
            Exec::Foreground(self.clone())
        }

        fn exec(&self) -> Exec {
            Exec::Main(self.clone())
        }

        fn crit(&self) -> Exec {
            Exec::Crit(self.clone())
        }
    }
}

#[cfg(test)]
mod pool_test {
    use super::*;
    use super::smart_pool::*;
    use ::stopwatch::Stopwatch;
    use ::rand;
    use rand::Rng;
    use ::std::thread;
    use ::std::time::Duration;

    fn gen_test<E>(thread_count: u32, to_exec: E)
        where E: Fn(Arc<Pool>) -> Exec {
        let config = Config {
            thread_count
        };
        let pool = Pool::start_new(config);

        let sum = Arc::new(Monitor::new(0));

        for _ in 0..100 {
            let sum = sum.clone();
            to_exec(pool.clone()).exec(Box::new(move || {
                thread::sleep(Duration::from_millis(10));
                sum.with_lock(|mut guard| {
                    *guard += 1;
                    guard.notify_all();
                })
            }));
        }

        let result = sum.with_lock(|mut guard| {
            let sw = Stopwatch::start_new();
            while sw.elapsed_ms() < 2000 && *guard < 100 {
                guard.wait_timeout(Duration::from_millis(500));
            }
            if sw.elapsed_ms() >= 2000 {
                None
            } else {
                Some(*guard)
            }
        });

        assert!(result.is_some(), "test timed out");

        let result = result.unwrap();

        assert_eq!(result, 100);

        pool.close();
    }

    #[test]
    fn test_linear() {
        gen_test(4, |pool| pool.exec());
    }

    #[test]
    fn test_linear_crit() {
        gen_test(4, |pool| pool.crit());
    }

    #[test]
    fn test_foreground() {
        gen_test(4, |pool| pool.foreground());
    }

    #[test]
    fn test_mixed() {
        gen_test(7, |pool| {
            let mut rng = rand::thread_rng();
            match rng.next_u32() % 3 {
                0 => pool.foreground(),
                1 => pool.exec(),
                2 => pool.crit(),
                _ => unreachable!()
            }
        });
    }
}

pub mod pool_profile {
    use super::*;
    use super::smart_pool::*;
    use ::stopwatch::Stopwatch;
    use ::time;

    // this profile may be vulnerable to timeslicing
    pub fn profile_a() {
        for thread_count in 1..33 {
            let config = Config {
                thread_count
            };
            let pool = Pool::start_new(config);

            let sum = Arc::new(Monitor::new(0));

            let profiler = Stopwatch::start_new();

            for _ in 0..100 {
                let sum = sum.clone();
                pool.exec().exec(Box::new(move || {
                    // busy wait for 10 ms to simulate CPU work
                    let sw = Stopwatch::start_new();
                    while sw.elapsed_ms() < 10 {}
                    // lock and change
                    sum.with_lock(|mut guard| {
                        *guard += 1;
                        guard.notify_all();
                    })
                }));
            }

            let result = sum.with_lock(|mut guard| {
                while *guard < 100 {
                    guard.wait();
                }
                *guard
            });

            assert_eq!(result, 100);

            println!("{} threads completed in {:?} ms", thread_count, profiler.elapsed_ms());

            pool.close();
        }
    }

    // this version is meant to be not vulnerable to timeslicing
    // but it may be vulnerable to compiler optimization
    // and cache warmups
    pub fn profile_b() {
        let ms = 10;
        let mut cycles = 0;
        let end = ms * 1000000 + time::precise_time_ns();
        while time::precise_time_ns() < end {
            cycles += 1;
        }

        fn waste(cycles: u32) {
            let mut counter = 0;
            while counter < cycles {
                time::precise_time_ns();
                counter += 1;
            }
        }

        for thread_count in 1..33 {
            let config = Config {
                thread_count
            };
            let pool = Pool::start_new(config);

            let sum = Arc::new(Monitor::new(0));

            let profiler = Stopwatch::start_new();

            for _ in 0..100 {
                let sum = sum.clone();
                pool.exec().exec(Box::new(move || {
                    // busy wait for 10 ms to simulate CPU work
                    waste(cycles);
                    // lock and change
                    sum.with_lock(|mut guard| {
                        *guard += 1;
                        guard.notify_all();
                    })
                }));
            }

            let result = sum.with_lock(|mut guard| {
                while *guard < 100 {
                    guard.wait();
                }
                *guard
            });

            assert_eq!(result, 100);

            println!("{} threads completed in {:?} ms", thread_count, profiler.elapsed_ms());

            pool.close();
        }
    }
}
