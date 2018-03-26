use ::std::collections::VecDeque;
use ::time::Duration;
use ::time::precise_time_ns;
use ::std::fmt::*;
use ::std::sync::Mutex;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ThreadState {
    Work,
    Overhead,
    Contention,
    Sleep
}

pub trait ThreadStateLogger {
    fn enter(&self, now: ThreadState);
}

pub struct ThreadStateIgnorer;
impl ThreadStateLogger for ThreadStateIgnorer {
    fn enter(&self, _now: ThreadState) {}
}

struct TimeSlice {
    start: u64,
    end: u64
}

pub struct TimeSliceWatch {
    slices: VecDeque<TimeSlice>,
    running_since: Option<u64>
}
impl TimeSliceWatch {
    pub fn new() -> TimeSliceWatch {
        TimeSliceWatch {
            slices: VecDeque::new(),
            running_since: None
        }
    }

    pub fn start(&mut self) -> bool {
        if self.running_since.is_none() {
            self.running_since = Some(precise_time_ns());
            true
        } else {
            false
        }
    }

    pub fn stop(&mut self) -> bool {
        if self.running_since.is_some() {
            let now = precise_time_ns();
            self.slices.push_back(TimeSlice {
                start: self.running_since.unwrap(),
                end: now
            });
            self.running_since = None;
            true
        } else {
            false
        }
    }

    pub fn forget(&mut self, backtrace: Duration) {
        let now = precise_time_ns();
        let earliest = now - backtrace.num_nanoseconds().unwrap() as u64;
        while self.slices.front().filter(|slice| slice.end < earliest).is_some() {
            self.slices.pop_front();
        }
        match self.slices.front_mut() {
            Some(ref mut slice) if slice.start < earliest => slice.start = earliest,
            _ => ()
        };
        match self.running_since {
            Some(since) if since < earliest => self.running_since = Some(earliest),
            _ => ()
        };
    }

    pub fn accum(&self) -> Duration {
        let now = precise_time_ns();
        let mut accum = 0u64;
        for ref slice in self.slices.iter() {
            accum += slice.end - slice.start;
        }
        match self.running_since {
            Some(since) => accum += now - since,
            None => ()
        };
        Duration::nanoseconds(accum as i64)
    }
}

pub struct ThreadStateProfile {
    pub work: Duration,
    pub overhead: Duration,
    pub contention: Duration,
    pub sleep: Duration
}
impl ThreadStateProfile {
    fn zeroed() -> ThreadStateProfile {
        ThreadStateProfile {
            work: Duration::nanoseconds(0),
            overhead: Duration::nanoseconds(0),
            contention: Duration::nanoseconds(0),
            sleep: Duration::nanoseconds(0)
        }
    }
}
impl Debug for ThreadStateProfile {
    fn fmt(&self, f: &mut Formatter) -> Result {
        f.write_str("ThreadWorkProfile {")?;
        f.write_str(&format!(" work: {}s ", self.work.num_milliseconds() as f64 / 1000.0))?;
        f.write_str(&format!(" overhead: {}s ", self.overhead.num_milliseconds() as f64 / 1000.0))?;
        f.write_str(&format!(" contention: {}s ", self.contention.num_milliseconds() as f64 / 1000.0))?;
        f.write_str(&format!(" sleep: {}s ", self.sleep.num_milliseconds() as f64 / 1000.0))?;
        f.write_str("}")
    }
}

struct ThreadStateProfilerState {
    current: ThreadState,
    work: TimeSliceWatch,
    overhead: TimeSliceWatch,
    contention: TimeSliceWatch,
    sleep:  TimeSliceWatch
}
pub struct ThreadStateProfiler {
    state: Mutex<ThreadStateProfilerState>,
    backtrace: Duration
}
impl ThreadStateProfiler {
    pub fn start_new(initial_state: ThreadState, backtrace: Duration) -> ThreadStateProfiler {
        let mut state = ThreadStateProfilerState {
            current: initial_state,
            work: TimeSliceWatch::new(),
            overhead: TimeSliceWatch::new(),
            contention: TimeSliceWatch::new(),
            sleep: TimeSliceWatch::new()
        };
        match initial_state {
            ThreadState::Work => state.work.start(),
            ThreadState::Overhead => state.overhead.start(),
            ThreadState::Contention => state.contention.start(),
            ThreadState::Sleep => state.sleep.start()
        };
        ThreadStateProfiler {
            state: Mutex::new(state),
            backtrace
        }
    }

    pub fn profile(&self) -> ThreadStateProfile {
        let mut profile = ThreadStateProfile::zeroed();
        let mut guard = self.state.lock().unwrap();

        guard.work.forget(self.backtrace);
        guard.overhead.forget(self.backtrace);
        guard.contention.forget(self.backtrace);
        guard.sleep.forget(self.backtrace);

        profile.work = guard.work.accum();
        profile.overhead = guard.overhead.accum();
        profile.contention = guard.contention.accum();
        profile.sleep =  guard.sleep.accum();

        profile
    }
}
impl ThreadStateLogger for ThreadStateProfiler {
    fn enter(&self, now: ThreadState) {
        let mut guard = self.state.lock().unwrap();
        match guard.current {
            ThreadState::Work => guard.work.stop(),
            ThreadState::Overhead => guard.overhead.stop(),
            ThreadState::Contention => guard.contention.stop(),
            ThreadState::Sleep => guard.sleep.stop()
        };
        match now {
            ThreadState::Work => guard.work.start(),
            ThreadState::Overhead => guard.overhead.start(),
            ThreadState::Contention => guard.contention.start(),
            ThreadState::Sleep => guard.sleep.start()
        };
        guard.current = now;
    }
}