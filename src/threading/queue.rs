use ::std::marker::PhantomData;
use ::std::sync::{Arc, Mutex};
use ::std::collections::VecDeque;
use super::profile::*;

// the core interfaces for threading queues
pub trait QueueOut<T>: Send + Sync
    where T: Send + Sync + 'static {
    fn poll(&self, profile: &ThreadStateLogger) -> Option<T>;

    fn drain_to(&self, target: &mut FnMut(T) -> ()) -> ();

    fn map<R, M>(self, func: M) -> QueueOutMap<Self, T, R>
        where M: Fn(T) -> R + Send + Sync + 'static,
              R: Send + Sync + 'static,
              Self: Sized + Send + Sync + 'static {
        QueueOutMap::new(self, func)
    }

    fn clone(&self) -> Self
        where Self: Sized;

    fn clone_box(&self) -> Box<QueueOut<T>>;
}

pub trait QueueIn<T>: Send + Sync
    where T: Send + Sync + 'static {
    fn push(&self, elem: T, profile: &ThreadStateLogger);

    fn map<R, M>(self, func: M) -> QueueInMap<Self, T, R>
        where M: Fn(R) -> T + Send + Sync + 'static,
              R: Send + Sync + 'static,
              Self: Sized + Send + Sync + 'static {
        QueueInMap::new(self, func)
    }

    fn clone(&self) -> Self
        where Self: Sized;

    fn clone_box(&self) -> Box<QueueIn<T>>;

    fn len(&self) -> usize;
}

// mappings
pub struct QueueOutMap<Q, S, R>
    where Q: QueueOut<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    src: Q,
    func: Arc<Fn(S) -> R + Send + Sync + 'static>,

    p1: PhantomData<S>,
    p2: PhantomData<R>
}
impl<Q, S, R> QueueOutMap<Q, S, R>
    where Q: QueueOut<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    #[allow(dead_code)]
    fn new<M>(src: Q, func: M) -> QueueOutMap<Q, S, R>
        where M: Fn(S) -> R + Send + Sync + 'static {
        QueueOutMap {
            src: src,
            func: Arc::new(func),
            p1: PhantomData,
            p2: PhantomData
        }
    }
}
impl<Q, S, R> QueueOut<R> for QueueOutMap<Q, S, R>
    where Q: QueueOut<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    fn poll(&self, profile: &ThreadStateLogger) -> Option<R> {
        self.src.poll(profile).map(|s| (self.func)(s))
    }

    fn drain_to(&self, target: &mut FnMut(R) -> ()) -> () {
        self.src.drain_to(&mut |s| target((self.func)(s)));
    }

    fn clone(&self) -> Self {
        QueueOutMap {
            src: self.src.clone(),
            func: self.func.clone(),
            p1: PhantomData,
            p2: PhantomData
        }
    }

    fn clone_box(&self) -> Box<QueueOut<R>> {
        Box::new(self.clone())
    }
}

pub struct QueueInMap<Q, S, R>
    where Q: QueueIn<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    src: Q,
    func: Arc<Fn(R) -> S + Send + Sync + 'static>,

    p1: PhantomData<S>,
    p2: PhantomData<R>
}
impl<Q, S, R> QueueInMap<Q, S, R>
    where Q: QueueIn<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    #[allow(dead_code)]
    fn new<M>(src: Q, func: M) -> QueueInMap<Q, S, R>
        where M: Fn(R) -> S + Send + Sync + 'static {
        QueueInMap {
            src: src,
            func: Arc::new(func),
            p1: PhantomData,
            p2: PhantomData
        }
    }
}
impl<Q, S, R> QueueIn<R> for QueueInMap<Q, S, R>
    where Q: QueueIn<S> + Sized + Send + Sync + 'static,
          R: Send + Sync + 'static,
          S: Send + Sync + 'static {
    fn push(&self, elem: R, profile: &ThreadStateLogger) {
        self.src.push((self.func)(elem), profile);
    }

    fn clone(&self) -> Self {
        QueueInMap {
            src: self.src.clone(),
            func: self.func.clone(),
            p1: PhantomData,
            p2: PhantomData
        }
    }

    fn clone_box(&self) -> Box<QueueIn<R>> {
       Box::new(self.clone())
    }

    fn len(&self) -> usize {
        self.src.len()
    }
}

// the listener type
pub type Listener = Arc<Fn(bool) -> () + Send + Sync + 'static>;

// linear queue implementation
pub struct LinearQueueOut<T>
    where T: Send + Sync + 'static {
    queue: Arc<Mutex<VecDeque<T>>>,
    listener: Listener
}
impl<T> QueueOut<T> for LinearQueueOut<T>
    where T: Send + Sync + 'static {
    fn poll(&self, profile: &ThreadStateLogger) -> Option<T> {
        profile.enter(ThreadState::Contention);
        let mut queue = self.queue.lock().unwrap();
        profile.enter(ThreadState::Overhead);
        let result = queue.remove(0);
        (self.listener)(!queue.is_empty());
        result
    }

    fn drain_to(&self, target: &mut FnMut(T) -> ()) -> () {
        let mut queue = self.queue.lock().unwrap();
        while !queue.is_empty() {
            target(queue.remove(0).unwrap());
        }
        (self.listener)(false);
    }

    fn clone(&self) -> Self {
        LinearQueueOut {
            queue: self.queue.clone(),
            listener: self.listener.clone()
        }
    }

    fn clone_box(&self) -> Box<QueueOut<T>> {
        Box::new(self.clone())
    }
}
pub struct LinearQueueIn<T>
    where T: Send + Sync + 'static {
    queue: Arc<Mutex<VecDeque<T>>>,
    listener: Listener
}
impl<T> QueueIn<T> for LinearQueueIn<T>
    where T: Send + Sync + 'static {
    fn push(&self, elem: T, profile: &ThreadStateLogger) {
        profile.enter(ThreadState::Contention);
        let mut queue = self.queue.lock().unwrap();
        profile.enter(ThreadState::Overhead);
        queue.push_back(elem);
        (self.listener)(!queue.is_empty());
    }

    fn clone(&self) -> Self {
        LinearQueueIn {
            queue: self.queue.clone(),
            listener: self.listener.clone()
        }
    }

    fn clone_box(&self) -> Box<QueueIn<T>> {
        Box::new(self.clone())
    }

    fn len(&self) -> usize {
        let queue = self.queue.lock().unwrap();
        queue.len()
    }
}
pub struct LinearQueue<T>
    where T: Send + Sync + 'static {
    pub input: LinearQueueIn<T>,
    pub output: LinearQueueOut<T>,
}
impl<T> LinearQueue<T>
    where T: Send + Sync + 'static {
    pub fn new<L>(listener: L) -> LinearQueue<T>
        where L: Fn(bool) -> () + Send + Sync + 'static {
        let internal = Arc::new(Mutex::new(VecDeque::<T>::new()));
        let listener = Arc::new(listener);
        LinearQueue {
            input: LinearQueueIn {
                queue: internal.clone(),
                listener: listener.clone()
            },
            output: LinearQueueOut {
                queue: internal.clone(),
                listener: listener.clone()
            },
        }
    }
}
impl<T> Clone for LinearQueue<T>
    where T: Send + Sync + 'static {
    fn clone(&self) -> Self {
        LinearQueue {
            input: self.input.clone(),
            output: self.output.clone(),
        }
    }
}

// tests
#[cfg(test)]
mod queue_tests {
    use ::std::thread;
    use ::std::collections::HashSet;
    use ::std::time::Duration;
    use super::*;

    fn gen_test<I, O>(input: I, output: O)
        where I: QueueIn<i32> + Sized + Send + Sync + 'static,
              O: QueueOut<i32> + Sized {

        let mut handles = Vec::<thread::JoinHandle<()>>::new();
        for i in 0..10 {
            let input = input.clone();
            handles.push(thread::spawn(move || {
                thread::sleep(Duration::from_millis(i * 20));
                for n in 0..10 {
                    input.push((i * 10 + n) as i32, &mut ThreadStateIgnorer);
                }
            }));
        }

        let correct = (0..100).collect::<HashSet<i32>>();
        let mut actual = HashSet::<i32>::new();

        for handle in handles {
            handle.join().unwrap();
            loop {
                match output.poll(&mut ThreadStateIgnorer) {
                    Some(n) => actual.insert(n),
                    None => break
                };
            }
        }

        assert_eq!(correct, actual);
    }

    #[test]
    fn linear_gen_test() {
        let queues = LinearQueue::<i32>::new(move |_| ());
        gen_test(queues.input.clone(), queues.output.clone());
    }

}
