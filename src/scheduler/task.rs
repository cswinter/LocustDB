use futures::channel::oneshot;
use super::SharedSender;

pub trait Task: Sync + Send {
    fn execute(&self);
    fn completed(&self) -> bool;
    fn multithreaded(&self) -> bool;
}


impl Task for dyn Fn() -> () + Send + Sync + 'static {
    fn execute(&self) {
        self.call(());
    }

    fn completed(&self) -> bool { false }
    fn multithreaded(&self) -> bool { false }
}

struct FnTask<F, T> where
    F: Fn() -> T + Sync + Send + 'static,
    T: Send {
    fun: F,
    sender: SharedSender<T>,
}

impl<F, T> Task for FnTask<F, T> where
    F: Fn() -> T + Sync + Send + 'static,
    T: Send {
    fn execute(&self) {
        let result = self.fun.call(());
        self.sender.send(result);
    }

    fn completed(&self) -> bool { false }
    fn multithreaded(&self) -> bool { false }
}

impl dyn Task {
    pub fn from_fn<F, T>(fun: F) -> (impl Task, oneshot::Receiver<T>) where
        F: Fn() -> T + Sync + Send + 'static,
        T: Send {
        let (sender, receiver) = oneshot::channel();
        (FnTask { fun, sender: SharedSender::new(sender) }, receiver)
    }
}
