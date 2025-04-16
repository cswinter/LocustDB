use super::SharedSender;
use futures::channel::oneshot;

pub trait Task: Sync + Send {
    fn execute(&self);
    fn completed(&self) -> bool;
    fn max_parallelism(&self) -> usize;
    fn multithreaded(&self) -> bool {
        self.max_parallelism() > 1
    }
}

impl Task for dyn Fn() + Send + Sync + 'static {
    fn execute(&self) {
        self.call(());
    }

    fn completed(&self) -> bool {
        false
    }
    fn max_parallelism(&self) -> usize {
        1
    }
}

struct FnTask<F, T>
where
    F: Fn() -> T + Sync + Send + 'static,
    T: Send,
{
    fun: F,
    sender: SharedSender<T>,
}

impl<F, T> Task for FnTask<F, T>
where
    F: Fn() -> T + Sync + Send + 'static,
    T: Send,
{
    fn execute(&self) {
        let result = self.fun.call(());
        self.sender.send(result);
    }

    fn completed(&self) -> bool {
        false
    }
    fn max_parallelism(&self) -> usize {
        1
    }
}

impl dyn Task {
    pub fn from_fn<F, T>(fun: F) -> (impl Task, oneshot::Receiver<T>)
    where
        F: Fn() -> T + Sync + Send + 'static,
        T: Send,
    {
        let (sender, receiver) = oneshot::channel();
        (
            FnTask {
                fun,
                sender: SharedSender::new(sender),
            },
            receiver,
        )
    }
}
