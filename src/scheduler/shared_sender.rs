use futures::channel::oneshot::Sender;
use std::sync::Mutex;
use std::mem;

pub struct SharedSender<T> {
    inner: Mutex<Option<Sender<T>>>,
}

impl<T> SharedSender<T> {
    pub fn new(sender: Sender<T>) -> SharedSender<T> {
        SharedSender {
            inner: Mutex::new(Some(sender))
        }
    }

    pub fn send(&self, value: T) {
        let mut sender_opt = self.inner.lock().unwrap();
        let mut owned = None;
        mem::swap(&mut *sender_opt, &mut owned);
        if let Some(sender) = owned {
            let _ = sender.send(value);
        }
    }
}