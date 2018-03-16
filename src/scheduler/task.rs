pub trait Task: Sync + Send {
    fn execute(&self);
    fn completed(&self) -> bool;
    fn multithreaded(&self) -> bool;
}
