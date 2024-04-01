pub mod double;
pub mod single;

#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
}