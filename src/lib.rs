mod config;
mod database;
mod datafile;
mod error;
mod indexfile;
mod keydir;
mod utils;

pub use database::Database;
pub use database::Options;

pub use database::new;
pub use database::*;

pub type ErrorResult<T> = Result<T, Box<dyn std::error::Error>>;

/*
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
*/
