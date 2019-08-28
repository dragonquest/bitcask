use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create base dir for db from path '{}': {}", path.display(), source))]
    CreateDatabaseDir {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to fill keydir from path '{}': {}", path.display(), source))]
    KeyDirFill {
        path: std::path::PathBuf,
        source: Box<dyn std::error::Error>,
    },
}
