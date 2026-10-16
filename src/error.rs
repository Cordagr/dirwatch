use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Notify(String),
    InvalidConfig(&'static str),
    WatchLimitReached,
    WatchNotFound,
    Closed,
    Poisoned,
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Notify(error) => write!(formatter, "filesystem notification error: {error}"),
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::WatchLimitReached => write!(formatter, "watch limit reached"),
            Self::WatchNotFound => write!(formatter, "watch not found"),
            Self::Closed => write!(formatter, "watcher is closed"),
            Self::Poisoned => write!(formatter, "internal synchronization state is poisoned"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
