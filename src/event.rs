use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Created,
    Modified,
    Removed,
    Renamed,
    Overflow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct EventSet(u32);

impl EventSet {
    pub const EMPTY: Self = Self(0);
    pub const CREATED: Self = Self(1 << 0);
    pub const MODIFIED: Self = Self(1 << 1);
    pub const REMOVED: Self = Self(1 << 2);
    pub const RENAMED: Self = Self(1 << 3);
    pub const OVERFLOW: Self = Self(1 << 4);
    pub const ALL: Self = Self((1 << 5) - 1);

    pub const fn from_kind(kind: EventKind) -> Self {
        match kind {
            EventKind::Created => Self::CREATED,
            EventKind::Modified => Self::MODIFIED,
            EventKind::Removed => Self::REMOVED,
            EventKind::Renamed => Self::RENAMED,
            EventKind::Overflow => Self::OVERFLOW,
        }
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for EventSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Event {
    pub kind: EventKind,
    pub path: Option<PathBuf>,
    pub pid: Option<u32>,
    pub is_dir: bool,
    pub usn: Option<i64>,
    pub size_byte: Option<u64>,
    pub class: Option<String>,
    pub event_set: EventSet,
}

impl Event {
    pub fn new(kind: EventKind, path: impl AsRef<Path>) -> Self {
        Self {
            kind,
            path: Some(path.as_ref().to_path_buf()),
            pid: None,
            is_dir: false,
            usn: None,
            size_byte: None,
            class: None,
            event_set: EventSet::from_kind(kind),
        }
    }

    pub fn overflow(path: Option<PathBuf>) -> Self {
        Self {
            kind: EventKind::Overflow,
            path,
            pid: None,
            is_dir: false,
            usn: None,
            size_byte: None,
            class: None,
            event_set: EventSet::OVERFLOW,
        }
    }

    /// Approximate byte size used for queue accounting.
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.path.as_ref().map_or(0, PathBuf::capacity)
            + self.class.as_ref().map_or(0, String::capacity)
    }

    pub fn contains(&self, event_set: EventSet) -> bool {
        self.event_set.contains(event_set)
    }

    pub fn union(&self, event_set: EventSet) -> EventSet {
        self.event_set.union(event_set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_set_combines_and_checks_kinds() {
        let kinds = EventSet::CREATED | EventSet::MODIFIED;
        assert!(kinds.contains(EventSet::CREATED));
        assert!(kinds.contains(EventSet::MODIFIED));
        assert!(!kinds.contains(EventSet::REMOVED));
    }

    #[test]
    fn size_bytes_grows_with_reserved_buffers() {
        let event = Event::new(EventKind::Modified, "folder\\file.txt");
        let mut larger_event = event.clone();
        larger_event.path.as_mut().unwrap().reserve(32);
        larger_event.class = Some(String::with_capacity(64));

        assert!(larger_event.size_bytes() > event.size_bytes());
    }
}
