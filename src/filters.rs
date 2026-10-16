use std::error::Error;

use dirwatch::{Event, EventKind};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::args::EventFilter;

pub(crate) struct PathFilters {
    include: Option<GlobSet>,
    exclude: Option<GlobSet>,
}

impl PathFilters {
    pub(crate) fn new(includes: &[String], excludes: &[String]) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            include: compile_globs(includes)?,
            exclude: compile_globs(excludes)?,
        })
    }

    pub(crate) fn matches(&self, event: &Event) -> bool {
        if event.kind == EventKind::Overflow {
            return true;
        }
        let Some(path) = event.path.as_deref() else {
            return true;
        };
        let path = path.to_string_lossy().replace('\\', "/");
        if let Some(patterns) = &self.include {
            if !patterns.is_match(&path) {
                return false;
            }
        }
        if let Some(patterns) = &self.exclude {
            if patterns.is_match(&path) {
                return false;
            }
        }
        true
    }
}

fn compile_globs(patterns: &[String]) -> Result<Option<GlobSet>, Box<dyn Error>> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern);
        #[cfg(windows)]
        glob.case_insensitive(true);
        builder.add(glob.build()?);
    }
    Ok(Some(builder.build()?))
}

pub(crate) fn event_matches(event: &Event, kinds: &[EventFilter]) -> bool {
    event.kind == EventKind::Overflow
        || kinds.is_empty()
        || kinds.iter().any(|kind| {
            matches!(
                (kind, event.kind),
                (EventFilter::Created, EventKind::Created)
                    | (EventFilter::Modified, EventKind::Modified)
                    | (EventFilter::Removed, EventKind::Removed)
                    | (EventFilter::Renamed, EventKind::Renamed)
                    | (EventFilter::Overflow, EventKind::Overflow)
            )
        })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use dirwatch::{Event, EventKind};

    use super::{event_matches, PathFilters};
    use crate::args::EventFilter;

    #[test]
    fn glob_filters_include_and_exclude_paths() {
        let filters =
            PathFilters::new(&["**/*.rs".to_owned()], &["**/generated/**".to_owned()]).unwrap();
        let included = Event::new(EventKind::Modified, r"C:\code\src\main.rs");
        let excluded = Event::new(EventKind::Modified, r"C:\code\generated\build.rs");
        assert!(filters.matches(&included));
        assert!(!filters.matches(&excluded));
    }

    #[test]
    fn overflow_without_a_path_is_not_filtered_out() {
        let filters = PathFilters::new(&["**/*.rs".to_owned()], &[]).unwrap();
        assert!(filters.matches(&Event::overflow(None)));
        assert!(filters.matches(&Event::overflow(Some(PathBuf::from("changes.txt")))));
        assert!(event_matches(
            &Event::overflow(None),
            &[EventFilter::Created]
        ));
    }
}
