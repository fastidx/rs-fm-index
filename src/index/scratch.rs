use std::env;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

pub(crate) const SCRATCH_DIR_ENV: &str = "FM_INDEX_SCRATCH_DIR";

pub(crate) fn resolve_scratch_dir(explicit: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = explicit {
        return Some(path.to_path_buf());
    }

    env::var_os(SCRATCH_DIR_ENV).and_then(|value| {
        if value.as_os_str().is_empty() {
            None
        } else {
            Some(PathBuf::from(value))
        }
    })
}

pub(crate) fn named_temp_file(scratch_dir: Option<&Path>) -> io::Result<NamedTempFile> {
    if let Some(dir) = resolve_scratch_dir(scratch_dir) {
        NamedTempFile::new_in(dir)
    } else {
        NamedTempFile::new()
    }
}

pub(crate) fn temp_file(scratch_dir: Option<&Path>) -> io::Result<File> {
    if let Some(dir) = resolve_scratch_dir(scratch_dir) {
        tempfile::tempfile_in(dir)
    } else {
        tempfile::tempfile()
    }
}
