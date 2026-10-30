//! Relocation for stopped torrents. Destinations are created exclusively;
//! all source files stay in place until every destination is ready.
use anyhow::{Context, Result, bail};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Copy or link the listed files, without overwriting anything, then
/// remove the originals. Missing (skipped) files are left missing.
pub(super) fn move_files(pairs: &[(PathBuf, PathBuf)]) -> Result<()> {
    for (source, destination) in pairs {
        check_parents(source)?;
        check_parents(destination)?;
        match fs::symlink_metadata(source) {
            Ok(meta) if !meta.is_file() => bail!("{} is not a regular file", source.display()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
            _ => {}
        }
        match fs::symlink_metadata(destination) {
            Ok(_) => bail!("destination {} already exists", destination.display()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    let mut created = Vec::new();
    let copied = (|| -> Result<()> {
        for (source, destination) in pairs {
            if !source.try_exists()? {
                continue;
            }
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            // Hard links retain sparse allocation and work without a data copy
            // on the same filesystem. Copy exclusively on other filesystems.
            match fs::hard_link(source, destination) {
                Ok(()) => created.push((source, destination)),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Err(e.into()),
                Err(_) => {
                    let mut input = fs::File::open(source)?;
                    let mut output = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(destination)?;
                    created.push((source, destination));
                    // Preserve restrictive permissions before copying bytes.
                    output.set_permissions(input.metadata()?.permissions())?;
                    io::copy(&mut input, &mut output)?;
                    output.sync_all()?;
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = copied {
        for (_, path) in created {
            let _ = fs::remove_file(path);
        }
        return Err(error.context("sources were left in place"));
    }
    // A cleanup failure leaves a harmless extra original; the complete new
    // copy remains usable. Never roll back after removing any source.
    for (source, _) in created {
        if let Err(error) = fs::remove_file(source) {
            tracing::warn!(path = %source.display(), %error, "moved data, but could not remove the old copy");
        }
    }
    Ok(())
}

fn check_parents(path: &Path) -> Result<()> {
    for parent in path.ancestors().skip(1) {
        if parent.as_os_str().is_empty() {
            continue;
        }
        match fs::symlink_metadata(parent) {
            Ok(meta) if !meta.is_dir() => {
                bail!("{} is a link or not a directory", parent.display())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("cannot inspect {}", parent.display()));
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn existing_destinations_leave_every_source_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let dir = root.as_path();
        let a = dir.join("a");
        let b = dir.join("b");
        let x = dir.join("x");
        let y = dir.join("y");
        fs::write(&a, b"one").unwrap();
        fs::write(&b, b"two").unwrap();
        fs::write(&y, b"keep").unwrap();
        assert!(move_files(&[(a.clone(), x.clone()), (b.clone(), y.clone())]).is_err());
        assert_eq!(fs::read(a).unwrap(), b"one");
        assert_eq!(fs::read(b).unwrap(), b"two");
        assert!(!x.exists());
        assert_eq!(fs::read(y).unwrap(), b"keep");
    }
    #[test]
    fn moves_existing_files_and_keeps_skipped_files_missing() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let dir = root.as_path();
        let a = dir.join("old/a");
        let b = dir.join("new/a");
        fs::create_dir(a.parent().unwrap()).unwrap();
        fs::write(&a, b"data").unwrap();
        move_files(&[
            (a.clone(), b.clone()),
            (dir.join("old/skipped"), dir.join("new/skipped")),
        ])
        .unwrap();
        assert!(!a.exists());
        assert_eq!(fs::read(b).unwrap(), b"data");
        assert!(!dir.join("new/skipped").exists());
    }
    #[cfg(unix)]
    #[test]
    fn refuses_symlinked_source_and_destination_parents() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let dir = root.as_path();
        let source = dir.join("source");
        fs::write(&source, b"keep").unwrap();
        symlink(dir, dir.join("link")).unwrap();
        assert!(move_files(&[(source.clone(), dir.join("link/dest"))]).is_err());
        assert!(move_files(&[(dir.join("link/source"), dir.join("dest"))]).is_err());
        assert_eq!(fs::read(source).unwrap(), b"keep");
    }
}
