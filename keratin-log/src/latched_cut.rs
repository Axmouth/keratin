//! Marker left by a log handle that stopped after a failed sync. It names the
//! end of the last successful sync, and the next open cuts the log back to it
//! before anything reads the tail. Linux reports a writeback error once, so a
//! later scan or sync can accept pages that never reached the disk.
//!
//! Everything a log has confirmed, reported to a follower or sealed for
//! recovery lies at or below that end: confirms and progress follow syncs,
//! and a seal syncs the log before recording its history. The cut therefore
//! removes only records nobody relies on, for bound histories too.

use crate::{manifest::Manifest, suffix_repair, util::fsync_dir};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

pub(crate) const MARKER: &str = "latched-cut.pending";
const MAGIC: &[u8; 8] = b"KLATCH01";
const SIZE: usize = 8 + 8 + 4;

fn invalid(text: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text)
}

/// Persist the cut. The caller stores the manifest first, so the cut never
/// lies past the manifest's next offset.
pub(crate) fn write(root: &Path, cut: u64) -> io::Result<()> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&cut.to_be_bytes());
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_be_bytes());
    fs::create_dir_all(root.join("tmp"))?;
    let tmp = root.join("tmp/latched-cut.new");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(tmp, root.join(MARKER))?;
    fsync_dir(root)
}

fn load(root: &Path) -> io::Result<Option<u64>> {
    let mut file = match File::open(root.join(MARKER)) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if file.metadata()?.len() != SIZE as u64 {
        return Err(invalid("invalid latched cut marker size"));
    }
    let mut bytes = [0; SIZE];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] != MAGIC
        || crc32c::crc32c(&bytes[..SIZE - 4])
            != u32::from_be_bytes(bytes[SIZE - 4..].try_into().unwrap())
    {
        return Err(invalid("invalid latched cut marker checksum or version"));
    }
    Ok(Some(u64::from_be_bytes(bytes[8..16].try_into().unwrap())))
}

/// Carry out a pending latched cut, then drop the marker. Repeatable after a
/// crash at any step. A cut journal left by other repair work is refused, so
/// coordinated repairs keep their own handling.
pub(crate) fn heal(root: &Path) -> io::Result<bool> {
    let Some(cut) = load(root)? else {
        return Ok(false);
    };
    match suffix_repair::pending_cut(root)? {
        Some(pending) if pending != cut => {
            return Err(invalid("latched cut found another pending suffix repair"));
        }
        Some(_) => {}
        None => {
            let manifest = Manifest::read_from(&mut File::open(Manifest::path(root))?)?;
            if cut < manifest.next_offset {
                suffix_repair::prepare(root, &manifest, cut)?;
            }
        }
    }
    suffix_repair::resume(root)?;
    fs::remove_file(root.join(MARKER))?;
    fsync_dir(root)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_round_trips_and_rejects_corruption() {
        let dir = crate::test_dir!("latched_cut_marker");
        fs::create_dir_all(&dir.root).unwrap();
        assert_eq!(load(&dir.root).unwrap(), None);
        write(&dir.root, 42).unwrap();
        assert_eq!(load(&dir.root).unwrap(), Some(42));
        let path = dir.root.join(MARKER);
        let mut bytes = fs::read(&path).unwrap();
        bytes[10] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(load(&dir.root).is_err());
    }
}
