//! On-disk layout of a Stroma root, version 2.
//!
//! ```text
//! <root>/
//!   stroma-layout      magic and layout version
//!   global/            global key-value log
//!   partitions/<group>/<topic>/<part>/
//!     kind             queue or stream, written once
//!     current          name of the active generation
//!     <generation>/    log/, messages/ (streams only), state/
//!     recovery/        recovery bookkeeping for this partition
//!     staging/<plan>/  recovery staging for this partition
//!   trash/             destroyed partitions, removed at open
//!   tmp/               scratch for atomic writes, cleared at open
//! ```
//!
//! Paths depend only on the partition identity. `<group>` is `@none` for a
//! queue without a group. Real names always percent-encode `@`, so reserved
//! names starting with `@` never collide with a real group or topic.
use super::*;
use keratin_log::util::fsync_dir;
use std::ffi::OsString;

pub(crate) const MARKER: &str = "stroma-layout";
const MAGIC: &str = "stroma-layout";
const VERSION: u32 = 2;
pub(super) const GLOBAL: &str = "global";
pub(super) const PARTITIONS: &str = "partitions";
pub(super) const TRASH: &str = "trash";
pub(super) const TMP: &str = "tmp";
const NO_GROUP: &str = "@none";
const EMPTY_NAME: &str = "@empty";
const KIND: &str = "kind";
const CURRENT: &str = "current";
pub(super) const RECOVERY: &str = "recovery";
pub(super) const STAGING: &str = "staging";
// Resolved for a partition without a current generation. Nothing creates it,
// so reads find nothing there, and a stray write is refused at the next open.
pub(super) const NO_GENERATION: &str = "@no-generation";
pub(super) const LOG: &str = "log";
pub(super) const MESSAGES: &str = "messages";
pub(super) const STATE: &str = "state";

// Entries that only older layouts put directly in the root.
const OLD_LAYOUT_ENTRIES: &[&str] = &[
    "combined-queue-log.v1",
    "events",
    "messages",
    "snapshots",
    "recovery-routes",
    "recovery-installed",
    "recovery-staging",
    "recovery-inspection",
];

fn marker_bytes() -> Vec<u8> {
    format!("{MAGIC}\nversion {VERSION}\n").into_bytes()
}

fn invalid(message: impl Into<String>) -> StromaError {
    StromaError::InvalidArgument(message.into())
}

fn corrupt(message: impl Into<String>) -> StromaError {
    StromaError::Corruption(message.into())
}

fn pending_name(name: &str) -> String {
    format!("{name}.next")
}

/// Replace `dir/name` atomically: write a temporary file next to it, sync it,
/// rename it over the target, then sync the directory. Callers serialize
/// writers of the same file.
pub(super) fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let temp = dir.join(pending_name(name));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(io_err)?;
    io::Write::write_all(&mut file, bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    drop(file);
    fs::rename(&temp, dir.join(name)).map_err(io_err)?;
    fsync_dir(dir).map_err(io_err)
}

/// Create `dir` and any missing parents below `root`, syncing each parent
/// whose entries changed. `dir` must be inside `root`, which already exists.
pub(super) fn create_dir_within(root: &Path, dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let relative = dir
        .strip_prefix(root)
        .map_err(|_| invalid(format!("{} is outside {}", dir.display(), root.display())))?;
    let mut parent = root.to_path_buf();
    for component in relative.components() {
        let child = parent.join(component);
        match fs::create_dir(&child) {
            Ok(()) => fsync_dir(&parent).map_err(io_err)?,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && child.is_dir() => {}
            Err(e) => return Err(io_err(e)),
        }
        parent = child;
    }
    Ok(())
}

fn entry_names(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_err)? {
        let name = entry.map_err(io_err)?.file_name();
        names.push(utf8_name(dir, name)?);
    }
    names.sort();
    Ok(names)
}

fn utf8_name(dir: &Path, name: OsString) -> Result<String> {
    name.into_string().map_err(|name| {
        corrupt(format!(
            "unexpected entry {:?} in {}",
            name,
            dir.display()
        ))
    })
}

fn check_marker(root: &Path, bytes: &[u8]) -> Result<()> {
    let text = std::str::from_utf8(bytes).unwrap_or("");
    let mut lines = text.lines();
    if lines.next() != Some(MAGIC) {
        return Err(corrupt(format!(
            "{} has an unreadable {MARKER} marker",
            root.display()
        )));
    }
    let version = lines
        .next()
        .and_then(|line| line.strip_prefix("version "))
        .and_then(|v| v.parse::<u32>().ok())
        .ok_or_else(|| corrupt(format!("{} has an unreadable {MARKER} marker", root.display())))?;
    if version != VERSION {
        return Err(invalid(format!(
            "{} uses Stroma storage layout version {version}, this build only opens version {VERSION}",
            root.display()
        )));
    }
    Ok(())
}

/// Validate or initialize a Stroma root, then clear scratch space and finish
/// any destroy that was interrupted before its trash was removed.
pub(super) fn prepare_root(root: &Path) -> Result<()> {
    let names = match fs::read_dir(root) {
        Ok(_) => entry_names(root)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // The new root entry must survive a crash. Its parent is synced
            // here once instead of on every later write.
            crate::dir_sync::create_dir_all_synced(root).map_err(io_err)?;
            Vec::new()
        }
        Err(e) => return Err(io_err(e)),
    };
    let pending_marker = pending_name(MARKER);
    if names.iter().any(|n| n == MARKER) {
        check_marker(root, &fs::read(root.join(MARKER)).map_err(io_err)?)?;
        if let Some(unknown) = names
            .iter()
            .find(|n| ![MARKER, GLOBAL, PARTITIONS, TRASH, TMP].contains(&n.as_str()))
        {
            return Err(invalid(format!(
                "Stroma root {} contains an unknown entry `{unknown}`. Only {MARKER}, {GLOBAL}, {PARTITIONS}, {TRASH} and {TMP} belong there",
                root.display()
            )));
        }
    } else {
        // A temporary marker left by an interrupted first start is the only
        // thing a fresh root may contain.
        if names.iter().any(|n| *n != pending_marker) {
            if names.iter().any(|n| OLD_LAYOUT_ENTRIES.contains(&n.as_str())) {
                return Err(invalid(format!(
                    "{} holds an older Stroma storage layout that this version cannot open. Move it aside and start from an empty directory",
                    root.display()
                )));
            }
            return Err(invalid(format!(
                "{} is not empty and has no {MARKER} marker. Stroma only initializes a missing or empty directory",
                root.display()
            )));
        }
        write_atomic(root, MARKER, &marker_bytes())?;
    }
    let mut created = false;
    for name in [PARTITIONS, TRASH, TMP] {
        match fs::create_dir(root.join(name)) {
            Ok(()) => created = true,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io_err(e)),
        }
    }
    if created {
        fsync_dir(root).map_err(io_err)?;
    }
    clear_dir(&root.join(TMP))?;
    clear_dir(&root.join(TRASH))?;
    Ok(())
}

fn clear_dir(dir: &Path) -> Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let path = entry.path();
        if entry.file_type().map_err(io_err)?.is_dir() {
            fs::remove_dir_all(&path).map_err(io_err)?;
        } else {
            fs::remove_file(&path).map_err(io_err)?;
        }
        removed = true;
    }
    if removed {
        fsync_dir(dir).map_err(io_err)?;
    }
    Ok(())
}

fn encode_name(name: &str) -> String {
    if name.is_empty() {
        EMPTY_NAME.to_owned()
    } else {
        Stroma::enc_component(name)
    }
}

// Accept only the canonical encoding, so every partition has one directory.
fn decode_name(encoded: &str) -> Option<String> {
    if encoded == EMPTY_NAME {
        return Some(String::new());
    }
    let decoded = Stroma::dec_component(encoded).ok()?;
    (!decoded.is_empty() && Stroma::enc_component(&decoded) == encoded).then_some(decoded)
}

pub(super) fn partition_dir(root: &Path, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
    let group = match normalize_group(group) {
        Some(group) => encode_name(group),
        None => NO_GROUP.to_owned(),
    };
    root.join(PARTITIONS)
        .join(group)
        .join(encode_name(topic))
        .join(format!("{part:010}"))
}

/// Name of one immutable generation directory inside a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Generation {
    /// Created with the partition.
    Initial([u8; 16]),
    /// Built by a recovery installation, learner admission or reseed. The
    /// name is derived from the plan and storage instance, so a retry finds
    /// the same generation.
    Recovery { plan: [u8; 32], instance: [u8; 16] },
}

impl Generation {
    pub(super) fn new_initial() -> Self {
        Self::Initial(*uuid::Uuid::now_v7().as_bytes())
    }

    pub(super) fn name(&self) -> String {
        match self {
            Self::Initial(id) => format!("g-{}", uuid::Uuid::from_bytes(*id)),
            Self::Recovery { plan, instance } => format!(
                "r-{}-{}",
                blake3::Hash::from_bytes(*plan).to_hex(),
                uuid::Uuid::from_bytes(*instance)
            ),
        }
    }

    pub(super) fn parse(name: &str) -> Option<Self> {
        let generation = if let Some(id) = name.strip_prefix("g-") {
            Self::Initial(*uuid::Uuid::try_parse(id).ok()?.as_bytes())
        } else {
            let rest = name.strip_prefix("r-")?;
            let (plan, instance) = rest.split_once('-')?;
            let plan = blake3::Hash::from_hex(plan).ok()?;
            let instance = uuid::Uuid::try_parse(instance).ok()?;
            Self::Recovery {
                plan: *plan.as_bytes(),
                instance: *instance.as_bytes(),
            }
        };
        let valid = match generation {
            Self::Initial(id) => id != [0; 16],
            Self::Recovery { plan, instance } => plan != [0; 32] && instance != [0; 16],
        };
        (valid && generation.name() == name).then_some(generation)
    }
}

pub(super) fn read_current(part_dir: &Path) -> Result<Option<Generation>> {
    let bytes = match fs::read(part_dir.join(CURRENT)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    let generation = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.strip_suffix('\n'))
        .and_then(Generation::parse)
        .ok_or_else(|| {
            corrupt(format!(
                "{} has an unreadable current generation",
                part_dir.display()
            ))
        })?;
    Ok(Some(generation))
}

/// Switch the partition to `generation`. The generation directory must be
/// complete and durable first. Write `current.next`, sync it, rename it over
/// `current`, then sync the partition directory. A crash before the rename
/// keeps the previous generation.
pub(super) fn write_current(part_dir: &Path, generation: &Generation) -> Result<()> {
    if !part_dir.join(generation.name()).is_dir() {
        return Err(corrupt("cannot switch to a missing generation"));
    }
    write_atomic(part_dir, CURRENT, format!("{}\n", generation.name()).as_bytes())
}

pub(super) fn read_kind(part_dir: &Path) -> Result<Option<PartitionKind>> {
    let bytes = match fs::read(part_dir.join(KIND)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    match bytes.as_slice() {
        b"queue\n" => Ok(Some(PartitionKind::Queue)),
        b"stream\n" => Ok(Some(PartitionKind::Stream)),
        _ => Err(corrupt(format!(
            "{} has an unreadable partition kind",
            part_dir.display()
        ))),
    }
}

/// The kind is written once, before the partition gets its first generation.
pub(super) fn write_kind(part_dir: &Path, kind: PartitionKind) -> Result<()> {
    let text: &[u8] = match kind {
        PartitionKind::Queue => b"queue\n",
        PartitionKind::Stream => b"stream\n",
    };
    write_atomic(part_dir, KIND, text)
}

/// One partition directory found under `partitions/`.
pub(super) struct ScannedPartition {
    pub group: Option<String>,
    pub topic: String,
    pub part: u32,
    /// `None` until the partition has a current generation. Such a directory
    /// holds only an interrupted creation or recovery bookkeeping.
    pub current: Option<Generation>,
}

/// Walk `partitions/<group>/<topic>/<part>`. Anything Stroma would not have
/// written is refused, so a foreign or damaged tree is never served.
pub(super) fn scan_partitions(root: &Path) -> Result<Vec<ScannedPartition>> {
    let partitions = root.join(PARTITIONS);
    let mut out = Vec::new();
    for group_name in entry_names(&partitions)? {
        let group_dir = partitions.join(&group_name);
        let group = if group_name == NO_GROUP {
            None
        } else {
            match decode_name(&group_name) {
                Some(group) if normalize_group(Some(&group)).is_some() => Some(group),
                _ => return Err(unexpected(&group_dir)),
            }
        };
        require_dir(&group_dir)?;
        for topic_name in entry_names(&group_dir)? {
            let topic_dir = group_dir.join(&topic_name);
            let topic = decode_name(&topic_name).ok_or_else(|| unexpected(&topic_dir))?;
            require_dir(&topic_dir)?;
            for part_name in entry_names(&topic_dir)? {
                let part_dir = topic_dir.join(&part_name);
                let part = part_name
                    .parse::<u32>()
                    .ok()
                    .filter(|part| format!("{part:010}") == part_name)
                    .ok_or_else(|| unexpected(&part_dir))?;
                require_dir(&part_dir)?;
                let current = check_partition(&part_dir)?;
                out.push(ScannedPartition {
                    group: group.clone(),
                    topic: topic.clone(),
                    part,
                    current,
                });
            }
        }
    }
    Ok(out)
}

fn unexpected(path: &Path) -> StromaError {
    corrupt(format!(
        "unexpected entry {} in the Stroma partition tree",
        path.display()
    ))
}

fn require_dir(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).map_err(io_err)?.is_dir() {
        Ok(())
    } else {
        Err(unexpected(path))
    }
}

fn check_partition(part_dir: &Path) -> Result<Option<Generation>> {
    let current = read_current(part_dir)?;
    let kind = read_kind(part_dir)?;
    if current.is_some() && kind.is_none() {
        return Err(corrupt(format!(
            "{} has a current generation but no kind",
            part_dir.display()
        )));
    }
    if let Some(generation) = &current {
        require_dir(&part_dir.join(generation.name()))?;
    }
    let (kind_next, current_next) = (pending_name(KIND), pending_name(CURRENT));
    for name in entry_names(part_dir)? {
        let known = [KIND, CURRENT, RECOVERY, STAGING].contains(&name.as_str())
            || name == kind_next
            || name == current_next
            || Generation::parse(&name).is_some();
        if !known {
            return Err(unexpected(&part_dir.join(name)));
        }
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use keratin_log::test_dir;

    #[test]
    fn fresh_root_gets_marker_and_fixed_entries() {
        let dir = test_dir!("layout_fresh");
        let root = dir.root.join("storage");
        prepare_root(&root).unwrap();
        assert_eq!(fs::read(root.join(MARKER)).unwrap(), marker_bytes());
        assert_eq!(
            entry_names(&root).unwrap(),
            [PARTITIONS, MARKER, TMP, TRASH]
        );
        // Reopening keeps the marker and accepts every known entry.
        fs::create_dir(root.join(GLOBAL)).unwrap();
        prepare_root(&root).unwrap();

        // An empty existing directory is also fresh.
        let empty = dir.root.join("empty");
        fs::create_dir(&empty).unwrap();
        prepare_root(&empty).unwrap();
        assert!(empty.join(MARKER).is_file());

        // A marker temp left by a crash during the first start is replaced.
        let interrupted = dir.root.join("interrupted");
        fs::create_dir(&interrupted).unwrap();
        fs::write(interrupted.join(pending_name(MARKER)), b"stroma-la").unwrap();
        prepare_root(&interrupted).unwrap();
        assert_eq!(fs::read(interrupted.join(MARKER)).unwrap(), marker_bytes());
        assert!(!interrupted.join(pending_name(MARKER)).exists());
    }

    #[test]
    fn old_layouts_and_foreign_directories_are_refused() {
        let dir = test_dir!("layout_refuse");
        for (name, entry, expect) in [
            ("single-log", "combined-queue-log.v1", "older Stroma storage layout"),
            ("separate", "events", "older Stroma storage layout"),
            ("routes", "recovery-routes", "older Stroma storage layout"),
            ("foreign", "setup_complete", "has no stroma-layout marker"),
        ] {
            let root = dir.root.join(name);
            fs::create_dir_all(&root).unwrap();
            fs::write(root.join(entry), b"x").unwrap();
            let err = prepare_root(&root).unwrap_err();
            assert!(err.to_string().contains(expect), "{name}: {err}");
            // Nothing is written into a refused directory.
            assert!(!root.join(MARKER).exists());
        }
    }

    #[test]
    fn unknown_entries_and_versions_are_refused() {
        let dir = test_dir!("layout_unknown");
        prepare_root(&dir.root).unwrap();
        fs::write(dir.root.join("recovery-inspection"), b"x").unwrap();
        let err = prepare_root(&dir.root).unwrap_err();
        assert!(err.to_string().contains("unknown entry `recovery-inspection`"), "{err}");
        fs::remove_file(dir.root.join("recovery-inspection")).unwrap();

        fs::write(dir.root.join(MARKER), b"stroma-layout\nversion 3\n").unwrap();
        let err = prepare_root(&dir.root).unwrap_err();
        assert!(err.to_string().contains("version 3"), "{err}");
        fs::write(dir.root.join(MARKER), b"something else\n").unwrap();
        assert!(matches!(
            prepare_root(&dir.root),
            Err(StromaError::Corruption(_))
        ));
    }

    #[test]
    fn generation_names_round_trip_and_reject_noncanonical_forms() {
        let initial = Generation::new_initial();
        assert_eq!(Generation::parse(&initial.name()), Some(initial));
        let recovery = Generation::Recovery {
            plan: [7; 32],
            instance: [9; 16],
        };
        assert_eq!(Generation::parse(&recovery.name()), Some(recovery));
        let upper = initial.name().to_uppercase().replacen("G-", "g-", 1);
        for bad in [
            "g-",
            "g-not-a-uuid",
            upper.as_str(),
            "g-00000000-0000-0000-0000-000000000000",
            "r-00",
            "x-1",
            NO_GENERATION,
            "current",
        ] {
            assert_eq!(Generation::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn partition_paths_keep_groups_and_reserved_names_apart() {
        let root = Path::new("/s");
        let none = partition_dir(root, "q", 0, None);
        assert_eq!(none, root.join("partitions/@none/q/0000000000"));
        assert_eq!(partition_dir(root, "q", 0, Some("default")), none);
        assert_eq!(
            partition_dir(root, "q", 7, Some("@none")),
            root.join("partitions/%40none/q/0000000007")
        );
        assert_eq!(
            partition_dir(root, "", 0, Some("")),
            root.join("partitions/@empty/@empty/0000000000")
        );
        assert_eq!(
            partition_dir(root, "a/b", 1, Some("g x")),
            root.join("partitions/g%20x/a%2Fb/0000000001")
        );
        for name in ["", "@none", "a/b", "%41", "x"] {
            assert_eq!(decode_name(&encode_name(name)).as_deref(), Some(name));
        }
        // Only the canonical spelling decodes, so one name has one directory.
        assert_eq!(decode_name("%78"), None);
    }

    #[test]
    fn current_switch_is_atomic_and_needs_the_generation() {
        let dir = test_dir!("layout_current");
        let part = dir.root.join("p");
        fs::create_dir(&part).unwrap();
        let first = Generation::new_initial();
        let second = Generation::Recovery {
            plan: [1; 32],
            instance: [2; 16],
        };
        assert!(write_current(&part, &first).is_err());
        fs::create_dir(part.join(first.name())).unwrap();
        write_current(&part, &first).unwrap();
        assert_eq!(read_current(&part).unwrap(), Some(first));

        // A crash after writing current.next but before the rename leaves
        // the previous generation current.
        fs::create_dir(part.join(second.name())).unwrap();
        fs::write(part.join(pending_name(CURRENT)), format!("{}\n", second.name())).unwrap();
        assert_eq!(read_current(&part).unwrap(), Some(first));
        write_current(&part, &second).unwrap();
        assert_eq!(read_current(&part).unwrap(), Some(second));
        assert!(!part.join(pending_name(CURRENT)).exists());

        fs::write(part.join(CURRENT), b"garbage\n").unwrap();
        assert!(matches!(read_current(&part), Err(StromaError::Corruption(_))));
    }
}
