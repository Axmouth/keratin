#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KDurability {
    AfterWrite,
    AfterFsync,
    AfterReplicated, // reserved
}
