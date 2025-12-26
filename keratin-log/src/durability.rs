#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Durability {
    AfterWrite,
    AfterFsync,
    AfterReplicated, // reserved
}
