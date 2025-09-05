#[derive(Debug)]
pub struct BlockId(pub u64);

// (Optional but convenient) conversions:
impl From<u64> for BlockId {
    fn from(v: u64) -> Self {
        BlockId(v)
    }
}
impl From<BlockId> for u64 {
    fn from(b: BlockId) -> u64 {
        b.0
    }
}
