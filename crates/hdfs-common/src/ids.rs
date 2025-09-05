use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct BlockId(pub u64);

impl core::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for BlockId {
    fn from(value: u64) -> Self {
        BlockId(value)
    }
}

impl From<BlockId> for u64 {
    fn from(value: BlockId) -> Self {
        value.0
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct INodeId(pub u64);

impl core::fmt::Display for INodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for INodeId {
    fn from(value: u64) -> Self {
        INodeId(value)
    }
}

impl From<INodeId> for u64 {
    fn from(value: INodeId) -> Self {
        value.0
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct LeaseId(pub u64);

impl core::fmt::Display for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for LeaseId {
    fn from(value: u64) -> Self {
        LeaseId(value)
    }
}

impl From<LeaseId> for u64 {
    fn from(value: LeaseId) -> Self {
        value.0
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct DatanodeId(pub Uuid);

impl DatanodeId {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn short(&self) -> String {
        let mut buf = [0u8; uuid::fmt::Simple::LENGTH];
        let s = self.0.simple().encode_lower(&mut buf);
        s[..8].to_string()
    }
}

impl std::str::FromStr for DatanodeId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let u = Uuid::parse_str(s.trim())?;
        Ok(DatanodeId(u))
    }
}

impl core::fmt::Display for DatanodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for DatanodeId {
    fn from(value: Uuid) -> Self {
        DatanodeId(value)
    }
}

impl From<DatanodeId> for Uuid {
    fn from(value: DatanodeId) -> Self {
        value.0
    }
}

pub struct IdGen {
    inode: std::sync::atomic::AtomicU64,
    block: std::sync::atomic::AtomicU64,
}

impl IdGen {
    pub fn new(start_inode: u64, start_block: u64) -> Self {
        Self {
            inode: AtomicU64::new(start_inode),
            block: AtomicU64::new(start_block),
        }
    }
    pub fn next_inode(&self) -> INodeId {
        let id = self.inode.fetch_add(1, Ordering::Relaxed);
        INodeId(id)
    }
    pub fn next_block(&self) -> BlockId {
        let id = self.block.fetch_add(1, Ordering::Relaxed);
        BlockId(id)
    }

    pub fn peek_inode(&self) -> u64 {
        self.inode.load(Ordering::Relaxed)
    }

    pub fn peek_block(&self) -> u64 {
        self.block.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeSet, HashSet};
    use std::str::FromStr;

    const FIXED: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct HolderB {
        id: BlockId,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct HolderIN {
        id: INodeId,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct HolderL {
        id: LeaseId,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct HolderD {
        id: DatanodeId,
    }

    #[test]
    fn display_and_debug() {
        //block id
        let id = BlockId(42);
        assert_eq!(id.to_string(), "42");
        assert_eq!(format!("{:?}", id), "BlockId(42)");

        //inode id
        let id = INodeId(22);
        assert_eq!(id.to_string(), "22");
        assert_eq!(format!("{:?}", id), "INodeId(22)");

        //lease id
        let id = LeaseId(11);
        assert_eq!(id.to_string(), "11");
        assert_eq!(format!("{:?}", id), "LeaseId(11)");
    }

    #[test]
    fn from_into_roundtrip() {
        //block id
        for &n in &[0u64, 1, 42, u64::MAX] {
            let id: BlockId = BlockId::from(n);
            let back: u64 = u64::from(id);
            assert_eq!(n, back);
            let id: BlockId = n.into();
            let back: u64 = id.into();
            assert_eq!(n, back);
        }

        //inode id
        for &n in &[1u64, 2, 43, u64::MIN] {
            let id: INodeId = INodeId::from(n);
            let back: u64 = u64::from(id);
            assert_eq!(n, back);
            let id: INodeId = n.into();
            let back: u64 = id.into();
            assert_eq!(n, back);
        }

        //lease id
        for &n in &[3u64, 4, 44, u64::MIN] {
            let id: LeaseId = LeaseId::from(n);
            let back: u64 = u64::from(id);
            assert_eq!(n, back);
            let id: LeaseId = n.into();
            let back: u64 = id.into();
            assert_eq!(n, back);
        }
    }

    #[test]
    fn ordering_and_hash_works() {
        //block id
        let mut v = vec![BlockId(3), BlockId(1), BlockId(2)];
        v.sort();
        assert_eq!(v, vec![BlockId(1), BlockId(2), BlockId(3)]);

        let mut s = HashSet::new();
        s.insert(BlockId(7));
        assert!(s.contains(&BlockId(7)));

        let mut ts = BTreeSet::new();
        ts.insert(BlockId(2));
        ts.insert(BlockId(1));
        assert_eq!(
            ts.into_iter().collect::<Vec<_>>(),
            vec![BlockId(1), BlockId(2)]
        );

        //inode id
        let mut v = vec![INodeId(3), INodeId(1), INodeId(2)];
        v.sort();
        assert_eq!(v, vec![INodeId(1), INodeId(2), INodeId(3)]);

        let mut s = HashSet::new();
        s.insert(INodeId(7));
        assert!(s.contains(&INodeId(7)));

        let mut ts = BTreeSet::new();
        ts.insert(INodeId(2));
        ts.insert(INodeId(1));
        assert_eq!(
            ts.into_iter().collect::<Vec<_>>(),
            vec![INodeId(1), INodeId(2)]
        );

        //lease id
        let mut v = vec![LeaseId(3), LeaseId(1), LeaseId(2)];
        v.sort();
        assert_eq!(v, vec![LeaseId(1), LeaseId(2), LeaseId(3)]);

        let mut s = HashSet::new();
        s.insert(LeaseId(7));
        assert!(s.contains(&LeaseId(7)));

        let mut ts = BTreeSet::new();
        ts.insert(LeaseId(2));
        ts.insert(LeaseId(1));
        assert_eq!(
            ts.into_iter().collect::<Vec<_>>(),
            vec![LeaseId(1), LeaseId(2)]
        );
    }

    #[test]
    fn serde_json_transparent_roundtrip() {
        //block id
        let id = BlockId(123);

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "123");
        let back: BlockId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);

        //inode id
        let id = INodeId(456);

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "456");
        let back: INodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);

        //block id
        let id = LeaseId(789);

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "789");
        let back: LeaseId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn toml_transparent_roundtrip() {
        //block id
        let id = BlockId(9);
        let holder = HolderB { id };

        let s = toml::to_string(&holder).unwrap();
        assert_eq!(s.trim(), "id = 9");

        let back: HolderB = toml::from_str(&s).unwrap();
        assert_eq!(back, holder);
        assert_eq!(back.id, holder.id);

        //inode id
        let id = INodeId(10);
        let holder = HolderIN { id };

        let s = toml::to_string(&holder).unwrap();
        assert_eq!(s.trim(), "id = 10");

        let back: HolderIN = toml::from_str(&s).unwrap();
        assert_eq!(back, holder);
        assert_eq!(back.id, holder.id);

        //lease id
        let id = LeaseId(11);
        let holder = HolderL { id };

        let s = toml::to_string(&holder).unwrap();
        assert_eq!(s.trim(), "id = 11");

        let back: HolderL = toml::from_str(&s).unwrap();
        assert_eq!(back, holder);
        assert_eq!(back.id, holder.id);
    }

    #[test]
    fn copy_and_clone_behave() {
        //block id
        let a = BlockId(5);
        let b = a;
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);

        //inode id
        let a = INodeId(5);
        let b = a;
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);
        //lease id
        let a = LeaseId(5);
        let b = a;
        let c = a.clone();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn id_gen_next() {
        let idgen = IdGen::new(1, 1);
        let inode_id = idgen.next_inode();
        let block_id = idgen.next_block();

        assert_eq!(inode_id.0, 1);
        assert_eq!(block_id.0, 1);

        let inode_id1 = idgen.next_inode();
        let block_id1 = idgen.next_block();

        assert_eq!(inode_id1.0, 2);
        assert_eq!(block_id1.0, 2);
    }

    #[test]
    fn id_gen_peek() {
        let idgen = IdGen::new(1, 1);
        let _inode_id = idgen.next_inode();
        let _block_id = idgen.next_block();
        let next_inode_id = idgen.peek_inode();
        let next_block_id = idgen.peek_block();

        assert_eq!(next_inode_id, 2);
        assert_eq!(next_block_id, 2);

        let _inode_id = idgen.next_inode();
        let _block_id = idgen.next_block();
        let next_inode_id = idgen.peek_inode();
        let next_block_id = idgen.peek_block();

        assert_eq!(next_inode_id, 3);
        assert_eq!(next_block_id, 3);
    }

    #[test]
    fn new_v4_not_nil_and_short_hex() {
        let id = DatanodeId::new_v4();
        // Not the all-zero UUID
        assert_ne!(id.to_string(), uuid::Uuid::nil().to_string());
        // short() is exactly 8 lowercase hex chars
        let s = id.short();
        assert_eq!(s.len(), 8);
        assert!(
            s.chars()
                .all(|c| c.is_ascii_hexdigit() && (c.is_ascii_digit() || c.is_ascii_lowercase()))
        );
    }

    #[test]
    fn display_is_hyphenated_uuid() {
        let id = DatanodeId::from_str(FIXED).unwrap();
        // Display uses Uuid's canonical hyphenated form
        assert_eq!(id.to_string(), FIXED);
        // Debug contains the UUID string (derive Debug)
        assert!(format!("{:?}", id).contains(FIXED));
    }

    #[test]
    fn fromstr_parses_and_trims() {
        // exact
        let id1 = DatanodeId::from_str(FIXED).unwrap();
        // with surrounding whitespace/newline
        let id2 = DatanodeId::from_str(&format!("  {}\n", FIXED)).unwrap();
        assert_eq!(id1, id2);

        // bad strings fail
        assert!(DatanodeId::from_str("not-a-uuid").is_err());
        assert!(DatanodeId::from_str("").is_err());
    }

    #[test]
    fn short_matches_simple_prefix() {
        let uuid = uuid::Uuid::parse_str(FIXED).unwrap();
        let id = DatanodeId(uuid);
        // Compute the 32-char lowercase hex without hyphens, then compare prefix
        let mut buf = [0u8; uuid::fmt::Simple::LENGTH]; // 32
        let simple = uuid.as_simple().encode_lower(&mut buf);
        assert_eq!(id.short(), simple[..8]);
    }

    #[test]
    fn serde_json_roundtrip_transparent() {
        let id = DatanodeId::from_str(FIXED).unwrap();
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Holder {
            dn: DatanodeId,
        }

        let h = Holder { dn: id };
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!(r#"{{"dn":"{}"}}"#, FIXED));

        let back: Holder = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn serde_toml_roundtrip_transparent() {
        let id = DatanodeId::from_str(FIXED).unwrap();
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Holder {
            dn: DatanodeId,
        }

        let h = Holder { dn: id };
        let toml_s = toml::to_string(&h).unwrap();
        // Top-level TOML must be a table; value is a bare string
        assert_eq!(toml_s.trim(), format!(r#"dn = "{}""#, FIXED));

        let back: Holder = toml::from_str(&toml_s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn conversions_to_from_uuid() {
        let u = uuid::Uuid::new_v4();
        let d: DatanodeId = u.into();
        let u2: uuid::Uuid = d.into();
        assert_eq!(u, u2);
    }
}
