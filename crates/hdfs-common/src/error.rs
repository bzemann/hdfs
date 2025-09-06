use crate::ids::BlockId;
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum HdfsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config error ({key}): {msg}")]
    Config { key: &'static str, msg: String },

    #[error("invalid path '{path}': {reason}")]
    InvalidPath { path: String, reason: &'static str },

    #[error("already exists: {path}")]
    AlreadyExists { path: String },

    #[error("not found: {path}")]
    NotFound { path: String },

    #[error("state error ({what}): {details}")]
    State { what: &'static str, details: String },

    #[error("protocol error ({op}): {details}")]
    Protocol { op: &'static str, details: String },

    #[error(
        "checksum mismatch (blk_{block}, chunk {chunk_index}): expected 0x{expected:08X}, got 0x{got:08X}"
    )]
    ChecksumMismatch {
        block: BlockId,
        chunk_index: u64,
        expected: u32,
        got: u32,
    },

    #[error("timeout during {op}: {during}")]
    Timeout {
        op: &'static str,
        during: &'static str,
    },
}

pub type Result<T> = std::result::Result<T, HdfsError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;
    use std::io;

    #[test]
    fn io_from_and_display() {
        let e = HdfsError::from(io::Error::new(io::ErrorKind::NotFound, "file missing"));
        assert!(matches!(e, HdfsError::Io(_)));

        let s = e.to_string();
        assert!(s.contains("io:"));
        assert!(s.contains("file missing"));

        assert!(e.source().unwrap().to_string().contains("file missing"));
    }

    #[test]
    fn config_display() {
        let e = HdfsError::Config {
            key: "default_block_size",
            msg: "must be > 0 and multiple of checksum_chunk_size (512B)".into(),
        };

        assert_eq!(
            e.to_string(),
            "config error (default_block_size): must be > 0 and multiple of checksum_chunk_size (512B)"
        );
    }

    #[test]
    fn invalid_path_display() {
        let e = HdfsError::InvalidPath {
            path: "/a/\u{1}/b".into(),
            reason: "control character not allowed",
        };

        assert_eq!(
            e.to_string(),
            "invalid path '/a/\u{1}/b': control character not allowed"
        );
    }

    #[test]
    fn already_exists_and_not_found() {
        let e1 = HdfsError::AlreadyExists {
            path: "/data/raw".into(),
        };
        let e2 = HdfsError::NotFound {
            path: "/data/raw/file.csv".into(),
        };
        assert_eq!(e1.to_string(), "already exists: /data/raw");
        assert_eq!(e2.to_string(), "not found: /data/raw/file.csv");
    }

    #[test]
    fn state_and_protocol() {
        let st = HdfsError::State {
            what: "complete",
            details: "file not under construction".into(),
        };
        let pr = HdfsError::Protocol {
            op: "AddBlock",
            details: "missing field 'path'".into(),
        };
        assert_eq!(
            st.to_string(),
            "state error (complete): file not under construction"
        );
        assert_eq!(
            pr.to_string(),
            "protocol error (AddBlock): missing field 'path'"
        );
    }

    #[test]
    fn checksum_mismatch_display() {
        let e = HdfsError::ChecksumMismatch {
            block: BlockId(42),
            chunk_index: 7,
            expected: 0xDEADBEEF,
            got: 0xFEEDBEEF,
        };

        assert_eq!(
            e.to_string(),
            "checksum mismatch (blk_42, chunk 7): expected 0xDEADBEEF, got 0xFEEDBEEF"
        );
    }

    #[test]
    fn timeout_display() {
        let e = HdfsError::Timeout {
            op: "WriteChunk",
            during: "client->DN transfer",
        };
        assert_eq!(
            e.to_string(),
            "timeout during WriteChunk: client->DN transfer"
        );
    }
}
