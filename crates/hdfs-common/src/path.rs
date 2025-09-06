use crate::error::HdfsError;
use crate::error::Result;
use serde::{Deserialize, Serialize};

const MAX_NAME_LEN: usize = 255;
const MAX_PATH_LEN: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PathAbs(String);

impl core::fmt::Display for PathAbs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&str> for PathAbs {
    type Error = HdfsError;
    fn try_from(value: &str) -> Result<Self> {
        Ok(PathAbs(normalize(value)?))
    }
}

impl AsRef<str> for PathAbs {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for PathAbs {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PathAbs {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }

    pub fn name(&self) -> &str {
        if self.is_root() {
            "/"
        } else {
            self.0.rsplit('/').next().unwrap()
        }
    }
}

fn has_forbidden(ch: char) -> bool {
    ch == '\0' || (ch.is_control() || ch == '\u{F7}')
}

pub fn normalize(input: &str) -> Result<String> {
    if !input.starts_with('/') {
        return Err(HdfsError::InvalidPath {
            path: input.into(),
            reason: "must be absolut",
        });
    }

    if input.chars().any(has_forbidden) {
        return Err(HdfsError::InvalidPath {
            path: input.into(),
            reason: "control character not allowed",
        });
    }

    let mut stack: Vec<&str> = Vec::new();

    for seg in input.split('/') {
        match seg {
            "" | "." => continue,
            ".." => {
                stack.pop();
            }
            other => {
                if other.as_bytes().len() > MAX_NAME_LEN {
                    return Err(HdfsError::InvalidPath {
                        path: input.into(),
                        reason: "segement too long",
                    });
                }
                stack.push(other);
            }
        }
    }

    let out = if stack.is_empty() {
        "/".to_string()
    } else {
        let mut s = String::with_capacity(input.len().min(MAX_PATH_LEN));
        s.push('/');
        s.push_str(&stack.join("/"));
        s
    };

    if out.as_bytes().len() > MAX_PATH_LEN {
        return Err(HdfsError::InvalidPath {
            path: input.into(),
            reason: "path too long",
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use crate::error::HdfsError;

    fn assert_ok(input: &str, expected: &str) {
        let p = PathAbs::try_from(input).unwrap();
        assert_eq!(p.as_ref(), expected);
        assert_eq!(p.as_str(), expected);
        assert_eq!(p.to_string(), expected);
    }

    fn assert_err_reason(input: &str, expected_reason: &str) {
        match PathAbs::try_from(input) {
            Err(HdfsError::InvalidPath { path, reason }) => {
                assert_eq!(path, input);
                assert_eq!(reason, expected_reason);
            }
            other => panic!("expected InvalidPath error, got: {:?}", other),
        }
    }

    #[test]
    fn normalize_basics() {
        assert_ok("/", "/");
        assert_ok("/a/b", "/a/b");
        assert_ok("/a//b///c/", "/a/b/c");
        assert_ok("/a/./b/.//c", "/a/b/c");
        assert_ok("/a/b/../c", "/a/c");
        assert_ok("/../a", "/a");
        assert_ok("/a/../../..", "/");
    }

    #[test]
    fn rejects_relativ_and_bad_chars() {
        assert_err_reason("a/b", "must be absolut");
        assert_err_reason("", "must be absolut");
        assert_err_reason("/a/\u{1}", "control character not allowed");
    }

    #[test]
    fn segment_and_path_length_limits() {
        let too_long_seg = "a".repeat(MAX_NAME_LEN + 1);
        let input = format!("/{too_long_seg}");
        assert_err_reason(&input, "segement too long");

        let repeats = MAX_PATH_LEN / 2 + 16;
        let input = format!(
            "/{}",
            std::iter::repeat("a/").take(repeats).collect::<String>()
        );
        assert_err_reason(&input, "path too long");
    }

    #[test]
    fn name_and_is_root_helpers() {
        let root = PathAbs::try_from("/").unwrap();
        assert!(root.is_root());
        assert_eq!(root.name(), "/");

        let p = PathAbs::try_from("/a/b").unwrap();
        assert!(!p.is_root());
        assert_eq!(p.name(), "b");

        let single = PathAbs::try_from("/a").unwrap();
        assert_eq!(single.name(), "a");
    }

    #[test]
    fn asref_and_deref_work_like_str() {
        let p = PathAbs::try_from("/x/y").unwrap();

        fn takes_str<S: AsRef<str>>(s: S) -> usize {
            s.as_ref().len()
        }
        assert_eq!(takes_str(&p), 4);
        assert_eq!(takes_str(p.as_ref()), 4);

        fn starts_with_slash(s: &str) -> bool {
            s.starts_with('/')
        }
        assert!(starts_with_slash(&p));
    }

    #[test]
    fn normalize_is_idempotent() {
        let s = "/a//b/../c/./d/";
        let n1 = normalize(s).unwrap();
        let n2 = normalize(&n1).unwrap();
        assert_eq!(n1, n2);
    }
}
