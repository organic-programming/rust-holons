//! Parse holon.yaml identity files.

use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Parsed identity from a holon.yaml file.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct HolonIdentity {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub given_name: String,
    #[serde(default)]
    pub family_name: String,
    #[serde(default)]
    pub motto: String,
    #[serde(default)]
    pub composer: String,
    #[serde(default)]
    pub clade: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub born: String,
    #[serde(default)]
    pub lang: String,
    #[serde(default)]
    pub parents: Vec<String>,
    #[serde(default)]
    pub reproduction: String,
    #[serde(default)]
    pub generated_by: String,
    #[serde(default)]
    pub proto_status: String,
    #[serde(default)]
    pub aliases: Vec<String>,
}

impl HolonIdentity {
    /// Return the canonical slug derived from the holon's identity.
    pub fn slug(&self) -> String {
        let given = self.given_name.trim();
        let family = self.family_name.trim().trim_end_matches('?');
        if given.is_empty() && family.is_empty() {
            return String::new();
        }

        format!("{given}-{family}")
            .trim()
            .to_lowercase()
            .replace(' ', "-")
            .trim_matches('-')
            .to_string()
    }
}

/// Parse a holon.yaml file and return its identity.
pub fn parse_holon(path: &Path) -> Result<HolonIdentity, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    let value: serde_yaml::Value = serde_yaml::from_str(&text)?;
    if !value.is_mapping() {
        return Err(format!("{}: holon.yaml must be a YAML mapping", path.display()).into());
    }
    let identity: HolonIdentity = serde_yaml::from_value(value)?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_holon() {
        let dir = std::env::temp_dir();
        let path = dir.join("holon.yaml");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(
            f,
            "uuid: \"abc-123\"\ngiven_name: \"test\"\nfamily_name: \"Test\"\n\
             motto: \"A test.\"\nclade: \"deterministic/pure\"\nlang: \"rust\""
        )
        .unwrap();

        let id = parse_holon(&path).unwrap();
        assert_eq!(id.uuid, "abc-123");
        assert_eq!(id.given_name, "test");
        assert_eq!(id.lang, "rust");

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_parse_invalid_mapping() {
        let dir = std::env::temp_dir();
        let path = dir.join("invalid_holon_rust.yaml");
        fs::write(&path, "- not\n- a\n- mapping\n").unwrap();
        assert!(parse_holon(&path).is_err());
        fs::remove_file(&path).unwrap();
    }
}
