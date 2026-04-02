use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestConfigFile {
    pub input_patterns: Option<Vec<String>>,
    pub output_dir: Option<PathBuf>,
    pub chunk_size: Option<SizeValue>,
    pub read_buffer: Option<SizeValue>,
    pub num_workers: Option<usize>,
    pub sample_rate: Option<u32>,
    pub wavelet_mode: Option<String>,
    pub wavelet_max_bytes: Option<SizeValue>,
    pub scratch_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SizeValue {
    String(String),
    Number(u64),
}

impl IngestConfigFile {
    pub fn load(path: &Path) -> Result<Self> {
        let data = fs::read_to_string(path).with_context(|| format!("Failed to read {path:?}"))?;
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();

        match ext.as_str() {
            "json" => {
                let cfg: IngestConfigFile =
                    serde_json::from_str(&data).context("Invalid JSON config")?;
                Ok(cfg)
            }
            "toml" | "tml" => {
                let cfg: IngestConfigFile = toml::from_str(&data).context("Invalid TOML config")?;
                Ok(cfg)
            }
            _ => anyhow::bail!("Config must be .json or .toml"),
        }
    }
}

pub fn size_value_to_usize(value: &SizeValue) -> Result<usize> {
    match value {
        SizeValue::Number(n) => usize::try_from(*n).context("Size exceeds usize"),
        SizeValue::String(s) => parse_size(s),
    }
}

pub fn parse_size(input: &str) -> Result<usize> {
    let s = input.trim();
    if s.is_empty() {
        anyhow::bail!("size cannot be empty");
    }

    let (num_part, unit_part) = s
        .chars()
        .position(|c| !c.is_ascii_digit())
        .map(|idx| s.split_at(idx))
        .unwrap_or((s, ""));

    let value: u64 = num_part
        .parse()
        .with_context(|| format!("invalid size: {}", input))?;

    let unit = unit_part.trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1_000u64,
        "m" | "mb" => 1_000_000u64,
        "g" | "gb" => 1_000_000_000u64,
        "t" | "tb" => 1_000_000_000_000u64,
        "kib" => 1024u64,
        "mib" => 1024u64 * 1024,
        "gib" => 1024u64 * 1024 * 1024,
        "tib" => 1024u64 * 1024 * 1024 * 1024,
        _ => anyhow::bail!("invalid size unit: {}", unit_part),
    };

    let bytes = value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("size is too large"))?;
    usize::try_from(bytes).context("size exceeds usize")
}
