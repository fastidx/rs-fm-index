use std::env;

pub const EXTERNAL_SA_THRESHOLD_BYTES: usize = 512 * 1024 * 1024;
pub const DEFAULT_EXTERNAL_SA_MEM_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaBackendKind {
    /// Preserve existing behavior:
    /// - honor FM_INDEX_EXTERNAL_SA when set
    /// - otherwise use threshold-based selection
    Auto,
    /// In-memory SA via cdivsufsort (i32-indexed).
    DivSufSort32,
    /// Existing external SA implementation.
    External,
    /// In-memory 64-bit SA via libsais.
    LibSais64,
}

impl Default for SaBackendKind {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SaBuildConfig {
    pub kind: SaBackendKind,
    /// Optional explicit memory limit for the external backend.
    /// If None, resolves from env/default.
    pub external_mem_limit_bytes: Option<usize>,
}

impl SaBuildConfig {
    pub fn resolved_kind(&self, indexed_len: usize) -> SaBackendKind {
        match self.kind {
            SaBackendKind::Auto => {
                if let Ok(value) = env::var("FM_INDEX_EXTERNAL_SA") {
                    if value == "1" || value.eq_ignore_ascii_case("true") {
                        SaBackendKind::External
                    } else {
                        SaBackendKind::DivSufSort32
                    }
                } else if indexed_len >= EXTERNAL_SA_THRESHOLD_BYTES {
                    SaBackendKind::External
                } else {
                    SaBackendKind::DivSufSort32
                }
            }
            kind => kind,
        }
    }

    pub fn resolved_external_mem_limit_bytes(&self) -> usize {
        if let Some(mem) = self.external_mem_limit_bytes {
            return mem.max(1);
        }
        if let Ok(value) = env::var("FM_INDEX_EXTERNAL_SA_MEM_BYTES") {
            if let Ok(parsed) = value.parse::<usize>() {
                return parsed.max(1);
            }
        }
        DEFAULT_EXTERNAL_SA_MEM_BYTES
    }
}
