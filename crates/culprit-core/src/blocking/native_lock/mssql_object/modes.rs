//! Documented schema/common object-lock compatibility. An object lock never proves escalation.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", content = "native", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MssqlObjectLockMode {
    SchemaStability,
    SchemaModification,
    Other(String),
    Unknown(String),
}
impl MssqlObjectLockMode {
    pub fn from_native(mode: &str) -> Self {
        match mode {
            "Sch-S" => Self::SchemaStability,
            "Sch-M" => Self::SchemaModification,
            "S" | "U" | "X" | "IS" | "IU" | "IX" | "SIU" | "SIX" | "UIX" | "BU" | "RangeS-S" | "RangeS-U"
            | "RangeI-N" | "RangeI-S" | "RangeI-U" | "RangeI-X" | "RangeX-S" | "RangeX-U" | "RangeX-X" => {
                Self::Other(mode.into())
            }
            _ => Self::Unknown(mode.into()),
        }
    }
    pub fn native(&self) -> &str {
        match self {
            Self::SchemaStability => "Sch-S",
            Self::SchemaModification => "Sch-M",
            Self::Other(mode) | Self::Unknown(mode) => mode,
        }
    }
    pub fn is_valid(&self) -> bool {
        text(self.native(), 60, false)
            && match self {
                Self::Other(mode) => matches!(Self::from_native(mode), Self::Other(_)),
                Self::Unknown(mode) => matches!(Self::from_native(mode), Self::Unknown(_)),
                _ => true,
            }
    }
    pub fn conflicts_with(&self, other: &Self) -> Option<bool> {
        match (self, other) {
            (Self::Unknown(_), _) | (_, Self::Unknown(_)) => None,
            (Self::SchemaModification, _) | (_, Self::SchemaModification) => Some(true),
            (Self::SchemaStability, _) | (_, Self::SchemaStability) => Some(false),
            (Self::Other(requested), Self::Other(granted)) => ordinary_modes_conflict(requested, granted),
        }
    }
}

// Microsoft's common-mode compatibility table; additional/range modes keep unknown conflict semantics.
// https://learn.microsoft.com/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide#lock-compatibility
fn ordinary_modes_conflict(requested: &str, granted: &str) -> Option<bool> {
    const MODES: [&str; 6] = ["IS", "S", "U", "IX", "SIX", "X"];
    if !MODES.contains(&requested) || !MODES.contains(&granted) {
        return None;
    }
    let compatible = match requested {
        "IS" => matches!(granted, "IS" | "S" | "U" | "IX" | "SIX"),
        "S" => matches!(granted, "IS" | "S" | "U"),
        "U" => matches!(granted, "IS" | "S"),
        "IX" => matches!(granted, "IS" | "IX"),
        "SIX" => granted == "IS",
        _ => false, // X conflicts with every common mode.
    };
    Some(!compatible)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_schema_and_common_mode_conflicts_are_symmetric_and_unknowns_stay_unknown() {
        let modes = ["IS", "S", "U", "IX", "SIX", "X"];
        let compatible = [
            [true, true, true, true, true, false],
            [true, true, true, false, false, false],
            [true, true, false, false, false, false],
            [true, false, false, true, false, false],
            [true, false, false, false, false, false],
            [false, false, false, false, false, false],
        ];
        for (row, requested) in modes.iter().enumerate() {
            let requested = MssqlObjectLockMode::from_native(requested);
            for (column, granted) in modes.iter().enumerate() {
                let granted = MssqlObjectLockMode::from_native(granted);
                assert_eq!(requested.conflicts_with(&granted), Some(!compatible[row][column]));
                assert_eq!(requested.conflicts_with(&granted), granted.conflicts_with(&requested));
            }
            for (schema, conflict) in [("Sch-M", true), ("Sch-S", false)] {
                let schema = MssqlObjectLockMode::from_native(schema);
                assert_eq!(requested.conflicts_with(&schema), Some(conflict));
                assert_eq!(schema.conflicts_with(&requested), Some(conflict));
            }
        }
        let intent = MssqlObjectLockMode::from_native("IS");
        for native in ["IU", "BU", "RangeS-S", "unrecognized"] {
            let other = MssqlObjectLockMode::from_native(native);
            assert_eq!(intent.conflicts_with(&other), None);
            assert_eq!(other.conflicts_with(&intent), None);
        }
        for schema in ["Sch-M", "Sch-S"] {
            assert_eq!(
                MssqlObjectLockMode::from_native(schema)
                    .conflicts_with(&MssqlObjectLockMode::from_native("unrecognized")),
                None
            );
        }
    }
}
