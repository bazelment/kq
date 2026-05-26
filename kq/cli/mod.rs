use clap::ValueEnum;

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
pub enum OutputFormat {
    /// Pretty-printed table format
    Table,
    /// JSON format
    Json,
    /// CSV format
    Csv,
    /// Tab-separated values
    Tsv,
    /// Compact table format
    Compact,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Tsv => write!(f, "tsv"),
            OutputFormat::Compact => write!(f, "compact"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the canonical CLI spellings for `--format`. Every spelling here is
    /// already in user-facing docs and saved scripts; a silent rename would
    /// break those without any compiler-visible signal.
    #[test]
    fn output_format_round_trips_through_value_enum() {
        let cases = [
            ("table", OutputFormat::Table),
            ("json", OutputFormat::Json),
            ("csv", OutputFormat::Csv),
            ("tsv", OutputFormat::Tsv),
            ("compact", OutputFormat::Compact),
        ];
        for (spelling, expected) in cases {
            let parsed = OutputFormat::from_str(spelling, true).unwrap_or_else(|err| {
                panic!("clap rejected --format={spelling}: {err}")
            });
            assert_eq!(parsed, expected, "from_str({spelling}) returned wrong variant");
            assert_eq!(
                expected.to_string(),
                spelling,
                "Display impl for {expected:?} drifted from --format spelling"
            );
        }
    }

    #[test]
    fn output_format_rejects_unknown_spelling() {
        // Matches the clap behavior surfaced to users via `--format=bogus`.
        assert!(OutputFormat::from_str("bogus", true).is_err());
    }
}
