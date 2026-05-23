use std::time::Duration;

/// Timing details captured for one loaded snapshot path.
#[derive(Debug, Clone)]
pub struct FileTimingDetail {
    pub file_name: String,
    pub file_index: usize,
    pub file_io_duration: Duration,
    pub json_parsing_duration: Duration,
    pub arrow_conversion_duration: Duration,
    pub total_duration: Duration,
    pub object_count: usize,
}
