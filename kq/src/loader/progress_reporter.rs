//! Unified progress reporting interface
//! Supports both terminal (progress bars) and non-terminal (logging) output

use super::LoadingPhase;
use indicatif::ProgressStyle;
use std::collections::VecDeque;
use std::path::Path;

/// Unified progress reporter trait
/// Implementations can either show progress bars (terminal) or log messages (non-terminal)
pub trait ProgressReporter: Send + Sync {
    /// Initialize the reporter with total file count
    fn init(&mut self, total_files: usize, num_threads: usize);
    
    /// Register a file to be tracked
    fn register_file(&mut self, index: usize, path: &Path);
    
    /// Update the phase for a specific file
    fn update_file_phase(&mut self, file_index: usize, phase: LoadingPhase, percent: u8, message: String);
    
    /// Mark a file as complete
    fn finish_file(&mut self, file_index: usize, duration_secs: f64, object_count: usize, memory_freed: usize);
    
    /// Update merging status
    fn set_merging(&mut self, message: String);
    
    /// Finish merging phase
    fn finish_merging(&mut self, table_count: usize);
    
    /// Finalize the reporter (cleanup, print summary, etc.)
    fn finish(&mut self);
}

/// Terminal-based progress reporter using indicatif progress bars
pub struct TerminalProgressReporter {
    multi_progress: indicatif::MultiProgress,
    overall_bar: indicatif::ProgressBar,
    file_bars: std::collections::HashMap<usize, indicatif::ProgressBar>,
    total_files: usize,
}

impl TerminalProgressReporter {
    pub fn new() -> Self {
        Self {
            multi_progress: indicatif::MultiProgress::new(),
            overall_bar: indicatif::ProgressBar::new(0),
            file_bars: std::collections::HashMap::new(),
            total_files: 0,
        }
    }
}

impl ProgressReporter for TerminalProgressReporter {
    fn init(&mut self, total_files: usize, _num_threads: usize) {
        self.total_files = total_files;
        
        // Create overall progress bar
        let style = bar_style(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} files ({msg})",
            "#>-",
        );
        
        self.overall_bar = self.multi_progress.add(indicatif::ProgressBar::new(total_files as u64));
        self.overall_bar.set_style(style);
        self.overall_bar.set_message("Starting...");
    }
    
    fn register_file(&mut self, index: usize, path: &Path) {
        let short_name = short_path_file_name(path);
        let style = spinner_style("{spinner:.green} {msg}", "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");
        
        let bar = self.multi_progress.add(indicatif::ProgressBar::new_spinner());
        bar.set_style(style);
        bar.set_message(format!("[{}] {}", index, short_name));
        
        self.file_bars.insert(index, bar);
    }
    
    fn update_file_phase(&mut self, file_index: usize, phase: LoadingPhase, _percent: u8, message: String) {
        if let Some(bar) = self.file_bars.get(&file_index) {
            let icon = match phase {
                LoadingPhase::ReadingFile => "📖",
                LoadingPhase::ParsingJSON => "🔍",
                LoadingPhase::ConvertingNodes | 
                LoadingPhase::ConvertingPods |
                LoadingPhase::ConvertingNamespaces |
                LoadingPhase::ConvertingDaemonSets => "⚡",
                LoadingPhase::Finalizing => "✨",
            };
            bar.set_message(format!("{} {}", icon, message));
        }
    }
    
    fn finish_file(&mut self, file_index: usize, duration_secs: f64, object_count: usize, _memory_freed: usize) {
        if let Some(bar) = self.file_bars.get(&file_index) {
            bar.finish_with_message(format!("✓ ({:.1}s, {} objects)", duration_secs, object_count));
        }
        self.overall_bar.inc(1);
        self.overall_bar.set_message(format!("{}/{} files complete", 
            self.overall_bar.position(), self.total_files));
    }
    
    fn set_merging(&mut self, message: String) {
        self.overall_bar.set_message(message);
    }
    
    fn finish_merging(&mut self, table_count: usize) {
        self.overall_bar.finish_with_message(format!("✓ {} tables created", table_count));
    }
    
    fn finish(&mut self) {
        // Progress bars are automatically cleaned up when dropped
    }
}

/// Simplified single-line progress reporter
pub struct SimplifiedProgressReporter {
    single_bar: indicatif::ProgressBar,
    thread_bars: Vec<indicatif::ProgressBar>,
    multi: indicatif::MultiProgress,
    total_files: usize,
    completed_files: usize,
    start_time: std::time::Instant,
    num_threads: usize,
    file_names: std::collections::HashMap<usize, String>,
    thread_assignments: std::collections::HashMap<usize, usize>, // file_index -> thread_index
    thread_to_file: std::collections::HashMap<usize, usize>, // thread_index -> current file_index
    file_progress: std::collections::HashMap<usize, u8>, // file_index -> percent
    completion_times: VecDeque<f64>, // Recent completion times for ETA
}

impl SimplifiedProgressReporter {
    pub fn new() -> Self {
        Self {
            single_bar: indicatif::ProgressBar::new(0),
            thread_bars: Vec::new(),
            multi: indicatif::MultiProgress::new(),
            total_files: 0,
            completed_files: 0,
            start_time: std::time::Instant::now(),
            num_threads: 0,
            file_names: std::collections::HashMap::new(),
            thread_assignments: std::collections::HashMap::new(),
            thread_to_file: std::collections::HashMap::new(),
            file_progress: std::collections::HashMap::new(),
            completion_times: VecDeque::new(),
        }
    }
}

impl ProgressReporter for SimplifiedProgressReporter {
    fn init(&mut self, total_files: usize, num_threads: usize) {
        self.total_files = total_files;
        self.num_threads = num_threads;
        self.completed_files = 0;
        self.start_time = std::time::Instant::now();
        
        // Create single header progress bar
        let style = bar_style(
            "Loading ({pos}/{len}) [{wide_bar}] {percent}% | ETA: {eta_precise}",
            "█░",
        );
        
        self.single_bar = self.multi.add(indicatif::ProgressBar::new(total_files as u64));
        self.single_bar.set_style(style);
        
        // Create thread bars (one per thread)
        self.thread_bars.clear();
        for i in 0..num_threads {
            let thread_style = bar_style(
                &format!("Thread {}: {{spinner:.green}} {{msg}} [{{bar}}] {{percent}}%", i + 1),
                "█░",
            );
            
            let bar = self.multi.add(indicatif::ProgressBar::new(100));
            bar.set_style(thread_style);
            bar.set_message("Waiting...");
            bar.set_position(0);
            self.thread_bars.push(bar);
        }
    }
    
    fn register_file(&mut self, index: usize, path: &Path) {
        let filename = path_file_name(path);
        self.file_names.insert(index, filename);
        self.file_progress.insert(index, 0);
    }
    
    fn update_file_phase(&mut self, file_index: usize, _phase: LoadingPhase, percent: u8, _message: String) {
        // Assign file to a thread slot if not already assigned
        let thread_idx = if let Some(thread_idx) = self.thread_assignments.get(&file_index) {
            *thread_idx
        } else {
            // Find first available thread slot (one that's not currently assigned to a file)
            let mut best_thread = 0;
            let mut best_progress = 100;
            for tid in 0..self.num_threads {
                if let Some(current_file) = self.thread_to_file.get(&tid) {
                    // Thread is busy, check if it's nearly done
                    if let Some(progress) = self.file_progress.get(current_file) {
                        if *progress < best_progress {
                            best_progress = *progress;
                            best_thread = tid;
                        }
                    }
                } else {
                    // Thread is available
                    best_thread = tid;
                    break;
                }
            }
            self.thread_assignments.insert(file_index, best_thread);
            self.thread_to_file.insert(best_thread, file_index);
            best_thread
        };
        
        // Update thread bar - reuse the existing bar, don't create a new one
        if let Some(thread_bar) = self.thread_bars.get_mut(thread_idx) {
            let filename = self.file_names.get(&file_index)
                .cloned()
                .unwrap_or_else(|| format!("file-{}", file_index));
            
            let short_name = short_display_name(&filename);
            
            thread_bar.set_message(short_name);
            thread_bar.set_position(percent as u64);
            
            // Enable spinner for active processing
            let spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";
            thread_bar.enable_steady_tick(std::time::Duration::from_millis(80));
            thread_bar.set_style(
                bar_style(
                    &format!("Thread {}: {{spinner:.green}} {{msg}} [{{bar}}] {{percent}}%", thread_idx + 1),
                    "█░",
                )
                    .tick_chars(spinner_chars)
            );
        }
        
        self.file_progress.insert(file_index, percent);
    }
    
    fn finish_file(&mut self, file_index: usize, duration_secs: f64, _object_count: usize, _memory_freed: usize) {
        // Find thread that was working on this file
        if let Some(thread_idx) = self.thread_assignments.remove(&file_index) {
            self.thread_to_file.remove(&thread_idx);
            
            // Reset the existing thread bar for reuse (don't create a new one)
            if let Some(thread_bar) = self.thread_bars.get_mut(thread_idx) {
                // Reset the bar for reuse (don't finish it - that would create duplicate lines)
                // Just reset position and message, keeping the same bar instance
                thread_bar.reset();
                thread_bar.set_length(100);
                thread_bar.set_position(0);
                thread_bar.set_message("Waiting...");
                
                // Re-enable spinner and style (reset() may have cleared these)
                let spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";
                thread_bar.enable_steady_tick(std::time::Duration::from_millis(80));
                thread_bar.set_style(
                    bar_style(
                        &format!("Thread {}: {{spinner:.green}} {{msg}} [{{bar}}] {{percent}}%", thread_idx + 1),
                        "█░",
                    )
                        .tick_chars(spinner_chars)
                );
            }
        }
        
        // Update completion tracking for ETA
        self.completion_times.push_back(duration_secs);
        if self.completion_times.len() > 10 {
            self.completion_times.pop_front();
        }
        
        // Update overall progress bar
        self.completed_files += 1;
        self.single_bar.set_position(self.completed_files as u64);
        
        // ETA is automatically calculated by indicatif based on rate.
    }
    
    fn set_merging(&mut self, message: String) {
        self.single_bar.set_message(message);
    }
    
    fn finish_merging(&mut self, table_count: usize) {
        self.single_bar.finish_with_message(format!("✓ {} tables created", table_count));
    }
    
    fn finish(&mut self) {
        // Clean up thread bars
        for bar in &self.thread_bars {
            bar.finish();
        }
    }
}

/// Logging-based progress reporter for non-terminal output
pub struct LoggingProgressReporter {
    total_files: usize,
    file_names: std::collections::HashMap<usize, String>,
    start_time: std::time::Instant,
}

impl LoggingProgressReporter {
    pub fn new() -> Self {
        Self {
            total_files: 0,
            file_names: std::collections::HashMap::new(),
            start_time: std::time::Instant::now(),
        }
    }
}

impl ProgressReporter for LoggingProgressReporter {
    fn init(&mut self, total_files: usize, num_threads: usize) {
        self.total_files = total_files;
        self.start_time = std::time::Instant::now();
        tracing::info!("Loading {} files with {} threads", total_files, num_threads);
    }
    
    fn register_file(&mut self, index: usize, path: &Path) {
        let filename = path_file_name(path);
        self.file_names.insert(index, filename);
    }
    
    fn update_file_phase(&mut self, file_index: usize, phase: LoadingPhase, _percent: u8, message: String) {
        if let Some(filename) = self.file_names.get(&file_index) {
            tracing::debug!("[{}] {}: {}", filename, phase.description(), message);
        }
    }
    
    fn finish_file(&mut self, file_index: usize, duration_secs: f64, object_count: usize, _memory_freed: usize) {
        if let Some(filename) = self.file_names.get(&file_index) {
            tracing::info!("[{}] Complete: {:.2}s, {} objects", filename, duration_secs, object_count);
        }
    }
    
    fn set_merging(&mut self, message: String) {
        tracing::info!("{}", message);
    }
    
    fn finish_merging(&mut self, table_count: usize) {
        tracing::info!("Merging complete: {} tables created", table_count);
    }
    
    fn finish(&mut self) {
        let total_duration = self.start_time.elapsed();
        tracing::info!("All files loaded in {:.2}s", total_duration.as_secs_f64());
    }
}

fn path_file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

fn short_path_file_name(path: &Path) -> String {
    short_display_name(&path_file_name(path))
}

fn short_display_name(filename: &str) -> String {
    if filename.len() > 30 {
        format!("{}...{}", &filename[..20], &filename[filename.len() - 7..])
    } else {
        filename.to_string()
    }
}

fn bar_style(template: &str, progress_chars: &str) -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(template)
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars(progress_chars)
}

fn spinner_style(template: &str, tick_chars: &'static str) -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template(template)
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_chars(tick_chars)
}

/// Create appropriate progress reporter based on terminal availability
pub fn create_progress_reporter(enable_progress: bool, simple: bool) -> Box<dyn ProgressReporter> {
    use std::io::IsTerminal;
    if enable_progress && std::io::stderr().is_terminal() {
        if simple {
            Box::new(SimplifiedProgressReporter::new())
        } else {
            Box::new(TerminalProgressReporter::new())
        }
    } else {
        Box::new(LoggingProgressReporter::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_display_name_keeps_short_names() {
        assert_eq!(short_display_name("snapshot.json.gz"), "snapshot.json.gz");
    }

    #[test]
    fn short_display_name_preserves_start_and_extension_for_long_names() {
        let short = short_display_name(
            "this-is-a-very-long-snapshot-file-name-that-needs-truncation.json.gz",
        );

        assert_eq!(short, "this-is-a-very-long-...json.gz");
    }

    #[test]
    fn progress_style_helpers_fall_back_for_invalid_templates() {
        let _bar = bar_style("{not a valid template", "#>-");
        let _spinner = spinner_style("{not a valid template", "-\\|/");
    }

    #[test]
    fn logging_reporter_tracks_registered_files() {
        let mut reporter = LoggingProgressReporter::new();

        reporter.init(2, 1);
        reporter.register_file(1, Path::new("/tmp/snapshot-a.json.gz"));
        reporter.update_file_phase(
            1,
            LoadingPhase::ReadingFile,
            10,
            "reading".to_string(),
        );
        reporter.finish_file(1, 0.2, 42, 0);
        reporter.finish();

        assert_eq!(reporter.total_files, 2);
        assert_eq!(
            reporter.file_names.get(&1).map(String::as_str),
            Some("snapshot-a.json.gz")
        );
    }

    #[test]
    fn simplified_reporter_reuses_thread_after_file_finishes() {
        let mut reporter = SimplifiedProgressReporter::new();

        reporter.init(2, 1);
        reporter.register_file(1, Path::new("/tmp/first.json.gz"));
        reporter.register_file(2, Path::new("/tmp/second.json.gz"));
        reporter.update_file_phase(
            1,
            LoadingPhase::ReadingFile,
            50,
            "reading first".to_string(),
        );
        reporter.finish_file(1, 0.5, 10, 0);
        reporter.update_file_phase(
            2,
            LoadingPhase::ParsingJSON,
            25,
            "reading second".to_string(),
        );
        reporter.finish_file(2, 0.4, 20, 0);
        reporter.finish();

        assert_eq!(reporter.completed_files, 2);
        assert!(reporter.thread_to_file.is_empty());
        assert_eq!(reporter.completion_times.len(), 2);
    }
}
