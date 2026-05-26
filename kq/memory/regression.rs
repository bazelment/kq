use anyhow::{Context, Result};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemorySample {
    pub heap_allocated: u64,
    pub jemalloc_resident: u64,
    pub process_rss: u64,
    pub process_high_water: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeakMemory {
    pub heap_allocated: u64,
    pub jemalloc_resident: u64,
    pub process_rss: u64,
    pub process_high_water: u64,
    pub samples: usize,
}

impl PeakMemory {
    pub fn observe(&mut self, sample: MemorySample) {
        self.heap_allocated = self.heap_allocated.max(sample.heap_allocated);
        self.jemalloc_resident = self.jemalloc_resident.max(sample.jemalloc_resident);
        self.process_rss = self.process_rss.max(sample.process_rss);
        self.process_high_water = self.process_high_water.max(sample.process_high_water);
        self.samples += 1;
    }
}

pub struct MemorySampler {
    running: Arc<AtomicBool>,
    peak: Arc<Mutex<PeakMemory>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MemorySampler {
    pub fn start(interval: Duration) -> Result<Self> {
        if interval.is_zero() {
            anyhow::bail!("memory sample interval must be greater than zero");
        }

        let running = Arc::new(AtomicBool::new(true));
        let peak = Arc::new(Mutex::new(PeakMemory::default()));
        let thread_running = Arc::clone(&running);
        let thread_peak = Arc::clone(&peak);

        let handle = thread::spawn(move || {
            while thread_running.load(Ordering::Relaxed) {
                if let Ok(sample) = read_memory_sample() {
                    if let Ok(mut peak) = thread_peak.lock() {
                        peak.observe(sample);
                    }
                }
                thread::sleep(interval);
            }

            if let Ok(sample) = read_memory_sample() {
                if let Ok(mut peak) = thread_peak.lock() {
                    peak.observe(sample);
                }
            }
        });

        Ok(Self {
            running,
            peak,
            handle: Some(handle),
        })
    }

    pub fn stop(mut self) -> Result<PeakMemory> {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("memory sampler thread panicked"))?;
        }

        self.peak
            .lock()
            .map(|peak| *peak)
            .map_err(|_| anyhow::anyhow!("memory sampler lock poisoned"))
    }
}

impl Drop for MemorySampler {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryThresholds {
    pub max_peak_rss_mb: Option<f64>,
    pub max_peak_heap_mb: Option<f64>,
    pub max_peak_jemalloc_resident_mb: Option<f64>,
}

impl MemoryThresholds {
    pub fn check(&self, peak: PeakMemory) -> Result<()> {
        check_threshold("peak RSS", peak.process_rss, self.max_peak_rss_mb)?;
        check_threshold(
            "peak heap allocated",
            peak.heap_allocated,
            self.max_peak_heap_mb,
        )?;
        check_threshold(
            "peak jemalloc resident",
            peak.jemalloc_resident,
            self.max_peak_jemalloc_resident_mb,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MemoryBenchmarkSummary {
    snapshots: Vec<PathBuf>,
    generated_format: String,
    table_count: usize,
    total_rows: usize,
    duration: Duration,
    baseline: MemorySample,
    peak: PeakMemory,
    final_sample: MemorySample,
}

impl MemoryBenchmarkSummary {
    pub fn new(
        snapshots: &[PathBuf],
        generated_format: impl Into<String>,
        table_count: usize,
        total_rows: usize,
        duration: Duration,
        baseline: MemorySample,
        peak: PeakMemory,
        final_sample: MemorySample,
    ) -> Self {
        Self {
            snapshots: snapshots.to_vec(),
            generated_format: generated_format.into(),
            table_count,
            total_rows,
            duration,
            baseline,
            peak,
            final_sample,
        }
    }

    pub fn print(&self) {
        println!();
        println!("summary");
        println!("  snapshot_count: {}", self.snapshots.len());
        println!("  table_count: {}", self.table_count);
        println!("  total_rows: {}", self.total_rows);
        println!("  total_time_s: {:.3}", self.duration.as_secs_f64());
        println!("  samples: {}", self.peak.samples);
        println!(
            "  peak_process_rss_mb: {:.2}",
            bytes_to_mb(self.peak.process_rss)
        );
        println!(
            "  peak_process_hwm_mb: {:.2}",
            bytes_to_mb(self.peak.process_high_water)
        );
        println!(
            "  peak_heap_allocated_mb: {:.2}",
            bytes_to_mb(self.peak.heap_allocated)
        );
        println!(
            "  peak_jemalloc_resident_mb: {:.2}",
            bytes_to_mb(self.peak.jemalloc_resident)
        );
        println!(
            "  final_process_rss_mb: {:.2}",
            bytes_to_mb(self.final_sample.process_rss)
        );
        println!(
            "  final_heap_allocated_mb: {:.2}",
            bytes_to_mb(self.final_sample.heap_allocated)
        );
        println!(
            "  rss_delta_mb: {:.2}",
            bytes_to_mb(
                self.final_sample
                    .process_rss
                    .saturating_sub(self.baseline.process_rss)
            )
        );
        println!(
            "  heap_delta_mb: {:.2}",
            bytes_to_mb(
                self.final_sample
                    .heap_allocated
                    .saturating_sub(self.baseline.heap_allocated)
            )
        );
        println!(
            "  peak_rss_bytes_per_row: {:.1}",
            bytes_per_row(self.peak.process_rss, self.total_rows)
        );
        println!(
            "  peak_heap_bytes_per_row: {:.1}",
            bytes_per_row(self.peak.heap_allocated, self.total_rows)
        );
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        std::fs::write(path, serde_json::to_string_pretty(&self.to_json_value())?)
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("  json_output: {}", path.display());
        Ok(())
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        json!({
            "snapshot_count": self.snapshots.len(),
            "snapshots": self.snapshots.iter().map(|path| path.display().to_string()).collect::<Vec<_>>(),
            "generated_format": self.generated_format,
            "table_count": self.table_count,
            "total_rows": self.total_rows,
            "total_time_s": self.duration.as_secs_f64(),
            "samples": self.peak.samples,
            "baseline": memory_json(self.baseline),
            "peak": peak_json(self.peak),
            "final": memory_json(self.final_sample),
            "rss_delta_bytes": self.final_sample.process_rss.saturating_sub(self.baseline.process_rss),
            "heap_delta_bytes": self.final_sample.heap_allocated.saturating_sub(self.baseline.heap_allocated),
            "peak_rss_bytes_per_row": bytes_per_row(self.peak.process_rss, self.total_rows),
            "peak_heap_bytes_per_row": bytes_per_row(self.peak.heap_allocated, self.total_rows),
        })
    }
}

pub fn read_memory_sample() -> Result<MemorySample> {
    use jemalloc_ctl::{epoch, stats};

    epoch::advance().map_err(|e| anyhow::anyhow!("failed to advance jemalloc epoch: {e}"))?;
    let heap_allocated = stats::allocated::read()
        .map_err(|e| anyhow::anyhow!("failed to read jemalloc allocated memory: {e}"))?
        as u64;
    let jemalloc_resident = stats::resident::read()
        .map_err(|e| anyhow::anyhow!("failed to read jemalloc resident memory: {e}"))?
        as u64;
    let (process_rss, process_high_water) = read_proc_status_memory()?;

    Ok(MemorySample {
        heap_allocated,
        jemalloc_resident,
        process_rss,
        process_high_water,
    })
}

fn read_proc_status_memory() -> Result<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status")
        .context("failed to read /proc/self/status")?;
    parse_proc_status_memory(&status)
}

fn parse_proc_status_memory(status: &str) -> Result<(u64, u64)> {
    let mut rss = 0;
    let mut high_water = 0;

    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            rss = parse_status_kb(value)?;
        } else if let Some(value) = line.strip_prefix("VmHWM:") {
            high_water = parse_status_kb(value)?;
        }
    }

    Ok((rss, high_water))
}

fn parse_status_kb(value: &str) -> Result<u64> {
    let kb = value
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing /proc status memory value"))?
        .parse::<u64>()
        .context("failed to parse /proc status memory value")?;
    Ok(kb * 1024)
}

fn check_threshold(label: &str, actual_bytes: u64, limit_mb: Option<f64>) -> Result<()> {
    let Some(limit_mb) = limit_mb else {
        return Ok(());
    };
    let actual_mb = bytes_to_mb(actual_bytes);
    if actual_mb > limit_mb {
        anyhow::bail!("{label} exceeded threshold: {actual_mb:.2} MB > {limit_mb:.2} MB");
    }
    Ok(())
}

fn memory_json(sample: MemorySample) -> serde_json::Value {
    json!({
        "heap_allocated_bytes": sample.heap_allocated,
        "jemalloc_resident_bytes": sample.jemalloc_resident,
        "process_rss_bytes": sample.process_rss,
        "process_high_water_bytes": sample.process_high_water,
    })
}

fn peak_json(peak: PeakMemory) -> serde_json::Value {
    json!({
        "heap_allocated_bytes": peak.heap_allocated,
        "jemalloc_resident_bytes": peak.jemalloc_resident,
        "process_rss_bytes": peak.process_rss,
        "process_high_water_bytes": peak.process_high_water,
    })
}

fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

fn bytes_per_row(bytes: u64, rows: usize) -> f64 {
    if rows == 0 {
        0.0
    } else {
        bytes as f64 / rows as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peak_memory_tracks_max_values_and_sample_count() {
        let mut peak = PeakMemory::default();

        peak.observe(MemorySample {
            heap_allocated: 10,
            jemalloc_resident: 20,
            process_rss: 30,
            process_high_water: 40,
        });
        peak.observe(MemorySample {
            heap_allocated: 15,
            jemalloc_resident: 15,
            process_rss: 35,
            process_high_water: 25,
        });

        assert_eq!(
            peak,
            PeakMemory {
                heap_allocated: 15,
                jemalloc_resident: 20,
                process_rss: 35,
                process_high_water: 40,
                samples: 2,
            }
        );
    }

    #[test]
    fn proc_status_parser_reads_rss_and_high_water_bytes() {
        let status = "\
Name:\tkq
VmHWM:\t  8192 kB
VmRSS:\t  4096 kB
Threads:\t1
";

        let (rss, high_water) = parse_proc_status_memory(status).unwrap();

        assert_eq!(rss, 4096 * 1024);
        assert_eq!(high_water, 8192 * 1024);
    }

    #[test]
    fn proc_status_parser_reports_malformed_memory_values() {
        let status = "VmRSS:\tunknown kB\n";

        let err = parse_proc_status_memory(status).unwrap_err();

        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn sampler_rejects_zero_interval() {
        match MemorySampler::start(Duration::ZERO) {
            Ok(_) => panic!("zero sample interval should be rejected"),
            Err(err) => assert!(err.to_string().contains("greater than zero")),
        }
    }

    #[test]
    fn thresholds_fail_when_peak_exceeds_configured_limit() {
        let thresholds = MemoryThresholds {
            max_peak_rss_mb: Some(1.0),
            max_peak_heap_mb: None,
            max_peak_jemalloc_resident_mb: None,
        };
        let peak = PeakMemory {
            process_rss: 2 * 1024 * 1024,
            ..Default::default()
        };

        let err = thresholds.check(peak).unwrap_err();

        assert!(err.to_string().contains("peak RSS exceeded threshold"));
    }

    #[test]
    fn thresholds_allow_equal_limit() {
        let thresholds = MemoryThresholds {
            max_peak_rss_mb: Some(1.0),
            max_peak_heap_mb: Some(1.0),
            max_peak_jemalloc_resident_mb: Some(1.0),
        };
        let peak = PeakMemory {
            heap_allocated: 1024 * 1024,
            jemalloc_resident: 1024 * 1024,
            process_rss: 1024 * 1024,
            ..Default::default()
        };

        thresholds.check(peak).unwrap();
    }

    #[test]
    fn summary_json_preserves_deltas_and_zero_row_ratios() {
        let summary = MemoryBenchmarkSummary::new(
            &[PathBuf::from("/tmp/snapshot")],
            "ipc",
            3,
            0,
            Duration::from_millis(1500),
            MemorySample {
                heap_allocated: 600,
                process_rss: 1_000,
                ..Default::default()
            },
            PeakMemory {
                heap_allocated: 2_000,
                process_rss: 4_000,
                samples: 7,
                ..Default::default()
            },
            MemorySample {
                heap_allocated: 500,
                process_rss: 1_500,
                ..Default::default()
            },
        );

        let json = summary.to_json_value();

        assert_eq!(json["snapshot_count"], 1);
        assert_eq!(json["generated_format"], "ipc");
        assert_eq!(json["samples"], 7);
        assert_eq!(json["rss_delta_bytes"], 500);
        assert_eq!(json["heap_delta_bytes"], 0);
        assert_eq!(json["peak_rss_bytes_per_row"], 0.0);
        assert_eq!(json["peak"]["process_rss_bytes"], 4_000);
    }

    #[test]
    fn summary_writes_json_to_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("nested/summary.json");
        let summary = MemoryBenchmarkSummary::new(
            &[],
            "ndjson",
            0,
            1,
            Duration::from_secs(0),
            MemorySample::default(),
            PeakMemory::default(),
            MemorySample::default(),
        );

        summary.write_json(&output).unwrap();

        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(written["generated_format"], "ndjson");
        assert_eq!(written["peak_heap_bytes_per_row"], 0.0);
    }
}
