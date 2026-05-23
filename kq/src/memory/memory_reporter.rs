use bytesize::ByteSize;
use colored::*;
use std::collections::HashMap;
use jemalloc_ctl::{epoch, stats};
use std::time::Instant;

/// System memory information
#[derive(Debug, Clone)]
pub struct SystemMemoryInfo {
    pub total_physical: u64,
    pub available_physical: u64,
    pub used_physical: u64,
    pub total_virtual: u64,
    pub used_virtual: u64,
}

/// Application memory breakdown from jemalloc
#[derive(Debug, Clone)]
pub struct ApplicationMemoryInfo {
    pub heap_allocated: u64,     // Total bytes allocated by the application
    pub heap_active: u64,        // Total bytes in active pages allocated by the application
    pub heap_metadata: u64,      // Total bytes dedicated to jemalloc metadata
    pub heap_resident: u64,      // Total bytes of physical memory mapped
    pub heap_mapped: u64,        // Total bytes of virtual memory mapped
    pub arrow_tables_size: u64,  // Arrow table memory usage
    pub string_cache_size: u64,  // String interning cache size
    pub fragmentation_bytes: u64, // Memory fragmentation overhead
}

/// Complete memory usage report
#[derive(Debug, Clone)]
pub struct MemoryUsageReport {
    pub system: SystemMemoryInfo,
    pub application: ApplicationMemoryInfo,
    pub table_breakdown: HashMap<String, u64>,
    pub allocation_stats: AllocationStats,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Detailed allocation statistics from jemalloc
#[derive(Debug, Clone)]
pub struct AllocationStats {
    pub total_allocations: u64,
    pub total_deallocations: u64,
    pub current_allocations: u64,
    pub peak_allocated: u64,
    pub peak_resident: u64,
    pub allocation_rate: f64,     // allocations per second
    pub deallocation_rate: f64,   // deallocations per second
    pub fragmentation_ratio: f64, // (resident - allocated) / resident
}

/// Real-time memory profiler using jemalloc
pub struct MemoryProfiler {
    baseline_stats: Option<AllocationStats>,
    start_time: Instant,
}

impl MemoryProfiler {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            baseline_stats: None,
            start_time: Instant::now(),
        })
    }

    pub fn start_profiling(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.baseline_stats = Some(self.get_allocation_stats()?);
        self.start_time = Instant::now();
        Ok(())
    }

    pub fn get_allocation_stats(&self) -> Result<AllocationStats, Box<dyn std::error::Error>> {
        // Advance the epoch to get fresh statistics
        epoch::advance().map_err(|e| format!("Failed to advance jemalloc epoch: {}", e))?;

        let allocated: u64 = stats::allocated::read().map_err(|e| format!("Failed to read allocated: {}", e))? as u64;
        let _active: u64 = stats::active::read().map_err(|e| format!("Failed to read active: {}", e))? as u64;
        let _metadata: u64 = stats::metadata::read().map_err(|e| format!("Failed to read metadata: {}", e))? as u64;
        let resident: u64 = stats::resident::read().map_err(|e| format!("Failed to read resident: {}", e))? as u64;
        let _mapped: u64 = stats::mapped::read().map_err(|e| format!("Failed to read mapped: {}", e))? as u64;

        // Calculate rates if we have baseline
        let (alloc_rate, dealloc_rate) = if let Some(baseline) = &self.baseline_stats {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let alloc_rate = (allocated as f64 - baseline.peak_allocated as f64) / elapsed;
                let dealloc_rate = alloc_rate; // Approximation
                (alloc_rate, dealloc_rate)
            } else {
                (0.0, 0.0)
            }
        } else {
            (0.0, 0.0)
        };

        let fragmentation_ratio = if resident > 0 {
            (resident as f64 - allocated as f64) / resident as f64
        } else {
            0.0
        };

        Ok(AllocationStats {
            total_allocations: allocated / 1024, // Approximate
            total_deallocations: 0,              // Not easily available
            current_allocations: allocated,
            peak_allocated: allocated,
            peak_resident: resident,
            allocation_rate: alloc_rate,
            deallocation_rate: dealloc_rate,
            fragmentation_ratio,
        })
    }

    pub fn generate_report(&self) -> Result<MemoryUsageReport, Box<dyn std::error::Error>> {
        let allocation_stats = self.get_allocation_stats()?;
        
        Ok(MemoryUsageReport {
            system: get_system_memory(),
            application: get_application_memory()?,
            table_breakdown: HashMap::new(),
            allocation_stats,
            timestamp: chrono::Utc::now(),
        })
    }
}

impl MemoryUsageReport {
    pub fn current() -> Self {
        MemoryProfiler::new()
            .and_then(|profiler| profiler.generate_report())
            .unwrap_or_else(|_| fallback_memory_usage_report())
    }

    pub fn with_table_breakdown(mut self, tables: HashMap<String, u64>) -> Self {
        self.table_breakdown = tables;
        self
    }

    pub fn display(&self) {
        self.display_header();
        self.display_system_memory();
        self.display_application_memory();
        self.display_allocation_stats();
        self.display_table_breakdown();
        self.display_memory_analysis();
        self.display_summary();
    }

    fn display_header(&self) {
        println!("{}", "=".repeat(70));
        println!("{}", "🔍 Advanced Memory Profile Report (jemalloc)".bright_cyan().bold());
        println!("Generated at: {}", self.timestamp.format("%Y-%m-%d %H:%M:%S UTC"));
        println!("{}", "=".repeat(70));
    }

    fn display_system_memory(&self) {
        println!("\n{}", "System Memory:".bright_yellow().bold());
        println!(
            "  Total Physical:     {}",
            ByteSize::b(self.system.total_physical)
        );
        println!(
            "  Available Physical: {} ({:.1}%)",
            ByteSize::b(self.system.available_physical),
            (self.system.available_physical as f64 / self.system.total_physical as f64) * 100.0
        );
        println!(
            "  Used Physical:      {} ({:.1}%)",
            ByteSize::b(self.system.used_physical),
            (self.system.used_physical as f64 / self.system.total_physical as f64) * 100.0
        );
    }

    fn display_application_memory(&self) {
        println!("\n{}", "📊 Application Memory (jemalloc):".bright_yellow().bold());
        println!(
            "  Allocated (Active): {} ({:.1}%)",
            ByteSize::b(self.application.heap_allocated),
            if self.application.heap_resident > 0 {
                (self.application.heap_allocated as f64 / self.application.heap_resident as f64) * 100.0
            } else { 0.0 }
        );
        println!(
            "  Active Pages:       {}",
            ByteSize::b(self.application.heap_active)
        );
        println!(
            "  Resident (RSS):     {}",
            ByteSize::b(self.application.heap_resident)
        );
        println!(
            "  Mapped (Virtual):   {}",
            ByteSize::b(self.application.heap_mapped)
        );
        println!(
            "  Metadata Overhead:  {}",
            ByteSize::b(self.application.heap_metadata)
        );
        println!(
            "  Fragmentation:      {} ({:.1}%)",
            ByteSize::b(self.application.fragmentation_bytes),
            self.allocation_stats.fragmentation_ratio * 100.0
        );
        println!(
            "  Arrow Tables:       {}",
            ByteSize::b(self.application.arrow_tables_size)
        );
        println!(
            "  String Cache:       {}",
            ByteSize::b(self.application.string_cache_size)
        );
    }

    fn display_table_breakdown(&self) {
        if !self.table_breakdown.is_empty() {
            println!("\n{}", "Table Memory Breakdown:".bright_yellow().bold());
            let mut tables: Vec<_> = self.table_breakdown.iter().collect();
            tables.sort_by(|a, b| b.1.cmp(a.1)); // Sort by size descending

            for (table_name, size) in tables {
                println!("  {:<15} {}", table_name, ByteSize::b(*size));
            }
        }
    }

    fn display_summary(&self) {
        let total_app_memory = self.application.heap_allocated
            + self.application.arrow_tables_size
            + self.application.string_cache_size
            + self.application.heap_metadata;

        println!("\n{}", "Summary:".bright_cyan().bold());
        println!(
            "  Total Application Memory: {}",
            ByteSize::b(total_app_memory)
        );
        println!(
            "  System Memory Usage:      {:.1}%",
            (self.system.used_physical as f64 / self.system.total_physical as f64) * 100.0
        );
        println!(
            "  Application % of System:  {:.1}%",
            (total_app_memory as f64 / self.system.total_physical as f64) * 100.0
        );
        println!("{}", "=".repeat(70));
    }

    fn display_allocation_stats(&self) {
        println!("\n{}", "⚡ Allocation Statistics:".bright_blue().bold());
        println!(
            "  Current Allocations: {}",
            self.allocation_stats.current_allocations.to_string().bright_white()
        );
        println!(
            "  Peak Allocated:      {}",
            ByteSize::b(self.allocation_stats.peak_allocated)
        );
        println!(
            "  Peak Resident:       {}",
            ByteSize::b(self.allocation_stats.peak_resident)
        );
        
        if self.allocation_stats.allocation_rate > 0.0 {
            println!(
                "  Allocation Rate:     {:.1} MB/s",
                self.allocation_stats.allocation_rate / 1_000_000.0
            );
        }
        
        let fragmentation_status = if self.allocation_stats.fragmentation_ratio < 0.1 {
            "Excellent".bright_green()
        } else if self.allocation_stats.fragmentation_ratio < 0.2 {
            "Good".green()
        } else if self.allocation_stats.fragmentation_ratio < 0.3 {
            "Fair".yellow()
        } else {
            "Poor".red()
        };
        
        println!(
            "  Fragmentation:       {:.1}% ({})",
            self.allocation_stats.fragmentation_ratio * 100.0,
            fragmentation_status
        );
    }

    fn display_memory_analysis(&self) {
        println!("\n{}", "🔬 Memory Analysis:".bright_magenta().bold());
        
        // Memory efficiency analysis
        let total_app_memory = self.application.heap_allocated
            + self.application.arrow_tables_size
            + self.application.string_cache_size
            + self.application.heap_metadata;
        
        let efficiency = if self.application.heap_resident > 0 {
            (self.application.heap_allocated as f64 / self.application.heap_resident as f64) * 100.0
        } else {
            0.0
        };
        
        let efficiency_status = if efficiency > 90.0 {
            "Excellent".bright_green()
        } else if efficiency > 80.0 {
            "Good".green()
        } else if efficiency > 70.0 {
            "Fair".yellow()
        } else {
            "Poor - Consider optimization".red()
        };
        
        println!("  Memory Efficiency:   {:.1}% ({})", efficiency, efficiency_status);
        
        // Arrow table analysis
        if self.application.arrow_tables_size > 0 {
            let arrow_percentage = (self.application.arrow_tables_size as f64 / total_app_memory as f64) * 100.0;
            println!(
                "  Arrow Table Usage:   {:.1}% of total",
                arrow_percentage
            );
        }
        
        // Recommendations
        println!("\n{}", "💡 Recommendations:".bright_yellow().bold());
        
        if self.allocation_stats.fragmentation_ratio > 0.25 {
            println!("  • High fragmentation detected - consider memory pooling");
        }
        
        if efficiency < 80.0 {
            println!("  • Low memory efficiency - review allocation patterns");
        }
        
        if self.application.heap_metadata > self.application.heap_allocated / 10 {
            println!("  • High metadata overhead - consider larger allocation chunks");
        }
        
        let resident_to_mapped = if self.application.heap_mapped > 0 {
            self.application.heap_resident as f64 / self.application.heap_mapped as f64
        } else {
            1.0
        };
        
        if resident_to_mapped < 0.5 {
            println!("  • Low resident/mapped ratio - consider memory compaction");
        }
    }
}

/// Get system memory information (with cgroup/container awareness)
fn get_system_memory() -> SystemMemoryInfo {
    #[cfg(target_os = "macos")]
    {
        // Simplified implementation for macOS - use rough estimates
        // In a production system, you'd want to use system_profiler or sysctl
        SystemMemoryInfo {
            total_physical: 16 * 1024 * 1024 * 1024, // 16GB estimate
            available_physical: 4 * 1024 * 1024 * 1024, // 4GB estimate
            used_physical: 12 * 1024 * 1024 * 1024,
            total_virtual: 32 * 1024 * 1024 * 1024,
            used_virtual: 12 * 1024 * 1024 * 1024,
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Try to get cgroup memory limits first (container-aware)
        if let Some(cgroup_info) = get_cgroup_memory() {
            return cgroup_info;
        }

        // Fall back to /proc/meminfo for host memory
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            let mut total = 0u64;
            let mut available = 0u64;

            for line in contents.lines() {
                if let Some(value) = parse_meminfo_kb_line(line, "MemTotal") {
                    total = value;
                } else if let Some(value) = parse_meminfo_kb_line(line, "MemAvailable") {
                    available = value;
                }
            }

            SystemMemoryInfo {
                total_physical: total,
                available_physical: available,
                used_physical: total.saturating_sub(available),
                total_virtual: total * 2,
                used_virtual: total.saturating_sub(available),
            }
        } else {
            // Fallback
            SystemMemoryInfo {
                total_physical: 8 * 1024 * 1024 * 1024,
                available_physical: 2 * 1024 * 1024 * 1024,
                used_physical: 6 * 1024 * 1024 * 1024,
                total_virtual: 16 * 1024 * 1024 * 1024,
                used_virtual: 6 * 1024 * 1024 * 1024,
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        // Fallback for other platforms
        SystemMemoryInfo {
            total_physical: 8 * 1024 * 1024 * 1024,
            available_physical: 2 * 1024 * 1024 * 1024,
            used_physical: 6 * 1024 * 1024 * 1024,
            total_virtual: 16 * 1024 * 1024 * 1024,
            used_virtual: 6 * 1024 * 1024 * 1024,
        }
    }
}

fn fallback_memory_usage_report() -> MemoryUsageReport {
    MemoryUsageReport {
        system: get_system_memory(),
        application: get_application_memory().unwrap_or_else(|_| empty_application_memory()),
        table_breakdown: HashMap::new(),
        allocation_stats: empty_allocation_stats(),
        timestamp: chrono::Utc::now(),
    }
}

fn empty_application_memory() -> ApplicationMemoryInfo {
    ApplicationMemoryInfo {
        heap_allocated: 0,
        heap_active: 0,
        heap_metadata: 0,
        heap_resident: 0,
        heap_mapped: 0,
        arrow_tables_size: 0,
        string_cache_size: 0,
        fragmentation_bytes: 0,
    }
}

fn empty_allocation_stats() -> AllocationStats {
    AllocationStats {
        total_allocations: 0,
        total_deallocations: 0,
        current_allocations: 0,
        peak_allocated: 0,
        peak_resident: 0,
        allocation_rate: 0.0,
        deallocation_rate: 0.0,
        fragmentation_ratio: 0.0,
    }
}

#[cfg(target_os = "linux")]
fn parse_meminfo_kb_line(line: &str, key: &str) -> Option<u64> {
    let (line_key, rest) = line.split_once(':')?;
    if line_key != key {
        return None;
    }
    let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
/// Get memory information from cgroup limits (container-aware)
fn get_cgroup_memory() -> Option<SystemMemoryInfo> {
    // Try cgroup v2 first
    if let Some(info) = try_cgroup_v2_memory() {
        return Some(info);
    }
    
    // Fall back to cgroup v1
    if let Some(info) = try_cgroup_v1_memory() {
        return Some(info);
    }
    
    None
}

#[cfg(target_os = "linux")]
/// Try to read memory limits from cgroup v2
fn try_cgroup_v2_memory() -> Option<SystemMemoryInfo> {
    // cgroup v2 paths
    let max_path = "/sys/fs/cgroup/memory.max";
    let current_path = "/sys/fs/cgroup/memory.current";
    
    // Check if cgroup v2 is available
    if !std::path::Path::new(max_path).exists() {
        return None;
    }
    
    let max_str = std::fs::read_to_string(max_path).ok()?;
    let current_str = std::fs::read_to_string(current_path).ok()?;
    
    // Parse memory limit (could be "max" for unlimited)
    let total = if max_str.trim() == "max" {
        // If unlimited, fall back to host memory
        return None;
    } else {
        max_str.trim().parse::<u64>().ok()?
    };
    
    let used = current_str.trim().parse::<u64>().ok()?;
    let available = total.saturating_sub(used);
    
    Some(SystemMemoryInfo {
        total_physical: total,
        available_physical: available,
        used_physical: used,
        total_virtual: total,
        used_virtual: used,
    })
}

#[cfg(target_os = "linux")]
/// Try to read memory limits from cgroup v1
fn try_cgroup_v1_memory() -> Option<SystemMemoryInfo> {
    // cgroup v1 paths
    let limit_path = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    let usage_path = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    
    // Check if cgroup v1 is available
    if !std::path::Path::new(limit_path).exists() {
        return None;
    }
    
    let limit_str = std::fs::read_to_string(limit_path).ok()?;
    let usage_str = std::fs::read_to_string(usage_path).ok()?;
    
    let total = limit_str.trim().parse::<u64>().ok()?;
    let used = usage_str.trim().parse::<u64>().ok()?;
    
    // Check if limit is artificially high (essentially unlimited)
    // cgroup v1 often uses values like 9223372036854771712 (near u64::MAX) for unlimited
    const UNREALISTIC_LIMIT: u64 = 1_000_000_000_000_000; // 1 PB
    if total > UNREALISTIC_LIMIT {
        return None;
    }
    
    let available = total.saturating_sub(used);
    
    Some(SystemMemoryInfo {
        total_physical: total,
        available_physical: available,
        used_physical: used,
        total_virtual: total,
        used_virtual: used,
    })
}


/// Get application memory information using jemalloc
fn get_application_memory() -> Result<ApplicationMemoryInfo, Box<dyn std::error::Error>> {
    // Advance epoch to get fresh statistics
    epoch::advance().map_err(|e| format!("Failed to advance jemalloc epoch: {}", e))?;
    
    let allocated: u64 = stats::allocated::read().map_err(|e| format!("Failed to read allocated: {}", e))? as u64;
    let active: u64 = stats::active::read().map_err(|e| format!("Failed to read active: {}", e))? as u64;
    let metadata: u64 = stats::metadata::read().map_err(|e| format!("Failed to read metadata: {}", e))? as u64;
    let resident: u64 = stats::resident::read().map_err(|e| format!("Failed to read resident: {}", e))? as u64;
    let mapped: u64 = stats::mapped::read().map_err(|e| format!("Failed to read mapped: {}", e))? as u64;

    let fragmentation_bytes = if resident > allocated {
        resident - allocated
    } else {
        0
    };

    Ok(ApplicationMemoryInfo {
        heap_allocated: allocated,
        heap_active: active,
        heap_metadata: metadata,
        heap_resident: resident,
        heap_mapped: mapped,
        arrow_tables_size: 0, // Will be filled in by the caller
        string_cache_size: 0, // Will be filled in by the caller
        fragmentation_bytes,
    })
}



/// Get memory usage for Arrow tables
pub fn get_arrow_table_memory_usage(
    tables: &std::collections::HashMap<String, arrow::record_batch::RecordBatch>
) -> (u64, std::collections::HashMap<String, u64>) {
    let mut total_size = 0u64;
    let mut breakdown = std::collections::HashMap::new();

    for (name, batch) in tables {
        let mut table_size = 0u64;
        
        // Calculate size of each column
        for column in batch.columns() {
            table_size += get_array_memory_size(column);
        }
        
        breakdown.insert(name.clone(), table_size);
        total_size += table_size;
    }

    (total_size, breakdown)
}

/// Estimate memory size of an Arrow array
fn get_array_memory_size(array: &dyn arrow::array::Array) -> u64 {
    // This is a rough estimate of Arrow array memory usage
    // In practice, Arrow arrays have complex memory layouts
    let num_bytes = array.len() * size_of_arrow_type(array.data_type());
    
    // Add overhead for nulls, offsets, etc.
    let overhead = (num_bytes as f64 * 0.1) as u64; // 10% overhead estimate
    
    (num_bytes as u64) + overhead
}

/// Estimate size of Arrow data type in bytes
fn size_of_arrow_type(data_type: &arrow::datatypes::DataType) -> usize {
    use arrow::datatypes::DataType;
    
    match data_type {
        DataType::Boolean => 1,
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::UInt8 => 1,
        DataType::UInt16 => 2,
        DataType::UInt32 => 4,
        DataType::UInt64 => 8,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::Utf8 => 24, // Average string length estimate
        DataType::LargeUtf8 => 24,
        DataType::Binary => 16,
        DataType::LargeBinary => 16,
        DataType::Timestamp(_, _) => 8,
        DataType::Date32 => 4,
        DataType::Date64 => 8,
        DataType::Time32(_) => 4,
        DataType::Time64(_) => 8,
        DataType::Duration(_) => 8,
        DataType::Interval(_) => 16,
        DataType::List(_) => 24, // Estimate for list overhead
        DataType::LargeList(_) => 24,
        DataType::Struct(_) => 32, // Estimate for struct overhead
        _ => 16, // Default estimate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_parser_matches_key_and_converts_kb_to_bytes() {
        assert_eq!(
            parse_meminfo_kb_line("MemTotal:       16384 kB", "MemTotal"),
            Some(16_777_216)
        );
        assert_eq!(
            parse_meminfo_kb_line("MemAvailable:    2048 kB", "MemAvailable"),
            Some(2_097_152)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_parser_ignores_other_keys_and_malformed_values() {
        assert_eq!(parse_meminfo_kb_line("SwapTotal: 1024 kB", "MemTotal"), None);
        assert_eq!(parse_meminfo_kb_line("MemTotal: unknown kB", "MemTotal"), None);
        assert_eq!(parse_meminfo_kb_line("MemTotal", "MemTotal"), None);
    }

    #[test]
    fn fallback_report_uses_safe_empty_allocation_stats() {
        let report = fallback_memory_usage_report();

        assert_eq!(report.allocation_stats.current_allocations, 0);
        assert_eq!(report.table_breakdown.len(), 0);
        assert!(report.timestamp <= chrono::Utc::now());
    }
}
