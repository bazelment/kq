use anyhow::Result;
use colored::*;
use rustyline::error::ReadlineError;
use rustyline::history::{History, SearchDirection};
use rustyline::{Editor, Config, CompletionType};
use rustyline::completion::{Completer, Pair};
use rustyline::hint::Hinter;
use rustyline::highlight::Highlighter;
use rustyline::validate::Validator;
use rustyline::Helper;
use tracing::{error, info};
use kq_memory::{MemoryUsageReport, get_arrow_table_memory_usage};
use signal_hook::consts::SIGINT;
use signal_hook_tokio::Signals;
use tokio_stream::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use kq_output::ResultFormatter;
use kq_query::QueryEngine;
use kq_cli::OutputFormat;

/// Custom helper for SQL completion
struct SqlCompleter {
    table_names: Vec<String>,
    column_names: Vec<String>,
}

impl SqlCompleter {
    fn new(engine: &QueryEngine) -> Self {
        let mut table_names = Vec::new();
        let mut column_names = Vec::new();
        
        if let Ok(schema_info) = engine.describe_schema(None) {
            for table in &schema_info.tables {
                table_names.push(table.name.clone());
                
                // Add column names
                for field in table.schema.fields() {
                    let col_name = field.name().clone();
                    if !column_names.contains(&col_name) {
                        column_names.push(col_name);
                    }
                }
            }
        }
        
        Self { table_names, column_names }
    }
    
    fn sql_keywords() -> Vec<&'static str> {
        vec![
            "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
            "ON", "GROUP", "BY", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "AS", "AND", "OR",
            "NOT", "IN", "BETWEEN", "LIKE", "IS", "NULL", "CASE", "WHEN", "THEN",
            "ELSE", "END", "HAVING", "UNION", "ALL", "EXCEPT", "INTERSECT",
            "EXPLAIN", "VERBOSE", "ANALYZE", "DESCRIBE", "SHOW", "SET",
            "CREATE", "VIEW", "DROP", "ALTER", "TABLE",
        ]
    }
}

impl Completer for SqlCompleter {
    type Candidate = Pair;
    
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let mut candidates = Vec::new();
        let prefix = &line[..pos];

        // Dot-command completion: if the user is typing a meta-command as the
        // first token on the line (e.g. ".he"), suggest from the dot-command
        // list and skip SQL completion entirely. Detect this before the
        // generic word-boundary scan, which would otherwise treat `.` as a
        // separator and strand the leading dot.
        if prefix.starts_with('.') && !prefix.chars().any(char::is_whitespace) {
            for cmd in &[
                ".help", ".quit", ".history", ".memory", ".clear", ".format",
                ".tables", ".columns",
            ] {
                if cmd.starts_with(prefix) {
                    candidates.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
            return Ok((0, candidates));
        }

        // Find the word to complete (SQL identifiers).
        let start = prefix
            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == '.')
            .map(|i| i + 1)
            .unwrap_or(0);

        let word = &line[start..pos];
        let word_lower = word.to_lowercase();

        if word.is_empty() {
            return Ok((pos, candidates));
        }

        // Check if we're completing after FROM or JOIN
        let line_before = line[..start].to_uppercase();
        let completing_table = line_before.contains("FROM") ||
                               line_before.contains("JOIN") ||
                               line_before.ends_with("UPDATE") ||
                               line_before.ends_with("INTO");

        // Suggest table names
        if completing_table {
            for table in &self.table_names {
                if table.to_lowercase().starts_with(&word_lower) {
                    candidates.push(Pair {
                        display: table.clone(),
                        replacement: table.clone(),
                    });
                }
            }
        }

        // Suggest column names
        for column in &self.column_names {
            if column.to_lowercase().starts_with(&word_lower) {
                candidates.push(Pair {
                    display: column.clone(),
                    replacement: column.clone(),
                });
            }
        }

        // Suggest SQL keywords
        for keyword in Self::sql_keywords() {
            if keyword.to_lowercase().starts_with(&word_lower) {
                candidates.push(Pair {
                    display: keyword.to_string(),
                    replacement: keyword.to_string(),
                });
            }
        }

        Ok((start, candidates))
    }
}

impl Hinter for SqlCompleter {
    type Hint = String;
    
    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for SqlCompleter {}

impl Validator for SqlCompleter {}

impl Helper for SqlCompleter {}

pub struct InteractiveMode {
    engine: QueryEngine,
    formatter: ResultFormatter,
    editor: Editor<SqlCompleter, rustyline::history::DefaultHistory>,
    cancellation_flag: Arc<AtomicBool>,
    show_profile: bool,
    batch_mode: bool,
}

impl InteractiveMode {
    pub fn new(engine: QueryEngine) -> Result<Self> {
        Self::new_with_format(engine, OutputFormat::Table, None)
    }

    pub fn new_with_format(engine: QueryEngine, format: OutputFormat, limit: Option<usize>) -> Result<Self> {
        Self::new_with_options(engine, format, limit, false, false)
    }

    pub fn new_with_options(engine: QueryEngine, format: OutputFormat, limit: Option<usize>, show_profile: bool, batch_mode: bool) -> Result<Self> {
        // Create editor with custom config for tab completion
        let config = Config::builder()
            .completion_type(CompletionType::List)
            .build();
        
        let completer = SqlCompleter::new(&engine);
        let mut editor = Editor::with_config(config)?;
        editor.set_helper(Some(completer));
        
        // Load history from file if it exists (skip in batch mode)
        if !batch_mode {
            let history_file = dirs::home_dir()
                .map(|mut path| {
                    path.push(".kq_history");
                    path
                });
                
            if let Some(ref history_path) = history_file {
                if history_path.exists() {
                    let _ = editor.load_history(history_path);
                }
            }
        }
        
        Ok(Self {
            engine,
            formatter: ResultFormatter::new(format, limit),
            editor,
            cancellation_flag: Arc::new(AtomicBool::new(false)),
            show_profile,
            batch_mode,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.run_with_cli_options(None).await
    }

    pub async fn run_with_cli_options(&mut self, query_flag: Option<&str>) -> Result<()> {
        // Handle batch mode: read queries line-by-line from stdin
        if self.batch_mode {
            return self.run_batch_mode().await;
        }

        // Determine initial query: from --query flag or stdin if piped
        let initial_query = if let Some(query) = query_flag {
            Some(query.to_string())
        } else if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            // Read query from stdin if piped
            use std::io::Read;
            let mut buffer = String::new();
            std::io::stdin().read_to_string(&mut buffer)?;
            let query = buffer.trim().to_string();
            if !query.is_empty() {
                Some(query)
            } else {
                None
            }
        } else {
            None
        };
        
        // If there's an initial query, execute it first and exit
        if let Some(query) = initial_query {
            if let Err(e) = self.handle_input(&query).await {
                error!("Error: {}", e);
                eprintln!("{}: {}", "Error".red().bold(), e);
                std::process::exit(1);
            }
            return Ok(());
        }

        // Show welcome message and table info for interactive mode
        println!("{}", "Welcome to kq!".bright_green().bold());
        println!("Type {} for help, {} to exit", ".help".cyan(), ".quit".cyan());
        println!("Use {} and {} arrows to navigate command history", "↑".bright_blue(), "↓".bright_blue());
        println!("Press {} during query execution to interrupt long-running queries", "Ctrl-C".bright_red());
        println!("Press {} for tab completion of SQL keywords, tables, and columns", "Tab".bright_yellow());
        println!("Queries can span multiple lines - end with {} to execute", ";".bright_yellow());
        println!();

        // Show basic snapshot info
        if let Ok(schema_info) = self.engine.describe_schema(None) {
            println!("Available tables:");
            for table in &schema_info.tables {
                println!("  • {} ({} rows)", table.name.bright_blue(), table.row_count);
            }
            println!();
            println!("💡 Quick tips:");
            println!("   • List tables: {}", "SELECT * FROM information_schema.tables;".bright_yellow());
            println!("   • Show columns: {}", "DESCRIBE <table_name>;".bright_yellow());
            println!("   • Query plan: {}", "EXPLAIN <query>;".bright_yellow());
            println!();
        }

        let mut multiline_buffer = String::new();
        
        loop {
            let prompt = if multiline_buffer.is_empty() {
                format!("{} ", "kq>".bright_green().bold())
            } else {
                format!("{} ", "...>".bright_yellow().bold())
            };
            
            match self.editor.readline(&prompt) {
                Ok(line) => {
                    let trimmed = line.trim();
                    
                    // Handle empty lines in multiline mode
                    if trimmed.is_empty() {
                        if multiline_buffer.is_empty() {
                            continue;
                        } else {
                            // In multiline mode, empty line continues the query
                            multiline_buffer.push('\n');
                            continue;
                        }
                    }

                    // Check if this is a dot command - execute immediately
                    if trimmed.starts_with('.') && multiline_buffer.is_empty() {
                        // Add to history
                        self.editor.add_history_entry(trimmed)?;
                        
                        if let Err(e) = self.handle_input(trimmed).await {
                            error!("Error: {}", e);
                            println!("{}: {}", "Error".red().bold(), e);
                        }
                        continue;
                    }

                    // Accumulate lines for SQL queries
                    if !multiline_buffer.is_empty() {
                        multiline_buffer.push('\n');
                    }
                    multiline_buffer.push_str(trimmed);
                    
                    // Check if query is complete (ends with semicolon)
                    if multiline_buffer.trim_end().ends_with(';') {
                        let complete_query = multiline_buffer.clone();
                        multiline_buffer.clear();
                        
                        // Add to history if it's not a duplicate
                        let should_add = if self.editor.history().is_empty() {
                            true
                        } else {
                            match self.editor.history().get(self.editor.history().len() - 1, SearchDirection::Reverse) {
                                Ok(Some(search_result)) => search_result.entry != complete_query,
                                _ => true,
                            }
                        };
                        
                        if should_add {
                            self.editor.add_history_entry(&complete_query)?;
                        }

                        if let Err(e) = self.handle_input(&complete_query).await {
                            error!("Error: {}", e);
                            println!("{}: {}", "Error".red().bold(), e);
                        }
                    }
                    // Otherwise continue accumulating lines
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    if !multiline_buffer.is_empty() {
                        println!("{}", "Multiline query cancelled".yellow());
                        multiline_buffer.clear();
                    }
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("^D");
                    break;
                }
                Err(err) => {
                    error!("Error reading line: {:?}", err);
                    break;
                }
            }
        }

        // Save history to file
        let history_file = dirs::home_dir()
            .map(|mut path| {
                path.push(".kq_history");
                path
            });
            
        if let Some(history_path) = history_file {
            let _ = self.editor.save_history(&history_path);
        }

        println!("Goodbye!");
        Ok(())
    }
    
    /// Batch mode: read queries line-by-line from stdin, execute them without prompts/colors
    /// Outputs results as NDJSON (newline-delimited JSON) for easy parsing
    async fn run_batch_mode(&mut self) -> Result<()> {
        use std::io::BufRead;
        
        // Signal ready with a JSON object
        println!(r#"{{"ready":true}}"#);
        
        let stdin = std::io::stdin();
        let reader = stdin.lock();
        let mut multiline_buffer = String::new();
        
        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            
            // Skip empty lines when not building a query
            if trimmed.is_empty() && multiline_buffer.is_empty() {
                continue;
            }
            
            // Handle .quit command
            if trimmed == ".quit" && multiline_buffer.is_empty() {
                break;
            }
            
            // Accumulate lines
            if !multiline_buffer.is_empty() {
                multiline_buffer.push('\n');
            }
            multiline_buffer.push_str(trimmed);
            
            // Check if query is complete (ends with semicolon)
            if multiline_buffer.trim_end().ends_with(';') {
                let query = multiline_buffer.clone();
                multiline_buffer.clear();

                // Execute query and output as compact JSON (single line)
                if let Err(e) = self.execute_query_batch(&query).await {
                    // In batch mode, print error as JSON to stdout (not stderr).
                    // Build via serde_json so newlines / backslashes / quotes
                    // in the underlying error message stay properly escaped
                    // and the line remains valid NDJSON.
                    let payload = serde_json::json!({ "error": e.to_string() });
                    println!("{}", payload);
                }
            }
        }
        
        Ok(())
    }
    
    /// Execute a query in batch mode and output as compact NDJSON
    async fn execute_query_batch(&mut self, query: &str) -> Result<()> {
        match self.engine.execute(query).await {
            Ok(query_result) => {
                // Convert result to compact JSON (single line)
                let json_output = self.formatter.format_to_json_compact(&query_result)?;
                println!("{}", json_output);
            }
            Err(e) => {
                return Err(e);
            }
        }
        Ok(())
    }

    async fn handle_input(&mut self, input: &str) -> Result<()> {
        match input {
            ".quit" | ".exit" | "\\q" => {
                std::process::exit(0);
            }
            ".help" | "\\?" => {
                self.show_help();
                return Ok(());
            }
            ".tables" => {
                // Show tables using information_schema
                self.execute_query_with_cancellation(
                    "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema != 'information_schema' ORDER BY table_name;"
                ).await?;
                return Ok(());
            }
            ".history" => {
                self.show_history();
                return Ok(());
            }
            ".memory" => {
                self.show_memory_usage()?;
                return Ok(());
            }
            ".clear" => {
                print!("\x1B[2J\x1B[1;1H");
                return Ok(());
            }
            _ if input.starts_with(".columns ") => {
                let Some(table) = input.strip_prefix(".columns ") else {
                    return Ok(());
                };
                let table = table.trim();
                // Show columns using information_schema
                let query = format!(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position;",
                    table
                );
                self.execute_query_with_cancellation(&query).await?;
                return Ok(());
            }
            _ if input.starts_with(".format ") => {
                let Some(format_str) = input.strip_prefix(".format ") else {
                    return Ok(());
                };
                let format_str = format_str.trim();
                match format_str {
                    "table" => self.formatter.format = OutputFormat::Table,
                    "json" => self.formatter.format = OutputFormat::Json,
                    "csv" => self.formatter.format = OutputFormat::Csv,
                    "compact" => self.formatter.format = OutputFormat::Compact,
                    _ => {
                        println!("Unknown format: {}. Available: table, json, csv, compact", format_str);
                    }
                }
                return Ok(());
            }
            _ => {
                // Execute as SQL query with cancellation support
                // This now supports all DataFusion SQL including:
                // - EXPLAIN, EXPLAIN VERBOSE, EXPLAIN ANALYZE
                // - DESCRIBE <table>
                // - SET config = value
                // - SHOW ALL, SHOW config
                // - CREATE VIEW, DROP VIEW
                // - information_schema queries
                info!("Executing query: {}", input);
                self.execute_query_with_cancellation(input).await?;
            }
        }

        Ok(())
    }

    async fn execute_query_with_cancellation(&mut self, query: &str) -> Result<()> {
        // Set up signal handling for this query execution
        self.setup_signal_handler().await?;
        
        let start = std::time::Instant::now();
        
        // Execute query and check for cancellation periodically
        match self.engine.execute(query).await {
            Ok(query_result) => {
                if self.cancellation_flag.load(Ordering::Relaxed) {
                    println!("\n{}", "Query interrupted by user (Ctrl-C)".yellow().bold());
                    println!("Note: Query may have completed before interruption was processed");
                } else {
                    let duration = start.elapsed();
                    
                    // Show profile info if requested
                    if self.show_profile {
                        println!("Query executed in: {:?}", duration);
                        if let Some(stats) = self.engine.get_execution_stats() {
                            println!("Execution stats: {:#?}", stats);
                        }
                        println!();
                    }
                    
                    // Print results directly
                    self.formatter.print_result(&query_result)?;
                    
                    // Check if cancelled during output formatting
                    if !self.cancellation_flag.load(Ordering::Relaxed) {
                        if !self.show_profile {
                            println!();
                            println!("{} ({} rows, {:?})", 
                                "Query completed".green(), 
                                query_result.num_rows(), 
                                duration
                            );
                        }
                    }
                }
            }
            Err(e) => {
                if self.cancellation_flag.load(Ordering::Relaxed) {
                    println!("\n{}", "Query interrupted by user (Ctrl-C)".yellow().bold());
                } else {
                    return Err(e);
                }
            }
        }
        
        Ok(())
    }
    
    async fn setup_signal_handler(&mut self) -> Result<()> {
        // Reset cancellation token
        self.cancellation_flag.store(false, Ordering::Relaxed);
        
        // Create a signal stream for SIGINT
        let mut signals = Signals::new(&[SIGINT])?;
        let cancel_flag = Arc::clone(&self.cancellation_flag);
        
        // Spawn a task to handle signals during query execution
        tokio::spawn(async move {
            while let Some(_signal) = signals.next().await {
                cancel_flag.store(true, Ordering::Relaxed);
                // Break after first signal to avoid multiple interruptions
                break;
            }
        });
        
        Ok(())
    }

    fn show_help(&self) {
        println!("{}", "kq Interactive Commands:".bright_yellow().bold());
        println!();
        println!("  {}              - Show this help", ".help".cyan());
        println!("  {}              - Exit the program", ".quit".cyan());
        println!("  {}            - List all tables", ".tables".cyan());
        println!("  {} <table>  - Show columns for a table", ".columns".cyan());
        println!("  {}            - Show query history", ".history".cyan());
        println!("  {}             - Show current memory usage breakdown", ".memory".cyan());
        println!("  {}             - Clear screen", ".clear".cyan());
        println!("  {} <format>   - Change output format (table, json, csv, compact)", ".format".cyan());
        println!();
        println!("{}", "SQL Features:".bright_yellow().bold());
        println!("  • Multiline queries: End with {} to execute", ";".bright_yellow());
        println!("  • Tab completion: Press {} to complete SQL keywords, tables, columns", "Tab".bright_yellow());
        println!("  • History: Use {} {} to navigate previous queries", "↑".bright_blue(), "↓".bright_blue());
        println!();
        println!("{}", "Query Planning:".bright_yellow().bold());
        println!("  EXPLAIN SELECT * FROM pods;");
        println!("  EXPLAIN VERBOSE SELECT * FROM pods;");
        println!("  EXPLAIN ANALYZE SELECT * FROM pods;");
        println!();
        println!("{}", "Schema Discovery:".bright_yellow().bold());
        println!("  SELECT * FROM information_schema.tables;");
        println!("  SELECT * FROM information_schema.columns WHERE table_name = 'pods';");
        println!("  DESCRIBE pods;");
        println!();
        println!("{}", "Configuration:".bright_yellow().bold());
        println!("  SHOW ALL;");
        println!("  SHOW datafusion.execution.target_partitions;");
        println!("  SET datafusion.execution.target_partitions = 8;");
        println!();
        println!("{}", "Views (DDL):".bright_yellow().bold());
        println!("  CREATE VIEW running_pods AS SELECT * FROM pods WHERE phase = 'Running';");
        println!("  DROP VIEW running_pods;");
        println!();
        println!("{}", "Query Examples:".bright_yellow().bold());
        println!("  SELECT name, namespace FROM pods WHERE phase = 'Running';");
        println!("  SELECT pool, COUNT(*) FROM nodes GROUP BY pool;");
        println!("  SELECT metadata.name, spec.containers FROM pods LIMIT 5;");
        println!();
        println!("{}", "Multiline Example:".bright_yellow().bold());
        println!("  {}  SELECT name, namespace,", "kq>".bright_green());
        println!("  {}         phase", "...>".bright_yellow());
        println!("  {}  FROM pods", "...>".bright_yellow());
        println!("  {}  WHERE phase = 'Running';", "...>".bright_yellow());
        println!();
        println!("{}", "Interruption:".bright_yellow().bold());
        println!("  Press {} during query execution to interrupt", "Ctrl-C".bright_red());
        println!("  Press {} in multiline mode to cancel the query", "Ctrl-C".bright_red());
        println!();
    }

    fn show_history(&self) {
        let history = self.editor.history();
        if history.is_empty() {
            println!("No query history");
            return;
        }

        println!("{}", "Query History:".bright_yellow().bold());
        for i in 0..history.len() {
            if let Ok(Some(search_result)) = history.get(i, SearchDirection::Forward) {
                println!("  {}: {}", (i + 1).to_string().bright_blue(), search_result.entry);
            }
        }
    }

    fn show_memory_usage(&self) -> Result<()> {
        // Get current memory usage with table breakdown
        let tables = self.engine.get_tables_for_memory_analysis()?;
        let (total_arrow_size, table_breakdown) =
            get_arrow_table_memory_usage(&tables);

        let mut memory_report = MemoryUsageReport::current()
            .with_table_breakdown(table_breakdown);
        memory_report.application.arrow_tables_size = total_arrow_size;

        memory_report.display();
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kq_loader::SnapshotLoader;
    use rustyline::history::MemHistory;
    use rustyline::Context;
    use std::io::Write;

    async fn engine_with_pods(pod_names: &[&str]) -> QueryEngine {
        use serde_json::json;

        let pods: Vec<serde_json::Value> = pod_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                json!({
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "uid": format!("uid-{i}")
                    },
                    "spec": {
                        "nodeName": "n",
                        "containers": [{
                            "name": "c",
                            "image": "i",
                            "resources": {
                                "requests": { "cpu": "10m", "memory": "32Mi" }
                            }
                        }]
                    },
                    "status": { "phase": "Running" }
                })
            })
            .collect();

        let snapshot_json = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "nodes": [{
                "metadata": { "name": "n", "uid": "n-uid" },
                "spec": { "podCIDR": "10.0.0.0/24" },
                "status": {
                    "capacity": { "cpu": "1", "memory": "1Gi", "pods": "10" },
                    "allocatable": { "cpu": "1", "memory": "1Gi", "pods": "10" },
                    "phase": "Ready"
                }
            }],
            "namespaces": [{
                "metadata": { "name": "default", "uid": "ns-uid" },
                "status": { "phase": "Active" }
            }],
            "daemonSets": [],
            "pods": pods
        });

        let mut temp_file = tempfile::Builder::new()
            .suffix(".json")
            .tempfile()
            .unwrap();
        temp_file
            .write_all(snapshot_json.to_string().as_bytes())
            .unwrap();

        let data = SnapshotLoader::new()
            .load_and_combine(&[temp_file.path()])
            .await
            .unwrap();
        QueryEngine::new(data).await.unwrap()
    }

    fn completion_replacements(completer: &SqlCompleter, line: &str) -> Vec<String> {
        let history = MemHistory::new();
        let ctx = Context::new(&history);
        let (_, pairs) = completer.complete(line, line.len(), &ctx).unwrap();
        pairs.into_iter().map(|p| p.replacement).collect()
    }

    #[tokio::test]
    async fn sql_completer_indexes_table_names_from_engine() {
        // SqlCompleter pulls the table list from QueryEngine::describe_schema,
        // so it must reflect the user-facing views (pods/nodes/namespaces/
        // daemon_sets). Catches both a SqlCompleter regression and a view
        // rename in the loader/query layer.
        let engine = engine_with_pods(&["p1"]).await;
        let completer = SqlCompleter::new(&engine);
        for table in ["pods", "nodes", "namespaces", "daemon_sets"] {
            assert!(
                completer.table_names.iter().any(|t| t == table),
                "SqlCompleter missing table '{table}', got {:?}",
                completer.table_names
            );
        }
    }

    #[tokio::test]
    async fn sql_completer_suggests_tables_after_from_keyword() {
        let engine = engine_with_pods(&["p1"]).await;
        let completer = SqlCompleter::new(&engine);
        let suggestions = completion_replacements(&completer, "SELECT * FROM po");
        assert!(
            suggestions.iter().any(|s| s == "pods"),
            "expected 'pods' after FROM, got {suggestions:?}"
        );
    }

    #[tokio::test]
    async fn sql_completer_suggests_columns_from_table_schema() {
        // After SELECT, columns from registered tables should appear in
        // completions. This verifies that SqlCompleter::new indexed columns
        // (not just tables) from describe_schema.
        let engine = engine_with_pods(&["p1"]).await;
        let completer = SqlCompleter::new(&engine);
        let suggestions = completion_replacements(&completer, "SELECT meta");
        assert!(
            suggestions.iter().any(|s| s == "metadata"),
            "expected 'metadata' column in completions, got {suggestions:?}"
        );
    }

    #[tokio::test]
    async fn sql_completer_suggests_dot_commands_for_leading_dot_prefix() {
        // When the line starts with `.` and has no whitespace yet, the
        // completer should suggest meta-commands (and only those — SQL
        // keywords would be noise).
        let engine = engine_with_pods(&["p1"]).await;
        let completer = SqlCompleter::new(&engine);
        let suggestions = completion_replacements(&completer, ".h");
        assert!(
            suggestions.iter().any(|s| s == ".help"),
            "expected '.help' suggestion for '.h', got {suggestions:?}"
        );
        assert!(
            suggestions.iter().any(|s| s == ".history"),
            "expected '.history' suggestion for '.h', got {suggestions:?}"
        );
        // SQL keyword completion must NOT leak through (e.g. HAVING).
        assert!(
            !suggestions.iter().any(|s| s == "HAVING"),
            "dot-command completion should suppress SQL keywords, got {suggestions:?}"
        );
    }

    #[test]
    fn sql_completer_keyword_list_pins_documented_clauses() {
        // The keyword list drives REPL completion. If a clause is silently
        // dropped, tab-completion stops suggesting it without any warning.
        let keywords = SqlCompleter::sql_keywords();
        for clause in [
            "SELECT", "FROM", "WHERE", "JOIN", "GROUP", "ORDER", "LIMIT",
            "EXPLAIN", "DESCRIBE",
        ] {
            assert!(
                keywords.contains(&clause),
                "SQL keyword '{clause}' missing from completer; got {keywords:?}"
            );
        }
    }

    #[tokio::test]
    async fn interactive_mode_constructs_with_engine_and_options() {
        // Smoke test for the public constructor: verify it succeeds with all
        // option combinations users can hit via CLI flags. A regression here
        // would break `kq <snapshot>` invocation before any query runs.
        let engine = engine_with_pods(&["p1"]).await;
        let interactive = InteractiveMode::new_with_options(
            engine,
            OutputFormat::Json,
            Some(5),
            false, // show_profile
            true,  // batch_mode (avoids touching ~/.kq_history)
        );
        assert!(interactive.is_ok(), "InteractiveMode constructor failed: {:?}", interactive.err());
    }
}

