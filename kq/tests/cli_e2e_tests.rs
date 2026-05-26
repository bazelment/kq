//! End-to-end tests for the `kq` CLI binary.
//!
//! These tests run the actual binary as a subprocess against a synthetic
//! snapshot generated in a TempDir, and assert on stdout / exit code. They
//! cover the contract that users see: argv parsing, format flags, --limit,
//! batch-mode stdin, and error paths.
//!
//! Per docs/test-rules.md §"Use Real Dependencies Where They Matter", these
//! tests intentionally avoid mocking the loader, DataFusion, or the binary
//! itself — the boundary being validated is the whole pipeline from argv
//! to stdout.

use std::path::PathBuf;
use std::process::{Command, Stdio};

use chrono::Utc;
use kq::synthetic::{generate_ndjson_snapshot, SyntheticSnapshotConfig};
use tempfile::TempDir;

/// Locate the `kq` binary in Bazel test runfiles.
///
/// The BUILD rule passes `KQ_BIN_RLOC=$(rootpath //kq:kq)`, which gives us
/// a runfiles-relative path. Bazel sets `TEST_SRCDIR` to the runfiles root and
/// `TEST_WORKSPACE` to the workspace name; the binary lives at
/// `$TEST_SRCDIR/$TEST_WORKSPACE/$KQ_BIN_RLOC`.
fn kq_binary() -> PathBuf {
    let rloc = std::env::var("KQ_BIN_RLOC")
        .expect("KQ_BIN_RLOC must be set by the rust_test BUILD rule");
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("TEST_SRCDIR must be set by Bazel for rust_test targets");
    let workspace =
        std::env::var("TEST_WORKSPACE").unwrap_or_else(|_| "_main".to_string());
    PathBuf::from(srcdir).join(workspace).join(rloc)
}

fn snapshot_for_test(dir: &TempDir, seed: u64) -> PathBuf {
    let config = SyntheticSnapshotConfig {
        output_dir: dir.path().join("snap"),
        cluster_name: "e2e".to_string(),
        node_count: 4,
        min_pods_per_node: 3,
        max_pods_per_node: 5,
        namespace_count: 10,
        seed,
        overwrite: true,
        timestamp: Utc::now(),
    };
    let summary = generate_ndjson_snapshot(&config).expect("generate synthetic snapshot");
    summary.output_dir
}

/// Run `kq <snapshot> --query <q>` with the given extra flags. Returns
/// (stdout, stderr, exit_code).
fn run_kq_query(
    snapshot: &std::path::Path,
    sql: &str,
    extra_flags: &[&str],
) -> (String, String, i32) {
    let mut cmd = Command::new(kq_binary());
    cmd.arg(snapshot).arg("--query").arg(sql);
    cmd.args(extra_flags);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = cmd.output().expect("spawn kq binary");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.code().unwrap_or(-1),
    )
}

/// Extract the JSON object emitted by kq's --format json output from stdout.
/// kq prints a pretty `{ "results": [...], "count": N }` block followed by a
/// "Query completed (..)" footer, so we slice from the first `{` and parse
/// only the balanced JSON object.
fn extract_json_object(stdout: &str) -> serde_json::Value {
    let start = stdout
        .find('{')
        .unwrap_or_else(|| panic!("no JSON object in stdout:\n{stdout}"));
    let bytes = stdout.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for (i, &c) in bytes.iter().enumerate().skip(start) {
        if in_string {
            if escape {
                escape = false;
            } else if c == b'\\' {
                escape = true;
            } else if c == b'"' {
                in_string = false;
            }
            continue;
        }
        match c {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    let object_text = &stdout[start..=i];
                    return serde_json::from_str(object_text).unwrap_or_else(|err| {
                        panic!("failed to parse JSON object ({err}):\n{object_text}")
                    });
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced JSON object in stdout:\n{stdout}");
}

#[test]
fn cli_outputs_json_format() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 11);

    let (stdout, stderr, code) = run_kq_query(
        &snapshot,
        "SELECT COUNT(*) AS pod_count FROM pods",
        &["--format", "json"],
    );

    assert_eq!(code, 0, "kq exited non-zero. stderr=\n{stderr}");

    let value = extract_json_object(&stdout);
    let results = value
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing/typed `results` array in {value}"));
    assert_eq!(results.len(), 1, "expected one row, got {results:?}");
    let pod_count = results[0]
        .get("pod_count")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| panic!("missing/typed pod_count field in {:?}", results[0]));
    assert!(pod_count > 0, "expected at least one pod, got {pod_count}");
}

#[test]
fn cli_outputs_csv_format() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 12);

    let (stdout, stderr, code) = run_kq_query(
        &snapshot,
        "SELECT COUNT(*) AS pod_count FROM pods",
        &["--format", "csv"],
    );

    assert_eq!(code, 0, "kq exited non-zero. stderr=\n{stderr}");
    // CSV output must include the header line and at least one data line.
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|line| !line.is_empty())
        .collect();
    let header_idx = lines
        .iter()
        .position(|line| line.contains("pod_count"))
        .unwrap_or_else(|| panic!("no pod_count header line in:\n{stdout}"));
    assert!(
        lines.len() > header_idx + 1,
        "CSV output missing data row after header:\n{stdout}"
    );
}

#[test]
fn cli_applies_limit_flag() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 13);

    let (stdout, stderr, code) = run_kq_query(
        &snapshot,
        "SELECT metadata.name FROM pods",
        &["--format", "json", "--limit", "3"],
    );

    assert_eq!(code, 0, "kq exited non-zero. stderr=\n{stderr}");

    let value = extract_json_object(&stdout);
    let rows = value
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing/typed `results` array in {value}"));
    assert_eq!(
        rows.len(),
        3,
        "--limit 3 should yield exactly 3 rows, got {} ({:?})",
        rows.len(),
        rows
    );
}

#[test]
fn cli_reports_error_on_missing_snapshot() {
    let mut cmd = Command::new(kq_binary());
    cmd.arg("/this/path/does/not/exist.json")
        .arg("--query")
        .arg("SELECT 1");
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = cmd.output().expect("spawn kq binary");
    assert_ne!(
        output.status.code(),
        Some(0),
        "kq should exit non-zero when snapshot is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.is_empty(),
        "kq should emit a diagnostic on missing snapshot, got empty stderr"
    );
}

#[test]
fn cli_reports_error_on_invalid_sql() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 14);

    let (_stdout, stderr, code) = run_kq_query(&snapshot, "NOT VALID SQL", &[]);
    assert_ne!(code, 0, "invalid SQL should cause non-zero exit");
    assert!(
        !stderr.is_empty(),
        "kq should surface a SQL error on stderr, got empty"
    );
}

// ---------- Batch mode ----------

/// Run kq in `--batch` mode, feed `stdin_input` to its stdin, and return
/// (stdout, stderr, exit_code).
fn run_kq_batch(snapshot: &std::path::Path, stdin_input: &str) -> (String, String, i32) {
    use std::io::Write;

    let mut child = Command::new(kq_binary())
        .arg(snapshot)
        .arg("--batch")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kq binary");

    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(stdin_input.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait_with_output");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.code().unwrap_or(-1),
    )
}

/// Find the first stdout line that parses as a JSON object containing the
/// given top-level key, and return it as a Value. Skips logging output and
/// the ready handshake.
fn find_json_line_with_key(stdout: &str, key: &str) -> serde_json::Value {
    for line in stdout.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('{') {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if value.get(key).is_some() {
                return value;
            }
        }
    }
    panic!("no JSON line with key '{key}' in stdout:\n{stdout}");
}

#[test]
fn batch_mode_emits_ready_handshake_before_executing_queries() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 21);

    // Send .quit immediately — the binary should still emit the ready line.
    let (stdout, stderr, code) = run_kq_batch(&snapshot, ".quit\n");
    assert_eq!(code, 0, "batch mode should exit cleanly. stderr=\n{stderr}");

    let ready = find_json_line_with_key(&stdout, "ready");
    assert_eq!(
        ready.get("ready").and_then(|v| v.as_bool()),
        Some(true),
        "ready handshake malformed: {ready}"
    );
}

#[test]
fn batch_mode_executes_query_and_emits_compact_ndjson_result() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 22);

    let input = "SELECT COUNT(*) AS pod_count FROM pods;\n.quit\n";
    let (stdout, stderr, code) = run_kq_batch(&snapshot, input);
    assert_eq!(code, 0, "batch mode should exit cleanly. stderr=\n{stderr}");

    let result = find_json_line_with_key(&stdout, "results");
    let rows = result
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing `results` array in {result}"));
    assert_eq!(rows.len(), 1, "expected single COUNT(*) row");
    let pod_count = rows[0]
        .get("pod_count")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| panic!("missing pod_count: {:?}", rows[0]));
    assert!(pod_count > 0, "expected at least one pod, got {pod_count}");
}

#[test]
fn batch_mode_runs_multiple_queries_in_one_session() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 23);

    // Two queries separated by their semicolons; .quit terminates the session.
    let input = "SELECT COUNT(*) AS pod_count FROM pods;\n\
                 SELECT COUNT(*) AS node_count FROM nodes;\n\
                 .quit\n";
    let (stdout, stderr, code) = run_kq_batch(&snapshot, input);
    assert_eq!(code, 0, "batch mode should exit cleanly. stderr=\n{stderr}");

    // Collect every result line and verify both pod_count and node_count appear.
    let mut seen_pod_count = false;
    let mut seen_node_count = false;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('{') {
            continue;
        }
        let Ok(value): Result<serde_json::Value, _> = serde_json::from_str(trimmed) else {
            continue;
        };
        if let Some(rows) = value.get("results").and_then(|v| v.as_array()) {
            if let Some(row) = rows.first() {
                if row.get("pod_count").is_some() {
                    seen_pod_count = true;
                }
                if row.get("node_count").is_some() {
                    seen_node_count = true;
                }
            }
        }
    }
    assert!(
        seen_pod_count,
        "expected pod_count result line in stdout:\n{stdout}"
    );
    assert!(
        seen_node_count,
        "expected node_count result line in stdout:\n{stdout}"
    );
}

#[test]
fn batch_mode_reports_query_error_as_json_line() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_for_test(&dir, 24);

    // Invalid SQL must not crash the session — it should produce a single
    // valid JSON line on stdout (so downstream NDJSON consumers can parse
    // it line-by-line) and the `.quit` must still terminate cleanly.
    let input = "NOT VALID SQL;\n.quit\n";
    let (stdout, stderr, code) = run_kq_batch(&snapshot, input);
    assert_eq!(
        code, 0,
        "batch mode should survive query errors. stderr=\n{stderr}"
    );

    let err = find_json_line_with_key(&stdout, "error");
    let msg = err
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("error field is not a string: {err}"));
    assert!(
        msg.contains("SQL"),
        "error message should describe the SQL failure, got: {msg:?}"
    );
}
