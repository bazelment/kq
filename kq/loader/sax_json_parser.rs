use anyhow::{Context, Result};
use std::io::BufRead;

use super::resource_table::resource_for_json_key;

/// A SAX-style JSON parser that locates array fields without materializing the entire JSON tree.
/// 
/// This parser:
/// 1. Reads JSON incrementally
/// 2. Navigates to specific fields (timestamp, nodes, pods, etc.)
/// 3. Provides readers that yield just the bytes for each array
/// 4. Never loads the entire JSON into memory
///
/// Memory usage: O(buffer_size) instead of O(file_size)
pub struct SaxJsonParser<R: BufRead> {
    reader: R,
    buffer: Vec<u8>,
}

impl<R: BufRead> SaxJsonParser<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(1024),
        }
    }

    pub fn parse_streaming<F>(mut self, mut object_handler: F) -> Result<String>
    where
        F: FnMut(&'static str, &[u8]) -> Result<()>,
    {
        self.skip_whitespace()?;
        self.expect_byte(b'{')?;

        let mut timestamp = None;

        loop {
            self.skip_whitespace()?;

            if self.peek_byte()? == b'}' {
                self.read_byte()?;
                break;
            }

            let field_name = self.read_string()?;
            self.skip_whitespace()?;
            self.expect_byte(b':')?;
            self.skip_whitespace()?;

            if field_name == "timestamp" {
                timestamp = Some(self.read_string()?);
            } else if let Some(resource) = resource_for_json_key(&field_name) {
                self.stream_array_or_null_as_ndjson(resource.table_name, &mut object_handler)?;
            } else {
                self.skip_value()?;
            }

            self.skip_whitespace()?;
            if self.peek_byte()? == b',' {
                self.read_byte()?;
            }
        }

        timestamp.ok_or_else(|| anyhow::anyhow!("Missing timestamp field"))
    }

    /// Read a string value (handling escape sequences)
    fn read_string(&mut self) -> Result<String> {
        self.expect_byte(b'"')?;
        self.buffer.clear();

        loop {
            let byte = self.read_byte()?;
            match byte {
                b'"' => break,
                b'\\' => {
                    // Handle escape sequence
                    let next = self.read_byte()?;
                    match next {
                        b'"' | b'\\' | b'/' => self.buffer.push(next),
                        b'n' => self.buffer.push(b'\n'),
                        b'r' => self.buffer.push(b'\r'),
                        b't' => self.buffer.push(b'\t'),
                        b'u' => {
                            // Unicode escape - just pass through for now
                            self.buffer.push(b'\\');
                            self.buffer.push(b'u');
                        }
                        _ => {
                            self.buffer.push(b'\\');
                            self.buffer.push(next);
                        }
                    }
                }
                _ => self.buffer.push(byte),
            }
        }

        String::from_utf8(self.buffer.clone())
            .context("Invalid UTF-8 in string")
    }

    fn stream_array_or_null_as_ndjson<F>(
        &mut self,
        table_name: &'static str,
        object_handler: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&'static str, &[u8]) -> Result<()>,
    {
        let first_byte = self.peek_byte()?;

        if first_byte == b'n' {
            self.expect_bytes(b"null")?;
            return Ok(());
        }

        if first_byte != b'[' {
            anyhow::bail!("Expected '[' or 'null', got {}", first_byte as char);
        }

        object_handler(table_name, &[])?;
        self.read_byte()?;

        let mut object_buffer = Vec::with_capacity(512);
        let mut in_object = false;
        let mut depth = 0;
        let mut in_string = false;

        loop {
            let byte = self.read_byte()?;

            if byte == b']' && depth == 0 && !in_string {
                break;
            }

            if !in_object && (byte == b',' || byte.is_ascii_whitespace()) {
                continue;
            }

            if byte == b'{' && depth == 0 && !in_string {
                in_object = true;
                object_buffer.clear();
                object_buffer.push(byte);
                depth = 1;
                continue;
            }

            if in_object {
                object_buffer.push(byte);

                if !matches!(byte, b'\\' | b'"' | b'{' | b'}') {
                    continue;
                }

                match byte {
                    b'"' => in_string = !in_string,
                    b'\\' if in_string => {
                        let next = self.read_byte()?;
                        object_buffer.push(next);
                    }
                    b'{' if !in_string => depth += 1,
                    b'}' if !in_string => {
                        depth -= 1;
                        if depth == 0 {
                            object_buffer.push(b'\n');
                            object_handler(table_name, &object_buffer)?;
                            object_buffer.clear();
                            in_object = false;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Skip a JSON value (object, array, string, number, bool, null)
    fn skip_value(&mut self) -> Result<()> {
        self.skip_whitespace()?;
        let byte = self.peek_byte()?;

        match byte {
            b'"' => {
                self.read_string()?;
            }
            b'[' => {
                self.skip_array()?;
            }
            b'{' => {
                self.skip_object()?;
            }
            b't' => self.expect_bytes(b"true")?,
            b'f' => self.expect_bytes(b"false")?,
            b'n' => self.expect_bytes(b"null")?,
            b'-' | b'0'..=b'9' => {
                self.skip_number()?;
            }
            _ => anyhow::bail!("Unexpected byte: {}", byte as char),
        }

        Ok(())
    }

    /// Skip an array without reading its contents
    fn skip_array(&mut self) -> Result<()> {
        self.expect_byte(b'[')?;
        let mut depth = 1;
        let mut in_string = false;

        while depth > 0 {
            let byte = self.read_byte()?;

            // Fast path
            if !matches!(byte, b'\\' | b'"' | b'[' | b']') {
                continue;
            }

            match byte {
                b'"' => in_string = !in_string,
                b'\\' if in_string => { self.read_byte()?; }
                b'[' if !in_string => depth += 1,
                b']' if !in_string => depth -= 1,
                _ => {}
            }
        }
        Ok(())
    }

    /// Skip a JSON object
    /// Optimized version with early continue for common cases
    fn skip_object(&mut self) -> Result<()> {
        self.expect_byte(b'{')?;
        let mut depth = 1;
        let mut in_string = false;

        while depth > 0 {
            let byte = self.read_byte()?;

            // Fast path: skip non-special characters
            if !matches!(byte, b'\\' | b'"' | b'{' | b'}') {
                continue;
            }

            match byte {
                b'"' => in_string = !in_string,
                b'\\' if in_string => {
                    self.read_byte()?; // Skip escaped character
                }
                b'{' if !in_string => depth += 1,
                b'}' if !in_string => depth -= 1,
                _ => {}
            }
        }

        Ok(())
    }

    /// Skip a JSON number
    fn skip_number(&mut self) -> Result<()> {
        // Read until we hit a non-number character
        loop {
            let byte = self.peek_byte()?;
            match byte {
                b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9' => {
                    self.read_byte()?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// Skip whitespace
    fn skip_whitespace(&mut self) -> Result<()> {
        loop {
            let byte = self.peek_byte()?;
            match byte {
                b' ' | b'\t' | b'\n' | b'\r' => {
                    self.read_byte()?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// Read a single byte
    fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)
            .context("Unexpected end of input")?;
        Ok(buf[0])
    }

    /// Peek at the next byte without consuming it
    fn peek_byte(&mut self) -> Result<u8> {
        let buf = self.reader.fill_buf()
            .context("Failed to peek byte")?;
        if buf.is_empty() {
            anyhow::bail!("Unexpected end of input");
        }
        Ok(buf[0])
    }

    /// Expect a specific byte
    fn expect_byte(&mut self, expected: u8) -> Result<()> {
        let byte = self.read_byte()?;
        if byte != expected {
            anyhow::bail!(
                "Expected '{}' but got '{}'",
                expected as char,
                byte as char
            );
        }
        Ok(())
    }

    /// Expect a specific byte sequence
    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()> {
        for &byte in expected {
            self.expect_byte(byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn collect_streamed_objects(json: &str) -> Result<(String, Vec<(&'static str, Vec<u8>)>)> {
        let cursor = Cursor::new(json.as_bytes());
        let buf_reader = std::io::BufReader::new(cursor);
        let parser = SaxJsonParser::new(buf_reader);
        let mut seen = Vec::new();
        let timestamp = parser.parse_streaming(|table_name, object_bytes| {
            seen.push((table_name, object_bytes.to_vec()));
            Ok(())
        })?;

        Ok((timestamp, seen))
    }

    fn payloads_for<'a>(
        seen: &'a [(&'static str, Vec<u8>)],
        table_name: &str,
    ) -> Vec<&'a [u8]> {
        seen.iter()
            .filter(|(name, bytes)| *name == table_name && !bytes.is_empty())
            .map(|(_, bytes)| bytes.as_slice())
            .collect()
    }

    fn marker_count(seen: &[(&'static str, Vec<u8>)], table_name: &str) -> usize {
        seen.iter()
            .filter(|(name, bytes)| *name == table_name && bytes.is_empty())
            .count()
    }

    #[test]
    fn test_sax_parser_basic() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [{"name": "node1"}],
            "pods": [{"name": "pod1"}, {"name": "pod2"}],
            "namespaces": [],
            "daemonSets": null
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");

        let node_payloads = payloads_for(&seen, "nodes");
        assert_eq!(node_payloads.len(), 1);
        let nodes_str = std::str::from_utf8(node_payloads[0])?;
        assert!(nodes_str.contains("node1"));
        assert_eq!(payloads_for(&seen, "pods").len(), 2);
        assert_eq!(marker_count(&seen, "namespaces"), 1);
        assert_eq!(marker_count(&seen, "daemon_sets"), 0);

        Ok(())
    }

    #[test]
    fn test_sax_parser_iterator() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [{"name": "node1"}],
            "pods": [{"name": "pod1"}],
            "namespaces": null,
            "daemonSets": []
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");

        let nodes = payloads_for(&seen, "nodes");
        let pods = payloads_for(&seen, "pods");
        assert_eq!(nodes.len(), 1);
        assert_eq!(pods.len(), 1);
        assert!(std::str::from_utf8(nodes[0])?.contains("node1"));
        assert!(std::str::from_utf8(pods[0])?.contains("pod1"));
        assert_eq!(marker_count(&seen, "daemon_sets"), 1);
        assert_eq!(marker_count(&seen, "namespaces"), 0);

        Ok(())
    }

    #[test]
    fn test_sax_parser_streaming_callback() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [{"name": "node1"}, {"name": "node2"}],
            "pods": [],
            "namespaces": null,
            "daemonSets": [{"name": "ds1"}]
        }"#;

        let cursor = Cursor::new(json.as_bytes());
        let buf_reader = std::io::BufReader::new(cursor);
        let parser = SaxJsonParser::new(buf_reader);
        let mut seen = Vec::new();

        let timestamp = parser.parse_streaming(|table_name, object_bytes| {
            seen.push((table_name, object_bytes.to_vec()));
            Ok(())
        })?;

        assert_eq!(timestamp, "2024-01-01T12:00:00Z");
        assert_eq!(seen.iter().filter(|(name, _)| *name == "nodes").count(), 3);
        assert_eq!(seen.iter().filter(|(name, _)| *name == "pods").count(), 1);
        assert_eq!(seen.iter().filter(|(name, _)| *name == "daemon_sets").count(), 2);
        assert!(seen.iter().any(|(_, bytes)| bytes.is_empty()));
        assert!(seen.iter().any(|(_, bytes)| {
            std::str::from_utf8(bytes)
                .map(|json| json.contains("node2"))
                .unwrap_or(false)
        }));

        Ok(())
    }

    #[test]
    fn test_sax_parser_escaped_strings() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [{"name": "node\"1"}],
            "pods": null,
            "namespaces": null,
            "daemonSets": null
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");
        let nodes = payloads_for(&seen, "nodes");
        assert_eq!(nodes.len(), 1);
        assert!(std::str::from_utf8(nodes[0])?.contains(r#"node\"1"#));

        Ok(())
    }

    #[test]
    fn test_sax_parser_nested_objects() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [
                {
                    "metadata": {
                        "name": "node1",
                        "labels": {"key": "value"}
                    },
                    "spec": {"taints": [{"key": "taint1"}]}
                }
            ],
            "pods": null,
            "namespaces": null,
            "daemonSets": null
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");
        
        let nodes = payloads_for(&seen, "nodes");
        let nodes_str = std::str::from_utf8(nodes[0])?;
        assert!(nodes_str.contains("node1"));
        assert!(nodes_str.contains("taint1"));

        Ok(())
    }

    #[test]
    fn test_sax_parser_missing_timestamp() {
        let json = r#"{
            "nodes": [],
            "pods": []
        }"#;

        let cursor = Cursor::new(json.as_bytes());
        let buf_reader = std::io::BufReader::new(cursor);
        let parser = SaxJsonParser::new(buf_reader);

        let result = parser.parse_streaming(|_, _| Ok(()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timestamp"));
    }

    #[test]
    fn test_sax_parser_empty_arrays() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [],
            "pods": [],
            "namespaces": [],
            "daemonSets": []
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");

        assert_eq!(marker_count(&seen, "nodes"), 1);
        assert_eq!(marker_count(&seen, "pods"), 1);
        assert_eq!(marker_count(&seen, "namespaces"), 1);
        assert_eq!(marker_count(&seen, "daemon_sets"), 1);
        assert!(payloads_for(&seen, "nodes").is_empty());
        assert!(payloads_for(&seen, "pods").is_empty());
        assert!(payloads_for(&seen, "namespaces").is_empty());
        assert!(payloads_for(&seen, "daemon_sets").is_empty());

        Ok(())
    }

    #[test]
    fn test_sax_parser_preserves_json_structure() -> Result<()> {
        let json = r#"{
            "timestamp": "2024-01-01T12:00:00Z",
            "nodes": [
                {
                    "metadata": {
                        "name": "node1",
                        "uid": "uid-1"
                    },
                    "ready": true
                }
            ],
            "pods": null,
            "namespaces": null,
            "daemonSets": null
        }"#;

        let (timestamp, seen) = collect_streamed_objects(json)?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");

        let nodes = payloads_for(&seen, "nodes");
        let nodes_ndjson = std::str::from_utf8(nodes[0])?;
        
        // Verify the NDJSON contains all fields
        println!("Extracted nodes NDJSON: {}", nodes_ndjson);
        assert!(nodes_ndjson.contains(r#""name""#));
        assert!(nodes_ndjson.contains(r#""uid""#));
        assert!(nodes_ndjson.contains(r#""ready""#));
        
        // NDJSON format is: object\n (one JSON object followed by newline)
        // Parse the entire string as a single JSON object
        let trimmed = nodes_ndjson.trim();
        let parsed: serde_json::Value = serde_json::from_str(trimmed)?;
        assert!(parsed.is_object(), "Expected a JSON object, got: {:?}", parsed);
        assert_eq!(parsed["metadata"]["name"], "node1");
        assert_eq!(parsed["metadata"]["uid"], "uid-1");
        assert_eq!(parsed["ready"], true);

        Ok(())
    }

    #[test]
    fn test_sax_parser_large_array() -> Result<()> {
        // Create a larger JSON with many objects
        let mut nodes = Vec::new();
        for i in 0..100 {
            nodes.push(format!(r#"{{"name":"node{}","uid":"uid-{}"}}"#, i, i));
        }
        let nodes_json = format!("[{}]", nodes.join(","));

        let json = format!(
            r#"{{
                "timestamp": "2024-01-01T12:00:00Z",
                "nodes": {},
                "pods": null,
                "namespaces": null,
                "daemonSets": null
            }}"#,
            nodes_json
        );

        let cursor = Cursor::new(json.as_bytes());
        let buf_reader = std::io::BufReader::new(cursor);
        let parser = SaxJsonParser::new(buf_reader);

        let mut seen = Vec::new();
        let timestamp = parser.parse_streaming(|table_name, object_bytes| {
            seen.push((table_name, object_bytes.to_vec()));
            Ok(())
        })?;
        assert_eq!(timestamp, "2024-01-01T12:00:00Z");

        let nodes = payloads_for(&seen, "nodes");
        assert_eq!(nodes.len(), 100);
        assert!(std::str::from_utf8(nodes[0])?.contains("node0"));
        assert!(std::str::from_utf8(nodes[99])?.contains("node99"));

        Ok(())
    }
}
