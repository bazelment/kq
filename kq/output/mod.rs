use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema;
use colored::*;
use comfy_table::{Table, Cell, Color, Attribute, ContentArrangement, presets::UTF8_FULL};
use serde_json::{json, Value};
use std::io;

use kq_cli::OutputFormat;
use kq_schema::{SchemaInfo, TableInfo};

/// Result formatter for different output formats
pub struct ResultFormatter {
    pub format: OutputFormat,
    pub limit: Option<usize>,
}

impl ResultFormatter {
    pub fn new(format: OutputFormat, limit: Option<usize>) -> Self {
        Self { format, limit }
    }

    /// Print query results in the specified format
    pub fn print_result(&self, batch: &RecordBatch) -> Result<()> {
        match self.format {
            OutputFormat::Table => self.print_table(batch),
            OutputFormat::Json => self.print_json(batch),
            OutputFormat::Csv => self.print_csv(batch),
            OutputFormat::Tsv => self.print_tsv(batch),
            OutputFormat::Compact => self.print_compact(batch),
        }
    }

    /// Print schema information
    pub fn print_schema(&self, schema_info: &SchemaInfo) -> Result<()> {
        match self.format {
            OutputFormat::Json => self.print_schema_json(schema_info),
            _ => self.print_schema_table(schema_info),
        }
    }

    /// Print results as a formatted table
    fn print_table(&self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            println!("{}", "No results found".yellow());
            return Ok(());
        }
        let row_count = self.visible_row_count(batch);

        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // Add headers
        let headers: Vec<Cell> = batch.schema().fields().iter()
            .map(|field| {
                Cell::new(&field.name())
                    .add_attribute(Attribute::Bold)
                    .fg(Color::Blue)
            })
            .collect();
        table.set_header(headers);

        // Add rows
        for row_idx in 0..row_count {
            let mut row_cells = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let cell_value = self.format_array_value(array, row_idx);
                row_cells.push(Cell::new(cell_value));
            }
            
            table.add_row(row_cells);
        }

        println!("{}", table);
        self.print_row_count(row_count, batch.num_rows());

        Ok(())
    }

    /// Print results as JSON
    fn print_json(&self, batch: &RecordBatch) -> Result<()> {
        let output = self.batch_to_json(batch);
        println!("{}", serde_json::to_string_pretty(&output)?);
        Ok(())
    }

    /// Format results as compact JSON (single line, for NDJSON output)
    pub fn format_to_json_compact(&self, batch: &RecordBatch) -> Result<String> {
        let output = self.batch_to_json(batch);
        Ok(serde_json::to_string(&output)?)
    }

    /// Print results as CSV
    fn print_csv(&self, batch: &RecordBatch) -> Result<()> {
        let stdout = io::stdout();
        let mut writer = csv::Writer::from_writer(stdout.lock());

        // Write headers
        let schema = batch.schema();
        let headers: Vec<&str> = schema.fields().iter()
            .map(|field| field.name().as_str())
            .collect();
        writer.write_record(&headers)?;

        // Write rows
        for row_idx in 0..self.visible_row_count(batch) {
            let mut record = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = self.format_array_value(array, row_idx);
                record.push(value);
            }
            
            writer.write_record(&record)?;
        }

        writer.flush()?;
        Ok(())
    }

    /// Print results as TSV
    fn print_tsv(&self, batch: &RecordBatch) -> Result<()> {
        // Print headers
        let schema = batch.schema();
        let headers: Vec<&str> = schema.fields().iter()
            .map(|field| field.name().as_str())
            .collect();
        println!("{}", headers.join("\t"));

        // Print rows
        for row_idx in 0..self.visible_row_count(batch) {
            let mut values = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = self.format_array_value(array, row_idx);
                values.push(value);
            }
            
            println!("{}", values.join("\t"));
        }

        Ok(())
    }

    /// Print results in compact table format
    fn print_compact(&self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            println!("{}", "No results".yellow());
            return Ok(());
        }
        let row_count = self.visible_row_count(batch);

        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // Add headers (no styling for compact)
        let schema = batch.schema();
        let headers: Vec<&str> = schema.fields().iter()
            .map(|field| field.name().as_str())
            .collect();
        table.set_header(headers);

        // Add rows
        for row_idx in 0..row_count {
            let mut row_values = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = self.format_array_value(array, row_idx);
                row_values.push(value);
            }
            
            table.add_row(row_values);
        }

        println!("{}", table);
        self.print_row_count(row_count, batch.num_rows());

        Ok(())
    }

    /// Print schema information as a table
    fn print_schema_table(&self, schema_info: &SchemaInfo) -> Result<()> {
        if schema_info.tables.is_empty() {
            println!("{}", "No tables found".yellow());
            return Ok(());
        }

        for table_info in &schema_info.tables {
            println!("\n{} {}", "Table:".bold().blue(), table_info.name.bold());
            println!("{} {}", "Rows:".bold(), table_info.row_count.to_string().green());
            println!("{} {}", "Description:".bold(), table_info.description);
            
            // Check if this schema has nested structures
            let has_nested = table_info.schema.fields().iter().any(|field| {
                matches!(field.data_type(), 
                    arrow_schema::DataType::Struct(_) | 
                    arrow_schema::DataType::List(_) | 
                    arrow_schema::DataType::Map(_, _)
                )
            });

            if has_nested {
                // Print nested schema as tree
                self.print_nested_schema_tree(table_info)?;
            } else {
                // Print flat schema as table
                self.print_flat_schema_table(table_info)?;
            }
        }

        Ok(())
    }

    /// Print flat schema as a traditional table
    fn print_flat_schema_table(&self, table_info: &TableInfo) -> Result<()> {
        let mut schema_table = Table::new();
        schema_table.load_preset(UTF8_FULL);
        schema_table.set_header(vec![
            Cell::new("Column").add_attribute(Attribute::Bold).fg(Color::Blue),
            Cell::new("Type").add_attribute(Attribute::Bold).fg(Color::Blue),
            Cell::new("Nullable").add_attribute(Attribute::Bold).fg(Color::Blue),
        ]);

        for field in table_info.schema.fields() {
            schema_table.add_row(vec![
                Cell::new(&field.name()),
                Cell::new(&format!("{:?}", field.data_type())),
                Cell::new(if field.is_nullable() { "Yes" } else { "No" }),
            ]);
        }

        println!("{}", schema_table);
        Ok(())
    }

    /// Print nested schema as a tree structure
    fn print_nested_schema_tree(&self, table_info: &TableInfo) -> Result<()> {
        println!("\n{}", "Schema Structure:".bold().cyan());
        
        for field in table_info.schema.fields() {
            self.print_field_tree(field, "", true, true)?;
        }
        
        Ok(())
    }

    /// Recursively print a field and its nested structure as a tree
    fn print_field_tree(
        &self, 
        field: &arrow_schema::Field, 
        prefix: &str, 
        is_last: bool, 
        is_root: bool
    ) -> Result<()> {
        let (connector, continuation) = if is_root {
            ("", "")
        } else if is_last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };

        let field_name = if is_root {
            field.name().bold().blue().to_string()
        } else {
            field.name().to_string()
        };

        let type_info = self.format_data_type(field.data_type());
        let nullable_info = if field.is_nullable() { " (nullable)" } else { "" };
        
        println!("{}{}{} {} {}{}", 
            prefix, 
            connector, 
            field_name, 
            "→".dimmed(), 
            type_info, 
            nullable_info.dimmed()
        );

        // Recursively print nested structures
        match field.data_type() {
            arrow_schema::DataType::Struct(fields) => {
                for (i, nested_field) in fields.iter().enumerate() {
                    let is_last_nested = i == fields.len() - 1;
                    self.print_field_tree(nested_field, &format!("{}{}", prefix, continuation), is_last_nested, false)?;
                }
            }
            arrow_schema::DataType::List(item_field) => {
                let list_prefix = format!("{}{}", prefix, continuation);
                println!("{}└── [item] {} {}", 
                    list_prefix, 
                    "→".dimmed(), 
                    self.format_data_type(item_field.data_type())
                );
                
                // If the list item is a struct, show its fields
                if let arrow_schema::DataType::Struct(fields) = item_field.data_type() {
                    for (i, nested_field) in fields.iter().enumerate() {
                        let is_last_nested = i == fields.len() - 1;
                        self.print_field_tree(nested_field, &format!("{}    ", list_prefix), is_last_nested, false)?;
                    }
                }
            }
            arrow_schema::DataType::Map(entries_field, _sorted) => {
                let map_prefix = format!("{}{}", prefix, continuation);
                
                // Map in Arrow is actually a List of structs with "key" and "value" fields
                if let arrow_schema::DataType::Struct(fields) = entries_field.data_type() {
                    println!("{}└── [entries] {} struct", 
                        map_prefix, 
                        "→".dimmed()
                    );
                    
                    for (i, nested_field) in fields.iter().enumerate() {
                        let is_last_nested = i == fields.len() - 1;
                        self.print_field_tree(nested_field, &format!("{}    ", map_prefix), is_last_nested, false)?;
                    }
                } else {
                    println!("{}└── [entries] {} {}", 
                        map_prefix, 
                        "→".dimmed(), 
                        self.format_data_type(entries_field.data_type())
                    );
                }
            }
            _ => {} // No nested structure to display
        }

        Ok(())
    }

    /// Format data type for display with colors
    fn format_data_type(&self, data_type: &arrow_schema::DataType) -> String {
        match data_type {
            arrow_schema::DataType::Utf8 => "string".green().to_string(),
            arrow_schema::DataType::Int32 => "int32".yellow().to_string(),
            arrow_schema::DataType::Int64 => "int64".yellow().to_string(),
            arrow_schema::DataType::Float32 => "float32".magenta().to_string(),
            arrow_schema::DataType::Float64 => "float64".magenta().to_string(),
            arrow_schema::DataType::Boolean => "boolean".cyan().to_string(),
            arrow_schema::DataType::Timestamp(unit, tz) => {
                let tz_str = tz.as_ref().map(|tz| format!(", tz={}", tz)).unwrap_or_default();
                format!("timestamp({:?}{})", unit, tz_str).blue().to_string()
            }
            arrow_schema::DataType::Date32 => "date32".blue().to_string(),
            arrow_schema::DataType::Date64 => "date64".blue().to_string(),
            arrow_schema::DataType::Struct(_) => "struct".bold().red().to_string(),
            arrow_schema::DataType::List(_) => "list".bold().red().to_string(),
            arrow_schema::DataType::Map(_, _) => "map".bold().red().to_string(),
            _ => format!("{:?}", data_type).white().to_string(),
        }
    }

    /// Print schema information as JSON
    fn print_schema_json(&self, schema_info: &SchemaInfo) -> Result<()> {
        let mut tables = Vec::new();

        for table_info in &schema_info.tables {
            let mut fields = Vec::new();
            
            for field in table_info.schema.fields() {
                fields.push(json!({
                    "name": field.name(),
                    "type": format!("{:?}", field.data_type()),
                    "nullable": field.is_nullable()
                }));
            }

            tables.push(json!({
                "name": table_info.name,
                "row_count": table_info.row_count,
                "description": table_info.description,
                "fields": fields
            }));
        }

        let output = json!({
            "tables": tables
        });

        println!("{}", serde_json::to_string_pretty(&output)?);
        Ok(())
    }

    /// Format array value as string
    fn format_array_value(&self, array: &dyn arrow_array::Array, row_idx: usize) -> String {
        use arrow_array::*;

        if array.is_null(row_idx) {
            return "NULL".to_string();
        }

        match array.data_type() {
            arrow_schema::DataType::Utf8 => {
                typed_array::<StringArray>(array)
                    .map(|string_array| string_array.value(row_idx).to_string())
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Int32 => {
                typed_array::<Int32Array>(array)
                    .map(|int_array| int_array.value(row_idx).to_string())
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Int64 => {
                typed_array::<Int64Array>(array)
                    .map(|int_array| int_array.value(row_idx).to_string())
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Boolean => {
                typed_array::<BooleanArray>(array)
                    .map(|bool_array| bool_array.value(row_idx).to_string())
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Float64 => {
                typed_array::<Float64Array>(array)
                    .map(|float_array| format!("{:.2}", float_array.value(row_idx)))
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Timestamp(_, _) => {
                typed_array::<TimestampMillisecondArray>(array)
                    .map(|timestamp_array| {
                        let timestamp = timestamp_array.value(row_idx);
                        chrono::DateTime::from_timestamp_millis(timestamp)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                            .unwrap_or_else(|| timestamp.to_string())
                    })
                    .unwrap_or_else(|| debug_array_value(array, row_idx))
            }
            arrow_schema::DataType::Struct(_) => {
                self.format_struct_value(array, row_idx)
            }
            arrow_schema::DataType::List(_) => {
                self.format_list_value(array, row_idx)
            }
            arrow_schema::DataType::Map(_, _) => {
                self.format_map_value(array, row_idx)
            }
            arrow_schema::DataType::Dictionary(_, value_type) => {
                match value_type.as_ref() {
                    arrow_schema::DataType::Utf8 => utf8_dictionary_value(array, row_idx)
                        .unwrap_or_else(|| debug_array_value(array, row_idx)),
                    _ => debug_array_value(array, row_idx),
                }
            }
            _ => debug_array_value(array, row_idx),
        }
    }

    fn visible_row_count(&self, batch: &RecordBatch) -> usize {
        self.limit
            .map(|limit| limit.min(batch.num_rows()))
            .unwrap_or_else(|| batch.num_rows())
    }

    fn print_row_count(&self, visible_rows: usize, total_rows: usize) {
        if visible_rows == total_rows {
            println!("{} {} rows", "→".green(), total_rows.to_string().bold());
        } else {
            println!(
                "{} {} of {} rows",
                "→".green(),
                visible_rows.to_string().bold(),
                total_rows
            );
        }
    }

    fn batch_to_json(&self, batch: &RecordBatch) -> Value {
        let row_count = self.visible_row_count(batch);
        let mut results = Vec::with_capacity(row_count);

        for row_idx in 0..row_count {
            let mut row_obj = serde_json::Map::new();

            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let array = batch.column(col_idx);
                let value = self.array_value_to_json(array, row_idx);
                row_obj.insert(field.name().clone(), value);
            }

            results.push(Value::Object(row_obj));
        }

        if row_count == batch.num_rows() {
            json!({
                "results": results,
                "count": row_count
            })
        } else {
            json!({
                "results": results,
                "count": row_count,
                "total_count": batch.num_rows()
            })
        }
    }

    /// Format struct value in a concise way
    fn format_struct_value(&self, array: &dyn arrow_array::Array, row_idx: usize) -> String {
        use arrow_array::*;
        
        let Some(struct_array) = typed_array::<StructArray>(array) else {
            return debug_array_value(array, row_idx);
        };
        let mut parts = Vec::new();
        
        for (field_idx, column) in struct_array.columns().iter().enumerate() {
            let field_name = struct_array.fields()[field_idx].name();
            
            if column.is_null(row_idx) {
                continue; // Skip null fields for brevity
            }
            
            let value = self.format_array_value(column.as_ref(), row_idx);
            parts.push(format!("{}: {}", field_name, value));
        }
        
        format!("{{{}}}", parts.join(", "))
    }

    /// Format list value
    fn format_list_value(&self, array: &dyn arrow_array::Array, row_idx: usize) -> String {
        use arrow_array::*;
        
        let Some(list_array) = typed_array::<ListArray>(array) else {
            return debug_array_value(array, row_idx);
        };
        let value_array = list_array.value(row_idx);
        let mut items = Vec::new();
        
        for i in 0..value_array.len() {
            items.push(self.format_array_value(value_array.as_ref(), i));
        }
        
        format!("[{}]", items.join(", "))
    }

    /// Format map value
    fn format_map_value(&self, array: &dyn arrow_array::Array, row_idx: usize) -> String {
        use arrow_array::*;
        
        let Some(map_array) = typed_array::<MapArray>(array) else {
            return debug_array_value(array, row_idx);
        };
        let entries = map_array.value(row_idx);
        
        // Map is stored as a struct array with "key" and "value" fields
        if let Some(struct_array) = entries.as_any().downcast_ref::<StructArray>() {
            let keys = struct_array.column(0);
            let values = struct_array.column(1);
            let mut pairs = Vec::new();
            
            for i in 0..keys.len() {
                let key = self.format_array_value(keys.as_ref(), i);
                let value = self.format_array_value(values.as_ref(), i);
                pairs.push(format!("{}: {}", key, value));
            }
            
            format!("{{{}}}", pairs.join(", "))
        } else {
            "{}".to_string()
        }
    }

    /// Convert array value to JSON
    fn array_value_to_json(&self, array: &dyn arrow_array::Array, row_idx: usize) -> Value {
        use arrow_array::*;

        if array.is_null(row_idx) {
            return Value::Null;
        }

        match array.data_type() {
            arrow_schema::DataType::Utf8 => {
                typed_array::<StringArray>(array)
                    .map(|string_array| Value::String(string_array.value(row_idx).to_string()))
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Int32 => {
                typed_array::<Int32Array>(array)
                    .map(|int_array| Value::Number(int_array.value(row_idx).into()))
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Int64 => {
                typed_array::<Int64Array>(array)
                    .map(|int_array| Value::Number(int_array.value(row_idx).into()))
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Boolean => {
                typed_array::<BooleanArray>(array)
                    .map(|bool_array| Value::Bool(bool_array.value(row_idx)))
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Float64 => {
                typed_array::<Float64Array>(array)
                    .map(|float_array| float_json_value(float_array.value(row_idx)))
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Timestamp(_, _) => {
                typed_array::<TimestampMillisecondArray>(array)
                    .map(|timestamp_array| {
                        let timestamp = timestamp_array.value(row_idx);
                        Value::String(
                            chrono::DateTime::from_timestamp_millis(timestamp)
                                .map(|dt| dt.to_rfc3339())
                                .unwrap_or_else(|| timestamp.to_string()),
                        )
                    })
                    .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx)))
            }
            arrow_schema::DataType::Dictionary(_, value_type) => {
                match value_type.as_ref() {
                    arrow_schema::DataType::Utf8 => utf8_dictionary_value(array, row_idx)
                        .map(Value::String)
                        .unwrap_or_else(|| Value::String(debug_array_value(array, row_idx))),
                    _ => Value::String(format!("{:?}", array.slice(row_idx, 1))),
                }
            }
            _ => Value::String(format!("{:?}", array.slice(row_idx, 1))),
        }
    }
}

fn typed_array<T: arrow_array::Array + 'static>(
    array: &dyn arrow_array::Array,
) -> Option<&T> {
    array.as_any().downcast_ref::<T>()
}

fn debug_array_value(array: &dyn arrow_array::Array, row_idx: usize) -> String {
    format!("{:?}", array.slice(row_idx, 1))
}

fn float_json_value(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn utf8_dictionary_value(array: &dyn arrow_array::Array, row_idx: usize) -> Option<String> {
    use arrow_array::types::{
        Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };
    use arrow_array::{DictionaryArray, StringArray};

    macro_rules! try_dictionary {
        ($key_type:ty) => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                let values = dict_array.values();
                let string_values = values.as_any().downcast_ref::<StringArray>()?;
                let key = usize::try_from(dict_array.keys().value(row_idx)).ok()?;
                return Some(string_values.value(key).to_string());
            }
        };
    }

    try_dictionary!(Int8Type);
    try_dictionary!(Int16Type);
    try_dictionary!(Int32Type);
    try_dictionary!(Int64Type);
    try_dictionary!(UInt8Type);
    try_dictionary!(UInt16Type);
    try_dictionary!(UInt32Type);
    try_dictionary!(UInt64Type);

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{Schema, Field, DataType};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(vec!["test1", "test2", "test3"]));
        let count_array = Arc::new(Int64Array::from(vec![10, 20, 30]));

        RecordBatch::try_new(schema, vec![name_array, count_array]).unwrap()
    }

    #[test]
    fn test_result_formatter_creation() {
        let formatter = ResultFormatter::new(OutputFormat::Table, Some(10));
        assert!(matches!(formatter.format, OutputFormat::Table));
        assert_eq!(formatter.limit, Some(10));
    }

    #[test]
    fn test_format_array_value() {
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        let batch = create_test_batch();
        
        let name_value = formatter.format_array_value(batch.column(0), 0);
        assert_eq!(name_value, "test1");
        
        let count_value = formatter.format_array_value(batch.column(1), 1);
        assert_eq!(count_value, "20");
    }

    #[test]
    fn test_array_value_to_json() {
        let formatter = ResultFormatter::new(OutputFormat::Json, None);
        let batch = create_test_batch();
        
        let name_json = formatter.array_value_to_json(batch.column(0), 0);
        assert_eq!(name_json, Value::String("test1".to_string()));
        
        let count_json = formatter.array_value_to_json(batch.column(1), 1);
        assert_eq!(count_json, Value::Number(20.into()));
    }

    #[test]
    fn test_batch_to_json_applies_limit() {
        let formatter = ResultFormatter::new(OutputFormat::Json, Some(2));
        let batch = create_test_batch();

        let output = formatter.batch_to_json(&batch);

        assert_eq!(output["count"], 2);
        assert_eq!(output["total_count"], 3);
        assert_eq!(output["results"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_int32_dictionary_array_output() {
        // Regression test for dictionary array panic issue
        // Arrow dictionary arrays can use Int32 keys; output must decode them safely.
        use arrow_array::{DictionaryArray, types::Int32Type};
        
        // Create a dictionary array with Int32 keys.
        let dict_array = DictionaryArray::<Int32Type>::from_iter(vec![
            Some("namespace-1"),
            Some("namespace-2"),
            Some("namespace-1"),
            Some("namespace-3"),
        ]);
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", 
                DataType::Dictionary(
                    Box::new(DataType::Int32), 
                    Box::new(DataType::Utf8)
                ), 
                false
            ),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(dict_array)]
        ).unwrap();
        
        // Test table format output
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        let value = formatter.format_array_value(batch.column(0), 0);
        assert_eq!(value, "namespace-1");
        
        let value = formatter.format_array_value(batch.column(0), 1);
        assert_eq!(value, "namespace-2");
        
        // Test JSON format output
        let formatter_json = ResultFormatter::new(OutputFormat::Json, None);
        let json_value = formatter_json.array_value_to_json(batch.column(0), 0);
        assert_eq!(json_value, Value::String("namespace-1".to_string()));
        
        let json_value = formatter_json.array_value_to_json(batch.column(0), 2);
        assert_eq!(json_value, Value::String("namespace-1".to_string()));
    }

    #[test]
    fn dictionary_output_decodes_non_i32_keys() {
        use arrow_array::{types::Int8Type, DictionaryArray};

        let dict_array = DictionaryArray::<Int8Type>::from_iter(vec![
            Some("namespace-1"),
            Some("namespace-2"),
            Some("namespace-1"),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "namespace",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict_array)]).unwrap();
        let formatter = ResultFormatter::new(OutputFormat::Json, None);

        assert_eq!(
            formatter.format_array_value(batch.column(0), 1),
            "namespace-2"
        );
        assert_eq!(
            formatter.array_value_to_json(batch.column(0), 2),
            Value::String("namespace-1".to_string())
        );
    }

    #[test]
    fn json_output_uses_null_for_non_finite_float_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
        ]));
        let values = Arc::new(Float64Array::from(vec![f64::NAN, f64::INFINITY, 12.5]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let formatter = ResultFormatter::new(OutputFormat::Json, None);

        let output: Value = serde_json::from_str(&formatter.format_to_json_compact(&batch).unwrap()).unwrap();

        assert_eq!(output["results"][0]["value"], Value::Null);
        assert_eq!(output["results"][1]["value"], Value::Null);
        assert_eq!(output["results"][2]["value"], json!(12.5));
    }

    #[test]
    fn test_print_json() {
        let formatter = ResultFormatter::new(OutputFormat::Json, None);
        let batch = create_test_batch();
        
        // This test just ensures the function doesn't panic
        // In a real test environment, we'd capture stdout
        let result = formatter.print_json(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_csv() {
        let formatter = ResultFormatter::new(OutputFormat::Csv, None);
        let batch = create_test_batch();
        
        // This test just ensures the function doesn't panic
        let result = formatter.print_csv(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_tsv() {
        let formatter = ResultFormatter::new(OutputFormat::Tsv, None);
        let batch = create_test_batch();
        
        let result = formatter.print_tsv(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_compact() {
        let formatter = ResultFormatter::new(OutputFormat::Compact, None);
        let batch = create_test_batch();
        
        let result = formatter.print_compact(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_table() {
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        let batch = create_test_batch();
        
        let result = formatter.print_table(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_result() {
        let formatter = ResultFormatter::new(OutputFormat::Table, Some(1));
        let batch = create_test_batch();
        
        let result = formatter.print_result(&batch);
        assert!(result.is_ok());
    }

    fn create_test_schema_info() -> SchemaInfo {
        use kq_schema::{SchemaInfo, TableInfo};
        
        SchemaInfo {
            tables: vec![
                TableInfo {
                    name: "test_table".to_string(),
                    schema: arrow_schema::Schema::empty().into(),
                    row_count: 100,
                    description: "Test table".to_string(),
                },
            ],
        }
    }
    
    #[test]
    fn test_print_schema() {
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        let schema_info = create_test_schema_info();
        
        let result = formatter.print_schema(&schema_info);
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_nested_schema() {
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        let schema_info = create_nested_test_schema_info();
        
        let result = formatter.print_schema(&schema_info);
        assert!(result.is_ok());
    }

    fn create_nested_test_schema_info() -> SchemaInfo {
        use kq_schema::{SchemaInfo, TableInfo};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        
        // Create a nested schema similar to the Kubernetes nested schemas
        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("metadata", DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("uid", DataType::Utf8, true),
                    Field::new("creationTimestamp", DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None), true),
                ].into()
            ), true),
            Field::new("spec", DataType::Struct(
                vec![
                    Field::new("podCIDR", DataType::Utf8, true),
                ].into()
            ), true),
            Field::new("status", DataType::Struct(
                vec![
                    Field::new("phase", DataType::Utf8, true),
                ].into()
            ), true),
            Field::new("pool", DataType::Utf8, true), // Virtual column
        ]));
        
        SchemaInfo {
            tables: vec![
                TableInfo {
                    name: "nodes".to_string(),
                    schema: nested_schema,
                    row_count: 50,
                    description: "Kubernetes nodes with nested structure".to_string(),
                },
            ],
        }
    }

    #[test]
    fn test_output_format_enum() {
        // Test that all output formats are properly defined
        assert_eq!(format!("{:?}", OutputFormat::Table), "Table");
        assert_eq!(format!("{:?}", OutputFormat::Json), "Json");
        assert_eq!(format!("{:?}", OutputFormat::Csv), "Csv");
        assert_eq!(format!("{:?}", OutputFormat::Tsv), "Tsv");
        assert_eq!(format!("{:?}", OutputFormat::Compact), "Compact");
    }

    #[test]
    fn test_empty_batch_handling() {
        let formatter = ResultFormatter::new(OutputFormat::Table, None);
        
        // Create an empty batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let empty_batch = RecordBatch::new_empty(schema);
        
        // Should handle empty batches gracefully
        let result = formatter.print_result(&empty_batch);
        assert!(result.is_ok());
    }
}
