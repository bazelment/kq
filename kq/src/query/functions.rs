use anyhow::Result;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, MapArray, StringArray, StructArray,
};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use regex::Regex;
use std::sync::Arc;

/// Register custom Kubernetes-aware functions with DataFusion
pub fn register_kubernetes_functions(ctx: &SessionContext) -> Result<()> {
    // Register regexp_extract function
    ctx.register_udf(create_regexp_extract_udf());
    
    // Register extract_pool function (specifically for scheduler.kq.dev/node-selector)
    ctx.register_udf(create_extract_pool_udf());
    
    // Register json_extract_str function
    ctx.register_udf(create_json_extract_str_udf());
    
    // Register resource parsing functions
    ctx.register_udf(create_parse_cpu_udf());
    ctx.register_udf(create_parse_memory_udf());
    
    // Register container aggregation functions
    ctx.register_udf(create_container_count_udf());
    ctx.register_udf(create_total_cpu_request_udf());
    ctx.register_udf(create_total_memory_request_udf());
    ctx.register_udf(create_has_sidecar_udf());
    ctx.register_udf(create_container_names_udf());
    
    Ok(())
}

/// Create regexp_extract UDF
/// Usage: regexp_extract(string, pattern, group_index)
/// Example: regexp_extract(col, 'pool=([^"]+)', 1)
fn create_regexp_extract_udf() -> ScalarUDF {
    let regexp_extract = |args: &[ColumnarValue]| -> Result<ColumnarValue, datafusion::error::DataFusionError> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "regexp_extract requires 3 arguments: (string, pattern, group)".to_string()
            ));
        }

        let strings = match &args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "First argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "First argument must be an array".to_string()
            )),
        };
        
        let pattern = match &args[1] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Second argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Second argument must be an array".to_string()
            )),
        };
        
        let group_idx = match &args[2] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Third argument must be an integer".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Third argument must be an array".to_string()
            )),
        };

        // Get pattern and group (assuming they're constant for the column)
        let pattern_str = pattern.value(0);
        let group = group_idx.value(0) as usize;
        
        let re = Regex::new(pattern_str)
            .map_err(|e| datafusion::error::DataFusionError::Execution(
                format!("Invalid regex pattern: {}", e)
            ))?;

        let result: StringArray = strings
            .iter()
            .map(|s| {
                s.and_then(|text| {
                    re.captures(text)
                        .and_then(|caps| caps.get(group))
                        .map(|m| m.as_str().to_string())
                })
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    };

    ScalarUDF::from(datafusion::logical_expr::create_udf(
        "regexp_extract",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(regexp_extract),
    ))
}

/// Create extract_pool UDF
/// Extracts pool value from scheduler.kq.dev/node-selector annotation
/// Usage: extract_pool(annotation_value)
fn create_extract_pool_udf() -> ScalarUDF {
    let extract_pool = |args: &[ColumnarValue]| -> Result<ColumnarValue, datafusion::error::DataFusionError> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "extract_pool requires 1 argument".to_string()
            ));
        }

        let strings = match &args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Argument must be an array".to_string()
            )),
        };

        let result: StringArray = strings
            .iter()
            .map(|s| {
                s.and_then(|text| {
                    // Pattern: "node.kq.dev/pool=VALUE"
                    if let Some(start_idx) = text.find("node.kq.dev/pool=") {
                        let start = start_idx + "node.kq.dev/pool=".len();
                        let remaining = &text[start..];
                        // Find the end (either quote or bracket)
                        let end = remaining.find('"')
                            .or_else(|| remaining.find(']'))
                            .unwrap_or(remaining.len());
                        Some(remaining[..end].to_string())
                    } else {
                        None
                    }
                })
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    };

    ScalarUDF::from(datafusion::logical_expr::create_udf(
        "extract_pool",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(extract_pool),
    ))
}

/// Create json_extract_str UDF
/// Simple JSON string extraction (handles basic cases)
/// Usage: json_extract_str(json_string, key)
fn create_json_extract_str_udf() -> ScalarUDF {
    let json_extract_str = |args: &[ColumnarValue]| -> Result<ColumnarValue, datafusion::error::DataFusionError> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "json_extract_str requires 2 arguments: (json_string, key)".to_string()
            ));
        }

        let json_strings = match &args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "First argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "First argument must be an array".to_string()
            )),
        };
        
        let key = match &args[1] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Second argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Second argument must be an array".to_string()
            )),
        };

        let key_str = key.value(0);

        let result: StringArray = json_strings
            .iter()
            .map(|s| {
                s.and_then(|text| {
                    // Try to parse as JSON and extract the key
                    serde_json::from_str::<serde_json::Value>(text)
                        .ok()
                        .and_then(|json| {
                            json.get(key_str)
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                })
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    };

    ScalarUDF::from(datafusion::logical_expr::create_udf(
        "json_extract_str",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(json_extract_str),
    ))
}

/// Create parse_cpu UDF
/// Parses Kubernetes CPU quantity strings to millicores (Int64)
/// Usage: parse_cpu(cpu_string)
/// Examples: "500m" → 500, "1" → 1000, "0.5" → 500
fn create_parse_cpu_udf() -> ScalarUDF {
    let parse_cpu = |args: &[ColumnarValue]| -> Result<ColumnarValue, datafusion::error::DataFusionError> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "parse_cpu requires 1 argument".to_string()
            ));
        }

        let strings = match &args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Argument must be an array".to_string()
            )),
        };

        let result: arrow_array::Int64Array = strings
            .iter()
            .map(|s| {
                s.and_then(|cpu_str| parse_cpu_to_millicores(cpu_str.trim()))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    };

    ScalarUDF::from(datafusion::logical_expr::create_udf(
        "parse_cpu",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(parse_cpu),
    ))
}

/// Create parse_memory UDF
/// Parses Kubernetes memory quantity strings to bytes (Int64)
/// Usage: parse_memory(memory_string)
/// Examples: "128Mi" → 134217728, "1Gi" → 1073741824, "128172060Ki" → 131248189440
fn create_parse_memory_udf() -> ScalarUDF {
    let parse_memory = |args: &[ColumnarValue]| -> Result<ColumnarValue, datafusion::error::DataFusionError> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "parse_memory requires 1 argument".to_string()
            ));
        }

        let strings = match &args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "Argument must be a string".to_string()
                ))?,
            ColumnarValue::Scalar(_) => return Err(datafusion::error::DataFusionError::Execution(
                "Argument must be an array".to_string()
            )),
        };

        let result: arrow_array::Int64Array = strings
            .iter()
            .map(|s| {
                s.and_then(|mem_str| parse_memory_to_bytes(mem_str.trim()))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    };

    ScalarUDF::from(datafusion::logical_expr::create_udf(
        "parse_memory",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(parse_memory),
    ))
}

/// Parse Kubernetes CPU string to millicores
/// Examples: "1" → 1000, "500m" → 500, "0.5" → 500, "500000000n" → 500
fn parse_cpu_to_millicores(cpu_str: &str) -> Option<i64> {
    if cpu_str.ends_with('m') {
        // Already in millicores, e.g., "500m"
        cpu_str.trim_end_matches('m').parse::<i64>().ok()
    } else if cpu_str.ends_with('n') {
        // Nanocores, e.g., "500000000n" = 500m
        cpu_str.trim_end_matches('n').parse::<i64>().ok().map(|n| n / 1_000_000)
    } else {
        // Cores, e.g., "1" or "0.5"
        cpu_str.parse::<f64>().ok().map(|cores| (cores * 1000.0) as i64)
    }
}

/// Parse Kubernetes memory string to bytes
/// Examples: "128Mi" → 134217728, "1Gi" → 1073741824, "128172060Ki" → 131248189440
fn parse_memory_to_bytes(memory_str: &str) -> Option<i64> {
    // Try to match suffixes (binary units - base 1024)
    if memory_str.ends_with("Ki") {
        memory_str.trim_end_matches("Ki").parse::<i64>().ok().map(|n| n * 1024)
    } else if memory_str.ends_with("Mi") {
        memory_str.trim_end_matches("Mi").parse::<i64>().ok().map(|n| n * 1024 * 1024)
    } else if memory_str.ends_with("Gi") {
        memory_str.trim_end_matches("Gi").parse::<i64>().ok().map(|n| n * 1024 * 1024 * 1024)
    } else if memory_str.ends_with("Ti") {
        memory_str.trim_end_matches("Ti").parse::<i64>().ok().map(|n| n * 1024 * 1024 * 1024 * 1024)
    } else if memory_str.ends_with("Pi") {
        memory_str.trim_end_matches("Pi").parse::<i64>().ok().map(|n| n * 1024 * 1024 * 1024 * 1024 * 1024)
    // Decimal units (base 1000)
    } else if memory_str.ends_with('k') {
        memory_str.trim_end_matches('k').parse::<i64>().ok().map(|n| n * 1000)
    } else if memory_str.ends_with('M') {
        memory_str.trim_end_matches('M').parse::<i64>().ok().map(|n| n * 1000 * 1000)
    } else if memory_str.ends_with('G') {
        memory_str.trim_end_matches('G').parse::<i64>().ok().map(|n| n * 1000 * 1000 * 1000)
    } else if memory_str.ends_with('T') {
        memory_str.trim_end_matches('T').parse::<i64>().ok().map(|n| n * 1000 * 1000 * 1000 * 1000)
    } else if memory_str.ends_with('P') {
        memory_str.trim_end_matches('P').parse::<i64>().ok().map(|n| n * 1000 * 1000 * 1000 * 1000 * 1000)
    } else {
        // Plain bytes
        memory_str.parse::<i64>().ok()
    }
}

// ============================================================================
// Container Aggregation Functions
// ============================================================================

/// Count the number of containers in a containers array
/// Usage: container_count(spec.containers)
fn create_container_count_udf() -> ScalarUDF {
    ScalarUDF::from(ContainerCountUdf::new())
}

#[derive(Debug)]
struct ContainerCountUdf {
    signature: Signature,
}

impl ContainerCountUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ContainerCountUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "container_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "container_count requires 1 argument: (containers_array)".to_string()
            ));
        }
        
        let result = match &args[0] {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be a list/array".to_string()
                    ))?;
                
                let mut counts = Vec::with_capacity(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        counts.push(None);
                    } else {
                        let container_array = list_array.value(i);
                        counts.push(Some(container_array.len() as i64));
                    }
                }
                
                let result_array: Int64Array = counts.into_iter().collect();
                ColumnarValue::Array(Arc::new(result_array))
            }
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array argument, got scalar".to_string()
                ));
            }
        };
        
        Ok(result)
    }
}

/// Check if a pod has sidecar containers (more than one container)
/// Usage: has_sidecar(spec.containers)
fn create_has_sidecar_udf() -> ScalarUDF {
    ScalarUDF::from(HasSidecarUdf::new())
}

#[derive(Debug)]
struct HasSidecarUdf {
    signature: Signature,
}

impl HasSidecarUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HasSidecarUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "has_sidecar"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "has_sidecar requires 1 argument: (containers_array)".to_string()
            ));
        }
        
        let result = match &args[0] {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be a list/array".to_string()
                    ))?;
                
                let mut results = Vec::with_capacity(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        results.push(None);
                    } else {
                        let container_array = list_array.value(i);
                        results.push(Some(container_array.len() > 1));
                    }
                }
                
                let result_array: BooleanArray = results.into_iter().collect();
                ColumnarValue::Array(Arc::new(result_array))
            }
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array argument, got scalar".to_string()
                ));
            }
        };
        
        Ok(result)
    }
}

/// Get comma-separated list of container names
/// Usage: container_names(spec.containers)
fn create_container_names_udf() -> ScalarUDF {
    ScalarUDF::from(ContainerNamesUdf::new())
}

#[derive(Debug)]
struct ContainerNamesUdf {
    signature: Signature,
}

impl ContainerNamesUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ContainerNamesUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "container_names"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "container_names requires 1 argument: (containers_array)".to_string()
            ));
        }
        
        let result = match &args[0] {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be a list/array".to_string()
                    ))?;
                
                let mut names = Vec::with_capacity(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        names.push(None);
                    } else {
                        let container_array = list_array.value(i);
                        let struct_array = container_array.as_any().downcast_ref::<StructArray>()
                            .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                "Container array must contain structs".to_string()
                            ))?;
                        
                        // Get the 'name' field
                        if let Some(name_column) = struct_array.column_by_name("name") {
                            let name_strings = name_column.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                    "Container name must be a string".to_string()
                                ))?;
                            
                            let container_names: Vec<String> = (0..name_strings.len())
                                .filter_map(|i| name_strings.is_valid(i).then(|| name_strings.value(i).to_string()))
                                .collect();
                            
                            names.push(Some(container_names.join(",")));
                        } else {
                            names.push(None);
                        }
                    }
                }
                
                let result_array: StringArray = names.into_iter().collect();
                ColumnarValue::Array(Arc::new(result_array))
            }
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array argument, got scalar".to_string()
                ));
            }
        };
        
        Ok(result)
    }
}

/// Sum CPU requests across all containers in a pod (returns millicores)
/// Usage: total_cpu_request(spec.containers)
fn create_total_cpu_request_udf() -> ScalarUDF {
    ScalarUDF::from(TotalResourceRequestUdf::new(
        "total_cpu_request",
        "cpu",
        parse_cpu_to_millicores,
    ))
}

/// Sum memory requests across all containers in a pod (returns bytes)
/// Usage: total_memory_request(spec.containers)
fn create_total_memory_request_udf() -> ScalarUDF {
    ScalarUDF::from(TotalResourceRequestUdf::new(
        "total_memory_request",
        "memory",
        parse_memory_to_bytes,
    ))
}

type ResourceQuantityParser = fn(&str) -> Option<i64>;

#[derive(Debug)]
struct TotalResourceRequestUdf {
    name: &'static str,
    resource_key: &'static str,
    parser: ResourceQuantityParser,
    signature: Signature,
}

impl TotalResourceRequestUdf {
    fn new(
        name: &'static str,
        resource_key: &'static str,
        parser: ResourceQuantityParser,
    ) -> Self {
        Self {
            name,
            resource_key,
            parser,
            signature: Signature::one_of(
                vec![TypeSignature::Any(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TotalResourceRequestUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "{} requires 1 argument: (containers_array)",
                self.name
            )));
        }
        
        let result = match &args[0] {
            ColumnarValue::Array(arr) => {
                let list_array = arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Execution(
                        "Argument must be a list/array".to_string()
                    ))?;

                ColumnarValue::Array(Arc::new(sum_container_resource_requests(
                    list_array,
                    self.resource_key,
                    self.parser,
                )?))
            }
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Execution(
                    "Expected array argument, got scalar".to_string()
                ));
            }
        };
        
        Ok(result)
    }
}

fn sum_container_resource_requests(
    list_array: &ListArray,
    resource_key: &str,
    parser: ResourceQuantityParser,
) -> DataFusionResult<Int64Array> {
    let mut totals = Vec::with_capacity(list_array.len());

    for row_idx in 0..list_array.len() {
        if list_array.is_null(row_idx) {
            totals.push(None);
            continue;
        }

        let container_array = list_array.value(row_idx);
        let struct_array = container_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Container array must contain structs".to_string())
            })?;

        let total = sum_resource_requests_in_containers(struct_array, resource_key, parser)?;
        totals.push((total > 0).then_some(total));
    }

    Ok(totals.into_iter().collect())
}

fn sum_resource_requests_in_containers(
    struct_array: &StructArray,
    resource_key: &str,
    parser: ResourceQuantityParser,
) -> DataFusionResult<i64> {
    let Some(resources_column) = struct_array.column_by_name("resources") else {
        return Ok(0);
    };
    let resources_struct = resources_column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("Resources must be a struct".to_string()))?;

    let Some(requests_column) = resources_struct.column_by_name("requests") else {
        return Ok(0);
    };
    let requests_map = requests_column
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution("Requests must be a map".to_string()))?;

    let mut total = 0i64;
    for container_idx in 0..requests_map.len() {
        if requests_map.is_null(container_idx) {
            continue;
        }

        total +=
            resource_quantity_from_request_map(requests_map, container_idx, resource_key, parser)?;
    }

    Ok(total)
}

fn resource_quantity_from_request_map(
    requests_map: &MapArray,
    container_idx: usize,
    resource_key: &str,
    parser: ResourceQuantityParser,
) -> DataFusionResult<i64> {
    let entries = requests_map.value(container_idx);
    let entries_struct = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("Map entries must be a struct".to_string()))?;

    let (Some(keys), Some(values)) = (
        entries_struct.column_by_name("key"),
        entries_struct.column_by_name("value"),
    ) else {
        return Ok(0);
    };

    let key_array = keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Map keys must be strings".to_string()))?;
    let value_array = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Map values must be strings".to_string()))?;

    for entry_idx in 0..key_array.len() {
        if key_array.is_valid(entry_idx)
            && key_array.value(entry_idx) == resource_key
            && value_array.is_valid(entry_idx)
        {
            return Ok(parser(value_array.value(entry_idx)).unwrap_or(0));
        }
    }

    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_pool() {
        let json = r#"{"labels": ["node.kq.dev/pool=general"]}"#;
        assert!(json.contains("node.kq.dev/pool="));
    }

    #[test]
    fn test_parse_cpu_to_millicores() {
        // Test millicores
        assert_eq!(parse_cpu_to_millicores("500m"), Some(500));
        assert_eq!(parse_cpu_to_millicores("1000m"), Some(1000));
        assert_eq!(parse_cpu_to_millicores("100m"), Some(100));
        
        // Test cores
        assert_eq!(parse_cpu_to_millicores("1"), Some(1000));
        assert_eq!(parse_cpu_to_millicores("2"), Some(2000));
        assert_eq!(parse_cpu_to_millicores("0.5"), Some(500));
        assert_eq!(parse_cpu_to_millicores("0.25"), Some(250));
        
        // Test nanocores
        assert_eq!(parse_cpu_to_millicores("500000000n"), Some(500));
    }

    #[test]
    fn test_parse_memory_to_bytes() {
        // Test binary units (base 1024)
        assert_eq!(parse_memory_to_bytes("1Ki"), Some(1024));
        assert_eq!(parse_memory_to_bytes("1Mi"), Some(1024 * 1024));
        assert_eq!(parse_memory_to_bytes("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_to_bytes("128Mi"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_to_bytes("256Mi"), Some(256 * 1024 * 1024));
        assert_eq!(parse_memory_to_bytes("128172060Ki"), Some(128172060 * 1024));
        
        // Test decimal units (base 1000)
        assert_eq!(parse_memory_to_bytes("1k"), Some(1000));
        assert_eq!(parse_memory_to_bytes("1M"), Some(1_000_000));
        assert_eq!(parse_memory_to_bytes("1G"), Some(1_000_000_000));
        
        // Test plain bytes
        assert_eq!(parse_memory_to_bytes("1024"), Some(1024));
        assert_eq!(parse_memory_to_bytes("2048"), Some(2048));
    }
}
