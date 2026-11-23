use std::{collections::HashSet, sync::OnceLock};

use serde_json::{Map, Value};

#[cfg(test)]
use crate::ClientError;

const SERVER_REQUEST_SOURCE: &str = include_str!("../../schema/ServerRequest.json");
const SERVER_NOTIFICATION_SOURCE: &str = include_str!("../../schema/ServerNotification.json");
const PERMISSION_REQUEST_SOURCE: &str =
    include_str!("../../schema/PermissionsRequestApprovalParams.json");
const MAX_VALIDATION_STEPS: usize = 100_000;
const MAX_VALIDATION_DEPTH: usize = 128;
const MAX_DIAGNOSTIC_PATH_CHARS: usize = 128;
const VALIDATION_STEP_LIMIT_ERROR: &str = "JSON Schema validation step limit exceeded";
const VALIDATION_DEPTH_LIMIT_ERROR: &str = "JSON Schema validation depth limit exceeded";
const VALIDATION_SCHEMA_KEYWORDS: &[&str] = &[
    "$defs",
    "$ref",
    "additionalItems",
    "additionalProperties",
    "allOf",
    "anyOf",
    "const",
    "contains",
    "definitions",
    "dependencies",
    "else",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "if",
    "items",
    "maximum",
    "maxItems",
    "maxLength",
    "maxProperties",
    "minimum",
    "minItems",
    "minLength",
    "minProperties",
    "multipleOf",
    "not",
    "oneOf",
    "properties",
    "propertyNames",
    "required",
    "then",
    "type",
    "uniqueItems",
];
const ANNOTATION_SCHEMA_KEYWORDS: &[&str] = &[
    "$comment",
    "$id",
    "$schema",
    "contentEncoding",
    "contentMediaType",
    "default",
    "deprecated",
    "description",
    "examples",
    "format",
    "readOnly",
    "title",
    "writeOnly",
];

static PERMISSION_REQUEST_SCHEMA: OnceLock<Value> = OnceLock::new();
static SERVER_REQUEST_METHODS: OnceLock<HashSet<String>> = OnceLock::new();
static SERVER_NOTIFICATION_METHODS: OnceLock<HashSet<String>> = OnceLock::new();

struct ValidationContext {
    remaining_steps: usize,
    depth: usize,
}

impl ValidationContext {
    fn new() -> Self {
        Self {
            remaining_steps: MAX_VALIDATION_STEPS,
            depth: 0,
        }
    }

    fn consume_step(&mut self) -> Result<(), String> {
        if self.remaining_steps == 0 {
            return Err(VALIDATION_STEP_LIMIT_ERROR.to_owned());
        }
        self.remaining_steps -= 1;
        Ok(())
    }

    fn enter(&mut self) -> Result<(), String> {
        self.consume_step()?;
        let next = self.depth.saturating_add(1);
        if next > MAX_VALIDATION_DEPTH {
            return Err(VALIDATION_DEPTH_LIMIT_ERROR.to_owned());
        }
        self.depth = next;
        Ok(())
    }

    fn exit(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }
}

pub(crate) fn is_known_server_request_method(method: &str) -> bool {
    methods(&SERVER_REQUEST_METHODS, SERVER_REQUEST_SOURCE).contains(method)
}

pub(crate) fn is_known_server_notification_method(method: &str) -> bool {
    methods(&SERVER_NOTIFICATION_METHODS, SERVER_NOTIFICATION_SOURCE).contains(method)
}

/// Approval Response に再利用する permissions と、その参照先だけを検証する。
pub(crate) fn validate_permission_profile(value: &Value) -> Result<(), String> {
    let root = PERMISSION_REQUEST_SCHEMA.get_or_init(|| {
        let root = parse_schema(PERMISSION_REQUEST_SOURCE);
        audit_schema_keywords(&root["properties"]["permissions"], &root)
            .expect("checked-in permission profile JSON Schema must use supported keywords");
        root
    });
    validate(
        &root["properties"]["permissions"],
        value,
        root,
        "$",
        &mut ValidationContext::new(),
    )
}

fn parse_schema(source: &str) -> Value {
    serde_json::from_str(source).expect("checked-in app-server JSON Schema must be valid JSON")
}

#[cfg(test)]
pub(super) fn validate_test_document(
    source: &str,
    value: &Value,
    name: &str,
) -> Result<(), ClientError> {
    let schema = parse_schema(source);
    audit_schema_keywords(&schema, &schema)
        .expect("checked-in app-server JSON Schema uses an unsupported keyword");
    validate_document(&schema, value, name)
}

fn methods<'a>(lock: &'a OnceLock<HashSet<String>>, source: &str) -> &'a HashSet<String> {
    lock.get_or_init(|| {
        let mut methods = HashSet::new();
        collect_envelope_methods(&parse_schema(source), &mut methods);
        methods
    })
}

fn collect_envelope_methods(schema: &Value, methods: &mut HashSet<String>) {
    let Some(object) = schema.as_object() else {
        return;
    };
    if let Some(method_schema) = object
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("method"))
        .and_then(Value::as_object)
    {
        if let Some(value) = method_schema.get("const").and_then(Value::as_str) {
            methods.insert(value.to_owned());
        }
        if let Some(values) = method_schema.get("enum").and_then(Value::as_array) {
            methods.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
        }
    }
    for keyword in ["allOf", "anyOf", "oneOf"] {
        if let Some(children) = object.get(keyword).and_then(Value::as_array) {
            for child in children {
                collect_envelope_methods(child, methods);
            }
        }
    }
}

fn is_allowed_schema_keyword(keyword: &str) -> bool {
    VALIDATION_SCHEMA_KEYWORDS.contains(&keyword) || ANNOTATION_SCHEMA_KEYWORDS.contains(&keyword)
}

fn audit_schema_keywords(schema: &Value, root: &Value) -> Result<(), String> {
    audit_schema_node(schema, root, "$", &mut HashSet::new())
}

fn audit_schema_node<'a>(
    schema: &'a Value,
    root: &'a Value,
    path: &str,
    visited_references: &mut HashSet<&'a str>,
) -> Result<(), String> {
    match schema {
        Value::Bool(_) => return Ok(()),
        Value::Object(_) => {}
        _ => return Err(format!("{path} must be a schema object or boolean")),
    }
    let object = schema.as_object().expect("schema object checked above");
    for (keyword, value) in object {
        if !is_allowed_schema_keyword(keyword) {
            return Err(format!(
                "{path} contains unsupported JSON Schema keyword {}",
                escape_path(keyword)
            ));
        }
        match keyword.as_str() {
            "$ref" => {
                let reference = value
                    .as_str()
                    .ok_or_else(|| format!("{path} has a non-string JSON Schema reference"))?;
                if visited_references.insert(reference) {
                    audit_schema_node(
                        resolve_reference(root, reference)?,
                        root,
                        reference,
                        visited_references,
                    )?;
                }
            }
            "allOf" | "anyOf" | "oneOf" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        audit_schema_node(
                            child,
                            root,
                            &format!("{path}.{keyword}[{index}]"),
                            visited_references,
                        )?;
                    }
                }
            }
            "not"
            | "if"
            | "then"
            | "else"
            | "additionalProperties"
            | "additionalItems"
            | "propertyNames"
            | "contains" => {
                audit_schema_node(
                    value,
                    root,
                    &format!("{path}.{keyword}"),
                    visited_references,
                )?;
            }
            "items" => {
                if let Some(children) = value.as_array() {
                    for (index, child) in children.iter().enumerate() {
                        audit_schema_node(
                            child,
                            root,
                            &format!("{path}.items[{index}]"),
                            visited_references,
                        )?;
                    }
                } else {
                    audit_schema_node(value, root, &format!("{path}.items"), visited_references)?;
                }
            }
            "properties" => {
                if let Some(children) = value.as_object() {
                    for (name, child) in children {
                        audit_schema_node(
                            child,
                            root,
                            &format!("{path}.{keyword}.{}", escape_path(name)),
                            visited_references,
                        )?;
                    }
                }
            }
            "dependencies" => {
                if let Some(dependencies) = value.as_object() {
                    for (name, dependency) in dependencies {
                        if !dependency.is_array() {
                            audit_schema_node(
                                dependency,
                                root,
                                &format!("{path}.dependencies.{}", escape_path(name)),
                                visited_references,
                            )?;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
fn validate_document(schema: &Value, value: &Value, name: &str) -> Result<(), ClientError> {
    let mut context = ValidationContext::new();
    validate(schema, value, schema, "$", &mut context).map_err(|reason| {
        ClientError::ProtocolViolation {
            reason: format!("invalid {name}: {reason}"),
        }
    })
}

fn branch_matches(
    schema: &Value,
    value: &Value,
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<bool, String> {
    match validate(schema, value, root, path, context) {
        Ok(()) => Ok(true),
        Err(error) if is_budget_error(&error) => Err(error),
        Err(_) => Ok(false),
    }
}

fn is_budget_error(error: &str) -> bool {
    error == VALIDATION_STEP_LIMIT_ERROR || error == VALIDATION_DEPTH_LIMIT_ERROR
}

fn validate(
    schema: &Value,
    value: &Value,
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<(), String> {
    context.enter()?;
    let result = validate_node(schema, value, root, path, context);
    context.exit();
    result
}

fn validate_node(
    schema: &Value,
    value: &Value,
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<(), String> {
    match schema {
        Value::Bool(true) => return Ok(()),
        Value::Bool(false) => return Err(format!("{path} is rejected by a false schema")),
        Value::Object(_) => {}
        _ => return Err("checked-in JSON Schema node must be an object or boolean".to_owned()),
    }
    let object = schema.as_object().expect("schema object checked above");

    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let target = resolve_reference(root, reference)?;
        return validate(target, value, root, path, context);
    }

    validate_combinators(object, value, root, path, context)?;

    if let Some(expected) = object.get("type")
        && !matches_type(expected, value)
    {
        return Err(format!("{path} has an invalid type"));
    }
    if let Some(expected) = object.get("const")
        && expected != value
    {
        return Err(format!("{path} does not match const"));
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array)
        && !values.iter().any(|candidate| candidate == value)
    {
        return Err(format!("{path} is not an allowed enum value"));
    }

    if let Some(instance) = value.as_object() {
        validate_object(object, instance, root, path, context)?;
    }
    if let Some(instance) = value.as_array() {
        validate_array(object, instance, root, path, context)?;
    }
    if let Some(instance) = value.as_str() {
        validate_string(object, instance, path)?;
    }
    if value.is_number() {
        validate_number(object, value, path)?;
    }
    Ok(())
}

fn validate_combinators(
    schema: &Map<String, Value>,
    value: &Value,
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<(), String> {
    if let Some(schemas) = schema.get("allOf").and_then(Value::as_array) {
        for child in schemas {
            validate(child, value, root, path, context)?;
        }
    }
    if let Some(schemas) = schema.get("anyOf").and_then(Value::as_array) {
        let mut matches = false;
        for child in schemas {
            if branch_matches(child, value, root, path, context)? {
                matches = true;
                break;
            }
        }
        if !matches {
            return Err(format!("{path} does not match any anyOf branch"));
        }
    }
    if let Some(schemas) = schema.get("oneOf").and_then(Value::as_array) {
        let mut matches = 0;
        for child in schemas {
            if branch_matches(child, value, root, path, context)? {
                matches += 1;
            }
        }
        if matches != 1 {
            return Err(format!("{path} matches {matches} oneOf branches"));
        }
    }
    if let Some(child) = schema.get("not")
        && branch_matches(child, value, root, path, context)?
    {
        return Err(format!("{path} matches a forbidden schema"));
    }
    if let Some(condition) = schema.get("if") {
        let branch = if branch_matches(condition, value, root, path, context)? {
            schema.get("then")
        } else {
            schema.get("else")
        };
        if let Some(branch) = branch {
            validate(branch, value, root, path, context)?;
        }
    }
    Ok(())
}

fn validate_object(
    schema: &Map<String, Value>,
    value: &Map<String, Value>,
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<(), String> {
    if let Some(minimum) = schema.get("minProperties").and_then(Value::as_u64)
        && value.len() < minimum as usize
    {
        return Err(format!("{path} has too few properties"));
    }
    if let Some(maximum) = schema.get("maxProperties").and_then(Value::as_u64)
        && value.len() > maximum as usize
    {
        return Err(format!("{path} has too many properties"));
    }
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !value.contains_key(name) {
                return Err(format!("{path} is missing required property {name}"));
            }
        }
    }

    let properties = schema.get("properties").and_then(Value::as_object);
    for (name, child_value) in value {
        if let Some(child_schema) = properties.and_then(|properties| properties.get(name)) {
            validate(
                child_schema,
                child_value,
                root,
                &format!("{path}.{}", escape_path(name)),
                context,
            )?;
        }
    }

    if let Some(property_names) = schema.get("propertyNames") {
        for name in value.keys() {
            validate(
                property_names,
                &Value::String(name.clone()),
                root,
                &format!("{path}.<property-name>"),
                context,
            )?;
        }
    }

    if let Some(dependencies) = schema.get("dependencies").and_then(Value::as_object) {
        for (name, dependency) in dependencies {
            if !value.contains_key(name) {
                continue;
            }
            if let Some(required) = dependency.as_array() {
                for required_name in required.iter().filter_map(Value::as_str) {
                    if !value.contains_key(required_name) {
                        return Err(format!(
                            "{path} requires property {required_name} when {name} is present"
                        ));
                    }
                }
            } else {
                validate(
                    dependency,
                    &Value::Object(value.clone()),
                    root,
                    path,
                    context,
                )?;
            }
        }
    }

    if let Some(additional) = schema.get("additionalProperties") {
        for (name, child_value) in value {
            if properties.is_some_and(|properties| properties.contains_key(name)) {
                continue;
            }
            match additional {
                Value::Bool(true) => {}
                Value::Bool(false) => {
                    return Err(format!(
                        "{path} contains unknown property {}",
                        escape_path(name)
                    ));
                }
                _ => validate(
                    additional,
                    child_value,
                    root,
                    &format!("{path}.{}", escape_path(name)),
                    context,
                )?,
            }
        }
    }
    Ok(())
}

fn validate_array(
    schema: &Map<String, Value>,
    value: &[Value],
    root: &Value,
    path: &str,
    context: &mut ValidationContext,
) -> Result<(), String> {
    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64)
        && value.len() < minimum as usize
    {
        return Err(format!("{path} has too few items"));
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64)
        && value.len() > maximum as usize
    {
        return Err(format!("{path} has too many items"));
    }
    if schema.get("uniqueItems").and_then(Value::as_bool) == Some(true) {
        for index in 0..value.len() {
            for previous in &value[..index] {
                context.consume_step()?;
                if previous == &value[index] {
                    return Err(format!("{path} contains duplicate items"));
                }
            }
        }
    }
    if let Some(items) = schema.get("items") {
        if let Some(tuple) = items.as_array() {
            for (index, child_value) in value.iter().enumerate() {
                if let Some(child_schema) = tuple.get(index) {
                    validate(
                        child_schema,
                        child_value,
                        root,
                        &format!("{path}[{index}]"),
                        context,
                    )?;
                } else if let Some(additional) = schema.get("additionalItems") {
                    validate(
                        additional,
                        child_value,
                        root,
                        &format!("{path}[{index}]"),
                        context,
                    )?;
                }
            }
        } else {
            for (index, child_value) in value.iter().enumerate() {
                validate(
                    items,
                    child_value,
                    root,
                    &format!("{path}[{index}]"),
                    context,
                )?;
            }
        }
    }
    if let Some(contains) = schema.get("contains") {
        let mut matches = false;
        for (index, child) in value.iter().enumerate() {
            if branch_matches(contains, child, root, &format!("{path}[{index}]"), context)? {
                matches = true;
                break;
            }
        }
        if !matches {
            return Err(format!("{path} does not contain a matching item"));
        }
    }
    Ok(())
}

fn validate_string(schema: &Map<String, Value>, value: &str, path: &str) -> Result<(), String> {
    let length = value.chars().count();
    if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64)
        && length < minimum as usize
    {
        return Err(format!("{path} is too short"));
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64)
        && length > maximum as usize
    {
        return Err(format!("{path} is too long"));
    }
    if schema.contains_key("pattern") {
        return Err(
            "checked-in protocol schemas must not use unsupported pattern validation".to_owned(),
        );
    }
    Ok(())
}

fn validate_number(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    let value = value
        .as_f64()
        .ok_or_else(|| format!("{path} is not a finite JSON number"))?;
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
        && value < minimum
    {
        return Err(format!("{path} is below minimum"));
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
        && value > maximum
    {
        return Err(format!("{path} is above maximum"));
    }
    if let Some(minimum) = schema.get("exclusiveMinimum").and_then(Value::as_f64)
        && value <= minimum
    {
        return Err(format!("{path} is not above exclusiveMinimum"));
    }
    if let Some(maximum) = schema.get("exclusiveMaximum").and_then(Value::as_f64)
        && value >= maximum
    {
        return Err(format!("{path} is not below exclusiveMaximum"));
    }
    if let Some(divisor) = schema.get("multipleOf").and_then(Value::as_f64) {
        let quotient = value / divisor;
        if (quotient - quotient.round()).abs() > f64::EPSILON * quotient.abs().max(1.0) {
            return Err(format!("{path} is not a multipleOf value"));
        }
    }
    Ok(())
}

fn resolve_reference<'a>(root: &'a Value, reference: &str) -> Result<&'a Value, String> {
    if reference == "#" {
        return Ok(root);
    }
    let pointer = reference
        .strip_prefix('#')
        .ok_or_else(|| format!("external JSON Schema reference is unsupported: {reference}"))?;
    root.pointer(pointer)
        .ok_or_else(|| format!("unresolved JSON Schema reference: {reference}"))
}

fn matches_type(expected: &Value, value: &Value) -> bool {
    match expected {
        Value::String(expected) => matches_single_type(expected, value),
        Value::Array(expected) => expected
            .iter()
            .filter_map(Value::as_str)
            .any(|expected| matches_single_type(expected, value)),
        _ => false,
    }
}

fn matches_single_type(expected: &str, value: &Value) -> bool {
    match expected {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.is_number(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value.as_f64().is_some_and(|number| number.fract() == 0.0)
        }
        "string" => value.is_string(),
        _ => false,
    }
}

fn escape_path(name: &str) -> String {
    let mut escaped = String::new();
    let mut chars = name.chars();
    for character in chars.by_ref().take(MAX_DIAGNOSTIC_PATH_CHARS) {
        escaped.extend(character.escape_default());
    }
    if chars.next().is_some() {
        escaped.push('…');
    }
    escaped.replace('.', "\\.")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extracts_only_schema_envelope_methods() {
        assert!(is_known_server_request_method(
            "mcpServer/elicitation/request"
        ));
        assert!(is_known_server_notification_method("thread/deleted"));
        assert!(is_known_server_notification_method(
            "item/autoApprovalReview/completed"
        ));
        assert!(!is_known_server_notification_method("future/event"));
    }

    #[test]
    fn permission_profile_audit_visits_only_reachable_definitions() {
        let root = json!({
            "definitions": {
                "profile": {"type": "object"},
                "unused": {"futureKeyword": true}
            }
        });
        audit_schema_keywords(&json!({"$ref": "#/definitions/profile"}), &root).unwrap();
        assert!(
            audit_schema_keywords(&json!({"$ref": "#/definitions/unused"}), &root)
                .unwrap_err()
                .contains("futureKeyword")
        );
    }

    #[test]
    fn rejects_unknown_schema_keywords() {
        let schema = json!({
            "type": "object",
            "futureKeyword": true
        });
        let error = audit_schema_keywords(&schema, &schema).unwrap_err();
        assert!(error.contains("futureKeyword"));
    }

    #[test]
    fn schema_keyword_audit_ignores_property_and_definition_names() {
        let schema = json!({
            "definitions": {
                "futureKeyword": {"type": "string"}
            },
            "type": "object",
            "properties": {
                "futureKeyword": {"$ref": "#/definitions/futureKeyword"}
            },
            "dependencies": {
                "futureKeyword": ["other"]
            }
        });
        audit_schema_keywords(&schema, &schema).unwrap();
    }

    #[test]
    fn rejects_nested_permission_profile_schema_violations() {
        let invalid_profile = json!({
            "fileSystem": {
                "entries": [{
                    "access": "future",
                    "path": {"type": "special", "value": {"kind": "root"}}
                }]
            }
        });
        assert!(validate_permission_profile(&invalid_profile).is_err());
        assert!(serde_json::from_value::<crate::PermissionProfile>(invalid_profile).is_err());
    }

    #[test]
    fn rejects_recursive_schema_when_depth_budget_is_exhausted() {
        let recursive = json!({"$ref": "#"});
        let error = validate_document(&recursive, &json!({}), "recursive").unwrap_err();
        match error {
            ClientError::ProtocolViolation { reason } => {
                assert!(reason.contains("depth limit"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rejects_unique_item_work_when_step_budget_is_exhausted() {
        let schema = json!({"type": "array", "uniqueItems": true});
        let value = Value::Array((0..500).map(Value::from).collect());
        let error = validate_document(&schema, &value, "large unique array").unwrap_err();
        match error {
            ClientError::ProtocolViolation { reason } => {
                assert!(reason.contains("step limit"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn branch_validation_consumes_shared_budget_and_restores_depth() {
        let schema = json!({"type": "string"});
        let mut context = ValidationContext {
            remaining_steps: 2,
            depth: 0,
        };
        assert!(!branch_matches(&schema, &json!(1), &schema, "$", &mut context).unwrap());
        assert_eq!(context.remaining_steps, 1);
        assert_eq!(context.depth, 0);
        assert!(branch_matches(&schema, &json!("ok"), &schema, "$", &mut context).unwrap());
        assert_eq!(context.remaining_steps, 0);
        assert_eq!(context.depth, 0);
    }

    #[test]
    fn validation_documents_do_not_share_budget_state() {
        let expensive_schema = json!({"type": "array", "uniqueItems": true});
        let expensive_value = Value::Array((0..500).map(Value::from).collect());
        assert!(validate_document(&expensive_schema, &expensive_value, "expensive").is_err());
        validate_document(&json!({"type": "null"}), &Value::Null, "simple").unwrap();
    }

    #[test]
    fn diagnostic_paths_escape_control_characters() {
        let escaped = escape_path("line\n\u{1b}[31m");
        assert!(!escaped.contains('\n'));
        assert!(!escaped.contains('\u{1b}'));
    }
}
