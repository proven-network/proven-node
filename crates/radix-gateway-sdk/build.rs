use std::collections::HashMap;
use std::fs;
use std::path::Path;

use progenitor::InterfaceStyle;
use serde_yaml::Value;

fn main() {
    let api_schema_location = Path::new("./openapi/gateway-api-schema.yaml");
    println!(
        "cargo:rerun-if-changed={}",
        api_schema_location.to_str().unwrap()
    );

    let prepared_schema = prepare_schema_for_generation(api_schema_location);
    let spec = serde_yaml::from_str(&prepared_schema).unwrap();
    let mut generator = progenitor::Generator::new(
        progenitor::GenerationSettings::default().with_interface(InterfaceStyle::Builder),
    );

    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);

    let out_file = Path::new("./src/codegen.rs");

    std::fs::write(out_file, content).unwrap();
}

fn prepare_schema_for_generation(original_schema_file: &Path) -> String {
    let schema_content =
        fs::read_to_string(original_schema_file).expect("Unable to read schema file");
    let mut schema: Value =
        serde_yaml::from_str(&schema_content).expect("Unable to parse schema file");

    split_out_inherited_discriminated_types(&mut schema);

    serde_yaml::to_string(&schema).expect("Unable to serialize schema")
}

fn split_out_inherited_discriminated_types(schema: &mut Value) {
    let components = schema.get_mut("components").unwrap();
    let types = components
        .get_mut("schemas")
        .unwrap()
        .as_mapping_mut()
        .unwrap();
    let mut type_refs_changed = HashMap::new();
    let original_type_names: Vec<String> = types
        .keys()
        .map(|k| k.as_str().unwrap().to_string())
        .collect();

    // First pass - find discriminated unions and split them
    for type_name in &original_type_names {
        if let Some(type_data) = types.get(type_name) {
            let has_discriminator = type_data.get("discriminator").is_some();
            let is_object = type_data.get("type").map_or(false, |t| t == "object");
            let has_inheritance = type_data.get("allOf").is_some()
                || type_data.get("anyOf").is_some()
                || type_data.get("oneOf").is_some();

            if has_discriminator && is_object && !has_inheritance {
                // Create base type data by excluding discriminator
                let mut base_type_data = Value::Mapping(serde_yaml::Mapping::new());
                if let Some(mapping) = type_data.as_mapping() {
                    for (key, value) in mapping {
                        if key != "discriminator" {
                            base_type_data
                                .as_mapping_mut()
                                .unwrap()
                                .insert(key.clone(), value.clone());
                        }
                    }
                }

                // Create union type data
                let discriminator = type_data.get("discriminator").unwrap();
                let child_refs: Vec<Value> = discriminator
                    .get("mapping")
                    .unwrap()
                    .as_mapping()
                    .unwrap()
                    .values()
                    .map(|v| {
                        let mut map = serde_yaml::Mapping::new();
                        map.insert(Value::String("$ref".to_string()), v.clone());
                        Value::Mapping(map)
                    })
                    .collect();

                let mut union_type_data = serde_yaml::Mapping::new();
                union_type_data.insert(
                    Value::String("oneOf".to_string()),
                    Value::Sequence(child_refs),
                );
                union_type_data.insert(
                    Value::String("discriminator".to_string()),
                    discriminator.clone(),
                );

                // Generate unique base type name
                let mut base_type_name = format!("{}Base", type_name);
                while types.contains_key(Value::String(base_type_name.clone())) {
                    base_type_name = format!("{}Derived", base_type_name);
                }

                let type_ref = format!("#/components/schemas/{}", type_name);
                let base_type_ref = format!("#/components/schemas/{}", base_type_name);

                type_refs_changed.insert(type_ref, (base_type_ref, discriminator.clone()));

                // Update schema
                types.insert(Value::String(base_type_name), base_type_data);
                types.insert(
                    Value::String(type_name.clone()),
                    Value::Mapping(union_type_data),
                );
            }
        }
    }

    // Second pass - update references to new types
    for type_name in &original_type_names {
        if let Some(type_data) = types.get_mut(type_name) {
            if let Some(all_of) = type_data.get_mut("allOf") {
                if let Some(all_of_seq) = all_of.as_sequence_mut() {
                    let mut match_details = None;

                    // Update inherited type references
                    for inherited_type in all_of_seq.iter_mut() {
                        if let Some(ref_val) = inherited_type.get("$ref") {
                            let ref_str = ref_val.as_str().unwrap().to_string();
                            if let Some((base_ref, discriminator)) = type_refs_changed.get(&ref_str)
                            {
                                inherited_type.as_mapping_mut().unwrap().insert(
                                    Value::String("$ref".to_string()),
                                    Value::String(base_ref.clone()),
                                );
                                match_details = Some((discriminator.clone(), ref_str));
                            }
                        }
                    }

                    // Add discriminator property if needed
                    if let Some((discriminator, _parent_ref)) = match_details {
                        if let Some(mapping) = discriminator.get("mapping") {
                            let own_tag = mapping
                                .as_mapping()
                                .unwrap()
                                .iter()
                                .find(|(_, v)| {
                                    v.as_str().unwrap()
                                        == format!("#/components/schemas/{}", type_name)
                                })
                                .map(|(k, _)| k.as_str().unwrap().to_string());

                            if let Some(tag) = own_tag {
                                let property_name =
                                    discriminator.get("propertyName").unwrap().as_str().unwrap();

                                // Find or create inner type data
                                let inner_type_idx = all_of_seq.iter().position(|v| {
                                    v.get("type").map_or(false, |t| t == "object")
                                        && v.get("properties").is_some()
                                });

                                let inner_type = if let Some(idx) = inner_type_idx {
                                    &mut all_of_seq[idx]
                                } else {
                                    let mut new_inner = serde_yaml::Mapping::new();
                                    new_inner.insert(
                                        Value::String("type".to_string()),
                                        Value::String("object".to_string()),
                                    );
                                    new_inner.insert(
                                        Value::String("properties".to_string()),
                                        Value::Mapping(serde_yaml::Mapping::new()),
                                    );
                                    all_of_seq.push(Value::Mapping(new_inner));
                                    all_of_seq.last_mut().unwrap()
                                };

                                // Add discriminator property
                                let mut enum_prop = serde_yaml::Mapping::new();
                                enum_prop.insert(
                                    Value::String("type".to_string()),
                                    Value::String("string".to_string()),
                                );
                                enum_prop.insert(
                                    Value::String("enum".to_string()),
                                    Value::Sequence(vec![Value::String(tag)]),
                                );

                                inner_type
                                    .get_mut("properties")
                                    .unwrap()
                                    .as_mapping_mut()
                                    .unwrap()
                                    .insert(
                                        Value::String(property_name.to_string()),
                                        Value::Mapping(enum_prop),
                                    );
                            }
                        }
                    }
                }
            }
        }
    }
}
