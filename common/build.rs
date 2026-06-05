use std::path::PathBuf;

const DERIVE_ATTRS: &str = "#[derive(serde::Deserialize, serde::Serialize)]";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protobuf/");
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    let input_files = ["protobuf/quartzdb.proto", "protobuf/metastore.proto"];
    let output_dir = PathBuf::from("src/protobuf");

    tonic_prost_build::configure()
        .btree_map(".")
        .type_attribute("quartzdb.FieldName", DERIVE_ATTRS)
        .type_attribute("quartzdb.FieldType", DERIVE_ATTRS)
        .type_attribute("quartzdb.FieldConfig", DERIVE_ATTRS)
        .type_attribute("quartzdb.TableConfig", DERIVE_ATTRS)
        .type_attribute("quartzdb.StorageSettings", DERIVE_ATTRS)
        .type_attribute("quartzdb.IngesterSettings", DERIVE_ATTRS)
        .type_attribute("quartzdb.SearcherSettings", DERIVE_ATTRS)
        .type_attribute("quartzdb.RetentionSettings", DERIVE_ATTRS)
        .type_attribute("quartzdb.TableSettings", DERIVE_ATTRS)
        .type_attribute("quartzdb.TableMeta", DERIVE_ATTRS)
        .type_attribute("quartzdb.FieldValue", DERIVE_ATTRS)
        .type_attribute("quartzdb.FieldValue.kind", DERIVE_ATTRS)
        .type_attribute("quartzdb.SplitMeta", DERIVE_ATTRS)
        .type_attribute("quartzdb.ProtoDocument", DERIVE_ATTRS)
        .field_attribute(
            "quartzdb.FieldConfig.field_type",
            "#[serde(rename = \"type\")]",
        )
        .out_dir(&output_dir)
        .file_descriptor_set_path(output_dir.join("services_descriptor.bin"))
        .include_file("mod.rs")
        .compile_with_config(config, &input_files, &["protobuf"])?;
    Ok(())
}
