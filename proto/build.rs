use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protobuf/");
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    let input_files = ["protobuf/quartzdb.proto", "protobuf/storage.proto"];
    let output_dir = PathBuf::from("src/protobuf");

    tonic_build::configure()
        .btree_map(["."])
        .type_attribute(
            "quartzdb.PutBatchRequestFOO",
            "#[derive(Eq, serde::Serialize, serde::Deserialize)]",
        )
        .out_dir(&output_dir)
        .file_descriptor_set_path(output_dir.join("services_descriptor.bin"))
        .include_file("mod.rs")
        .compile_protos_with_config(config, &input_files, &["protobuf"])?;
    Ok(())
}
