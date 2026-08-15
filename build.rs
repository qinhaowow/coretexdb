use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let proto_file = "src/coretex_grpc/coretex.proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .compile(&[proto_file], &["src/coretex_grpc"])?;

    println!("cargo:rerun-if-changed={}", proto_file);
    println!("cargo:rerun-if-changed=src/coretex_grpc/");

    Ok(())
}
