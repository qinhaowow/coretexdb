use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let proto_file = "src/coretex_grpc/coretex.proto";

    // Skip gRPC codegen if protoc is not available
    let protoc_ok = match env::var("PROTOC") {
        Ok(p) => std::process::Command::new(&p)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false),
        Err(_) => std::process::Command::new("protoc")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false),
    };

    if protoc_ok {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir(&out_dir)
            .compile(&[proto_file], &["src/coretex_grpc"])?;
        println!("cargo:rerun-if-changed={}", proto_file);
        println!("cargo:rerun-if-changed=src/coretex_grpc/");
        println!("cargo:rerun-if-env-changed=PROTOC");
    } else {
        // The gRPC service include!s $OUT_DIR/coretex.rs, so skipping codegen
        // here just produces a cryptic missing-file error later. Fail fast.
        return Err("protoc not found: install protobuf-compiler (or set PROTOC) to build the gRPC service".into());
    }

    Ok(())
}
