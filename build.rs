
#[cfg(feature = "grpc")]
fn main() {
    use tonic_build::compile_protos;
    use std::path::Path;

    let proto_dir = Path::new("src/cortex_api/grpc/proto");
    let proto_files = vec![
        proto_dir.join("cortexdb.proto"),
    ];

    compile_protos(&proto_files).unwrap();
}

#[cfg(not(feature = "grpc"))]
fn main() {}
