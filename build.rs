fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: Cargo executes this build script as a single-threaded process.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    let proto_root = "../ThreadWeave-protocols/proto";
    let protos = [
        "../ThreadWeave-protocols/proto/threadweave_protocols/execution/v1/execution.proto",
        "../ThreadWeave-protocols/proto/threadweave_protocols/runtime/v1/runtime.proto",
        "../ThreadWeave-protocols/proto/threadweave_protocols/broker/v1/broker.proto",
    ];

    tonic_prost_build::configure().compile_protos(&protos, &[proto_root])?;
    println!("cargo:rerun-if-changed={proto_root}/threadweave_protocols");
    Ok(())
}
