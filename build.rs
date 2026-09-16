fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/clusdr/v1alpha1";
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                format!("{proto}/health.proto"),
                format!("{proto}/membership.proto"),
                format!("{proto}/watch.proto"),
                format!("{proto}/events.proto"),
                format!("{proto}/locks.proto"),
                format!("{proto}/leases.proto"),
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
