fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from("src/gen");
    std::fs::create_dir_all(&out_dir)?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(
            &["../go-holons/protos/holonmeta/v1/holonmeta.proto"],
            &["../go-holons/protos"],
        )?;

    println!("cargo:rerun-if-changed=../go-holons/protos/holonmeta/v1/holonmeta.proto");
    Ok(())
}
