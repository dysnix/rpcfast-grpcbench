fn main() {
    const PROTOC_ENV: &str = "PROTOC";
    if std::env::var(PROTOC_ENV).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENV, protobuf_src::protoc());
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &["protos/shared.proto", "protos/shredstream.proto"],
            &["protos"],
        )
        .expect("compile ShredStream protobufs");
}
