pub mod shredstream {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub mod shared {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/shared.rs"));
}

pub mod shreder_binary {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/shreder_binary.rs"));
}

pub mod arpc {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/arpc.rs"));
}

pub mod jetstream {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/jetstream.rs"));
}
