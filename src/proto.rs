pub mod shredstream {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub mod shared {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/shared.rs"));
}
