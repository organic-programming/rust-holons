//! holons — Organic Programming SDK for Rust
//!
//! Transport, serve, and identity utilities for building holons in Rust.

pub mod connect;
pub mod describe;
pub mod discover;
pub mod gen {
    pub mod holonmeta {
        pub mod v1 {
            include!("gen/holonmeta.v1.rs");
        }
    }
}
pub mod identity;
pub mod serve;
pub mod transport;

#[cfg(test)]
pub(crate) mod test_support;
