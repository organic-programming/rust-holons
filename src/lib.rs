//! holons — Organic Programming SDK for Rust
//!
//! Transport, serve, and identity utilities for building holons in Rust.

pub mod connect;
pub mod discover;
pub mod identity;
pub mod serve;
pub mod transport;

#[cfg(test)]
pub(crate) mod test_support;
