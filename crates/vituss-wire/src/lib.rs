//! # vituss-wire
//!
//! The client-facing servers. A client connects with its own driver and its own
//! protocol; Vituss looks like an ordinary database server of that kind.
//!
//! Which protocol a client speaks and which engine the shards run are independent
//! choices. A MySQL application can be served from PostgreSQL shards and vice
//! versa: the statement is parsed with the client's grammar, planned once, and
//! re-rendered for each shard's engine on the way down.

#[cfg(feature = "mysql-server")]
pub mod mysql;
#[cfg(feature = "postgres-server")]
pub mod postgres;
