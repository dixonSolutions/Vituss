//! # vituss-gate
//!
//! The query gateway — what a client actually connects to.
//!
//! It presents one logical database and hides however many shards are behind it.
//! Its state is entirely derived from the topology, so gates are interchangeable:
//! start more of them to scale out, stop one and clients reconnect elsewhere with
//! nothing lost.

pub mod discovery;
pub mod gate;
pub mod resolver;
pub mod sysvars;

pub use discovery::{Discovery, TabletEntry};
pub use gate::Gate;
pub use resolver::Resolver;

pub use vituss_engine::{new_session, SessionRef};
