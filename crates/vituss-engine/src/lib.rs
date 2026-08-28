//! # vituss-engine
//!
//! Executes plans. Given a [`Plan`](vituss_planner::Plan) it fans statements out
//! to shards, renders each one for the engine that shard actually runs, and
//! recombines the answers.
//!
//! It reaches the world through two traits and nothing else:
//! [`ShardGateway`] (how to find and talk to a shard) and
//! [`VCursor`](vituss_vindex::VCursor) (how a lookup vindex runs its own query).
//! That is what lets the same executor drive an in-process test cluster and a
//! real one.

pub mod combine;
pub mod cursor;
pub mod exec;
pub mod gateway;

pub use combine::{aggregate, concat, distinct, limit, merge_sorted, sort};
pub use cursor::EngineCursor;
pub use exec::{new_session, Executor};
pub use gateway::{SessionRef, ShardGateway};
