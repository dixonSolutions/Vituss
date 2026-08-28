//! Vindex kind registry — the plug-in point for new sharding functions.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;

use vituss_core::{Error, Result};

use crate::vindex::{VindexParams, VindexRef};

/// Constructs a vindex instance from its VSchema name and params.
pub type VindexBuilder = fn(name: &str, params: &VindexParams) -> Result<VindexRef>;

static REGISTRY: Lazy<RwLock<HashMap<&'static str, VindexBuilder>>> = Lazy::new(|| {
    let mut m: HashMap<&'static str, VindexBuilder> = HashMap::new();
    crate::impls::register_builtins(&mut m);
    RwLock::new(m)
});

/// Register a vindex kind, replacing any existing one of the same name.
pub fn register(kind: &'static str, builder: VindexBuilder) {
    REGISTRY.write().expect("vindex registry poisoned").insert(kind, builder);
}

/// Build a vindex instance.
pub fn create(kind: &str, name: &str, params: &VindexParams) -> Result<VindexRef> {
    let builder = {
        let reg = REGISTRY.read().expect("vindex registry poisoned");
        reg.get(kind).copied()
    };
    match builder {
        Some(b) => b(name, params),
        None => {
            let mut kinds = registered_kinds();
            kinds.sort();
            Err(Error::not_found(format!(
                "unknown vindex type {kind:?} for vindex {name:?}; registered types: {}",
                kinds.join(", ")
            )))
        }
    }
}

/// Every registered vindex kind.
pub fn registered_kinds() -> Vec<&'static str> {
    REGISTRY.read().expect("vindex registry poisoned").keys().copied().collect()
}

/// Convenience for implementors: wrap a concrete vindex as a [`VindexRef`].
pub fn boxed<T: crate::vindex::Vindex + 'static>(v: T) -> VindexRef {
    Arc::new(v)
}
