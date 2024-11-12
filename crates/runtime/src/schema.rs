use std::collections::HashSet;
use std::sync::LazyLock;

pub static SCHEMA_WHLIST: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut set = HashSet::with_capacity(1);
    set.insert("proven:".to_string());
    set
});
