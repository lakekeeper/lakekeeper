use std::{collections::HashSet, fmt::Debug, hash::Hash};

/// Validate that a vector of items is unique.
///
/// # Errors
/// Returns iceberg with `DataInvalid` error if the items are not unique.
pub fn validate_unique_vec<T>(items: &[T]) -> Result<(), iceberg::Error>
where
    T: Eq + Hash + Debug,
{
    let set: HashSet<_> = items.iter().collect();
    if set.len() != items.len() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Duplicate Values".to_string(),
        ));
    }

    Ok(())
}
