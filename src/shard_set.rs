//! # ShardSet
//!
//! A concurrent set based on a [`ShardMap`] with values of `()`.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use whirlwind::ShardSet;
//!
//! let rt = tokio::runtime::Runtime::new().unwrap();
//! let set = Arc::new(ShardSet::new());
//!
//! rt.block_on(async {
//!     for i in 0..10 {
//!         let k = i;
//!         if i % 2 == 0 {
//!             set.insert(k).await;
//!         } else {
//!             set.remove(&(k-1)).await;
//!         }
//!     }
//! });
//! ```
//!
use std::hash::{BuildHasher, Hash, RandomState};

use crate::{mapref::MapRef, shard_map::ShardMap};

/// A concurrent set based on a [`ShardMap`] with values of `()`.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use whirlwind::ShardSet;
///
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// let set = Arc::new(ShardSet::new());
///
/// rt.block_on(async {
///     for i in 0..10 {
///         let k = i;
///         if i % 2 == 0 {
///             set.insert(k).await;
///         } else {
///             set.remove(&(k-1)).await;
///         }
///     }
/// });
/// ```
///
pub struct ShardSet<T, S = RandomState> {
    inner: ShardMap<T, (), S>,
}

impl<T, S> Clone for ShardSet<T, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Eq + Hash + 'static> Default for ShardSet<T, RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Eq + Hash + 'static> ShardSet<T, RandomState> {
    pub fn new() -> Self {
        Self {
            inner: ShardMap::new(),
        }
    }

    pub fn with_shards(shards: usize) -> Self {
        Self {
            inner: ShardMap::with_shards(shards),
        }
    }

    /// Creates a new `ShardSet` with the default hasher and space for at least `capacity` values.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: ShardMap::with_capacity(capacity),
        }
    }

    /// Creates a new `ShardSet` with the default hasher, `shards` shards, and space for at least
    /// `capacity` values.
    pub fn with_shards_and_capacity(shards: usize, capacity: usize) -> Self {
        Self {
            inner: ShardMap::with_shards_and_capacity(shards, capacity),
        }
    }
}

impl<T, S> ShardSet<T, S>
where
    T: Eq + std::hash::Hash + 'static,
    S: BuildHasher,
{
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: ShardMap::with_hasher(hasher),
        }
    }

    pub fn with_shards_and_hasher(shards: usize, hasher: S) -> Self {
        Self {
            inner: ShardMap::with_shards_and_hasher(shards, hasher),
        }
    }

    /// Creates a new `ShardSet` with the provided hasher `S` and space for at least `capacity`
    /// values.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            inner: ShardMap::with_capacity_and_hasher(capacity, hasher),
        }
    }

    /// Creates a new `ShardSet` with the provided hasher `S`, `shards` shards, and space for at
    /// least `capacity` values.
    pub fn with_shards_and_capacity_and_hasher(shards: usize, capacity: usize, hasher: S) -> Self {
        Self {
            inner: ShardMap::with_shards_and_capacity_and_hasher(shards, capacity, hasher),
        }
    }

    /// Inserts a value into the set.
    ///
    /// Returns `true` if the value was not already present in the set.
    pub async fn insert(&self, value: T) -> bool {
        self.inner.insert(value, ()).await.is_none()
    }

    /// Returns a reference to the value in the set, if it is present.
    pub async fn get<'a>(&'a self, value: &T) -> Option<SetRef<'a, T>> {
        self.inner.get(value).await.map(SetRef::new)
    }

    /// Returns `true` if the set contains the specified value.
    pub async fn contains(&self, value: &T) -> bool {
        self.inner.contains_key(value).await
    }

    /// Adds a value to the set, returning `true` if the value was already present.
    ///
    /// This is equivalent to [`HashSet::replace`](std::collections::HashSet::replace).
    pub async fn replace(&self, value: T) -> bool {
        self.inner.insert(value, ()).await.is_some()
    }

    /// Removes a value from the set and returns it, if it was present.
    pub async fn take(&self, value: &T) -> Option<T> {
        self.inner.remove_entry(value).await.map(|(value, _)| value)
    }

    /// Removes a value from the set.
    ///
    /// Returns `true` if the value was present in the set.
    pub async fn remove(&self, value: &T) -> bool {
        self.inner.remove(value).await.is_some()
    }

    /// Returns the number of elements in the set.
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// Returns `true` if the set is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }

    /// Clears the set, removing all values.
    pub async fn clear(&self) {
        self.inner.clear().await;
    }

    /// Returns an iterator over the values in the set.
    pub async fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.keys().await
    }
}

impl<T, S> FromIterator<T> for ShardSet<T, S>
where
    T: Eq + std::hash::Hash + 'static,
    S: BuildHasher + Default,
{
    /// Creates a `ShardSet` from an iterator of values.
    ///
    /// # Example
    /// ```
    /// use whirlwind::ShardSet;
    ///
    /// let rt = tokio::runtime::Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let set: ShardSet<_> = ["foo", "bar", "foo"].into_iter().collect();
    ///     assert_eq!(set.len().await, 2);
    /// });
    /// ```
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            inner: iter.into_iter().map(|value| (value, ())).collect(),
        }
    }
}

/// A reference to a value in a [`ShardSet`].
///
/// Holds a shared (read-only) lock on the shard associated with the value. Dropping this
/// reference will release the lock.
pub struct SetRef<'a, T> {
    inner: MapRef<'a, T, ()>,
}

impl<'a, T> SetRef<'a, T>
where
    T: Eq + std::hash::Hash,
{
    pub(crate) fn new(inner: MapRef<'a, T, ()>) -> Self {
        Self { inner }
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &T {
        self.inner.key()
    }
}

impl<T> std::ops::Deref for SetRef<'_, T>
where
    T: Eq + std::hash::Hash,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.key()
    }
}
