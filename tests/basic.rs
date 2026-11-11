use std::hash::RandomState;
use std::sync::Arc;
use whirlwind::*;

#[tokio::test]
async fn test_shardmap() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    assert_eq!(map.len().await, 1);
    assert!(map.contains_key(&"foo").await);
    assert!(!map.contains_key(&"bar").await);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"bar");
    assert!(map.get(&"bar").await.is_none());
    assert_eq!(map.remove(&"foo").await, Some("bar"));
    assert_eq!(map.len().await, 0);
    assert!(!map.contains_key(&"foo").await);
}

#[tokio::test]
async fn test_shardmap_clone() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    let map2 = map.clone();
    assert_eq!(map2.len().await, 1);
    assert!(map2.contains_key(&"foo").await);
    assert!(!map2.contains_key(&"bar").await);
    assert_eq!(map2.get(&"foo").await.unwrap().value(), &"bar");
    assert!(map2.get(&"bar").await.is_none());
    assert_eq!(map2.remove(&"foo").await, Some("bar"));
    assert_eq!(map2.len().await, 0);
    assert!(!map2.contains_key(&"foo").await);
}

#[tokio::test]
async fn test_shardmap_shards() {
    let map = ShardMap::with_shards(4);
    map.insert("foo", "bar").await;
    assert_eq!(map.len().await, 1);
    assert!(map.contains_key(&"foo").await);
    assert!(!map.contains_key(&"bar").await);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"bar");
    assert!(map.get(&"bar").await.is_none());
    assert_eq!(map.remove(&"foo").await, Some("bar"));
    assert_eq!(map.len().await, 0);
    assert!(!map.contains_key(&"foo").await);
}

#[tokio::test]
async fn test_shardmap_len() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    assert_eq!(map.len().await, 1);
    map.insert("foo2", "bar2").await;
    assert_eq!(map.len().await, 2);
    map.remove(&"foo").await;
    assert_eq!(map.len().await, 1);
    map.remove(&"foo2").await;
    assert_eq!(map.len().await, 0);
}

#[tokio::test]
async fn test_shardmap_is_empty() {
    let map = ShardMap::new();
    assert!(map.is_empty().await);
    map.insert("foo", "bar").await;
    assert!(!map.is_empty().await);
    map.remove(&"foo").await;
    assert!(map.is_empty().await);
}

#[tokio::test]
async fn test_shardmap_iter() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    map.insert("baz", "qux").await;

    let mut pairs = Vec::new();
    for (key, value) in map.iter().await {
        pairs.push((*key, *value));
    }

    assert_eq!(pairs.len(), 2);
    assert!(pairs.contains(&("foo", "bar")));
    assert!(pairs.contains(&("baz", "qux")));
    // Verify map state is unchanged
    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"bar");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_iter_empty() {
    let map = ShardMap::new();
    let pairs: Vec<(&&str, &&str)> = map.iter().await.collect();
    assert_eq!(pairs.len(), 0);
    assert_eq!(map.len().await, 0);
}

#[tokio::test]
async fn test_shardmap_keys() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    map.insert("baz", "qux").await;

    let mut keys = Vec::new();
    for key in map.keys().await {
        keys.push(*key);
    }

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"foo"));
    assert!(keys.contains(&"baz"));
    // Verify map state is unchanged
    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"bar");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_values() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    map.insert("baz", "qux").await;

    let mut values = Vec::new();
    for value in map.values().await {
        values.push(*value);
    }

    assert_eq!(values.len(), 2);
    assert!(values.contains(&"bar"));
    assert!(values.contains(&"qux"));
    // Verify map state is unchanged
    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"bar");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_iter_mut() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    map.insert("baz", "qux").await;

    for (key, value) in map.iter_mut().await {
        if *key == "foo" {
            *value = "updated";
        }
    }

    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"updated");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_values_mut() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;
    map.insert("baz", "qux").await;

    for value in map.values_mut().await {
        if *value == "bar" {
            *value = "updated";
        }
    }

    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"updated");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_iter_mut_empty() {
    let map = ShardMap::new();
    let pairs: Vec<(&&str, &mut &str)> = map.iter_mut().await.collect();
    assert_eq!(pairs.len(), 0);
    assert_eq!(map.len().await, 0);
}

#[tokio::test]
async fn test_shardmap_from_iter() {
    let map: ShardMap<_, _> = [("foo", "bar"), ("baz", "qux"), ("foo", "new")]
        .into_iter()
        .collect();
    assert_eq!(map.len().await, 2);
    assert_eq!(map.get(&"foo").await.unwrap().value(), &"new");
    assert_eq!(map.get(&"baz").await.unwrap().value(), &"qux");
}

#[tokio::test]
async fn test_shardmap_remove_entry() {
    let map = ShardMap::new();
    map.insert("foo", "bar").await;

    assert_eq!(map.remove_entry(&"foo").await, Some(("foo", "bar")));
    assert_eq!(map.remove_entry(&"foo").await, None);
    assert_eq!(map.len().await, 0);
}

#[tokio::test]
async fn test_shardset_from_iter() {
    let set: ShardSet<_> = ["foo", "bar", "foo"].into_iter().collect();
    assert_eq!(set.len().await, 2);
    assert!(set.contains(&"foo").await);
    assert!(set.contains(&"bar").await);
}

#[tokio::test]
async fn test_shardset_clone() {
    let set = ShardSet::new();
    set.insert("foo").await;

    let cloned = set.clone();
    assert_eq!(cloned.len().await, 1);
    assert!(cloned.contains(&"foo").await);

    cloned.insert("bar").await;
    assert_eq!(set.len().await, 2);
}

#[tokio::test]
async fn test_shardset_capacity() {
    let set: ShardSet<&str> = ShardSet::with_capacity(16);
    assert!(set.is_empty().await);

    let set: ShardSet<&str> = ShardSet::with_shards_and_capacity(4, 16);
    assert!(set.is_empty().await);

    let set: ShardSet<&str> = ShardSet::with_capacity_and_hasher(16, RandomState::new());
    assert!(set.is_empty().await);

    let set: ShardSet<&str> =
        ShardSet::with_shards_and_capacity_and_hasher(4, 16, RandomState::new());
    assert!(set.is_empty().await);
}

#[tokio::test]
async fn test_shardset_constructors() {
    let set: ShardSet<&str> = ShardSet::with_shards(4);
    assert!(set.is_empty().await);

    let set: ShardSet<&str> = ShardSet::with_hasher(RandomState::new());
    assert!(set.is_empty().await);

    let set: ShardSet<&str> = ShardSet::with_shards_and_hasher(4, RandomState::new());
    assert!(set.is_empty().await);
}

#[tokio::test]
async fn test_shardset_insert_returns_bool() {
    let set = ShardSet::new();
    assert!(set.insert("foo").await);
    assert!(!set.insert("foo").await);
    assert_eq!(set.len().await, 1);
}

#[tokio::test]
async fn test_shardset_replace_get_take() {
    let set = ShardSet::new();

    assert!(!set.replace("foo").await);
    assert!(set.replace("foo").await);

    let value = set.get(&"foo").await.unwrap();
    assert_eq!(*value, "foo");
    assert_eq!(value.value(), &"foo");
    drop(value);

    assert_eq!(set.take(&"foo").await, Some("foo"));
    assert_eq!(set.take(&"foo").await, None);
    assert!(set.is_empty().await);
}

#[tokio::test]
async fn test_shardset_iter() {
    let set = ShardSet::new();
    set.insert("foo").await;
    set.insert("bar").await;

    let mut items = Vec::new();
    for item in set.iter().await {
        items.push(*item);
    }

    assert_eq!(items.len(), 2);
    assert!(items.contains(&"foo"));
    assert!(items.contains(&"bar"));
    // Verify set state is unchanged
    assert_eq!(set.len().await, 2);
    assert!(set.contains(&"foo").await);
    assert!(set.contains(&"bar").await);
}

#[tokio::test]
async fn test_get_with_local_key_binding() {
    let map = ShardMap::new();
    for i in 0..10u64 {
        map.insert(i, i).await;
    }

    // The key binding is a local that outlives neither the map nor the returned
    // reference, which must still compile.
    for i in 0..10u64 {
        let key = i;
        if let Some(value) = map.get(&key).await {
            assert_eq!(*value.value(), i);
        }
    }

    for i in 0..10u64 {
        let key = i;
        if let Some(mut value) = map.get_mut(&key).await {
            *value.value_mut() += 1;
        }
    }

    let set = ShardSet::new();
    for i in 0..10u64 {
        set.insert(i).await;
    }

    for i in 0..10u64 {
        let value = i;
        if let Some(entry) = set.get(&value).await {
            assert_eq!(*entry.value(), i);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_reads_and_writes() {
    let map = Arc::new(ShardMap::new());

    for i in 0..64u64 {
        map.insert(i, i).await;
    }

    let mut handles = Vec::new();
    for task in 0..8u64 {
        let map = Arc::clone(&map);
        handles.push(tokio::spawn(async move {
            for i in 0..500u64 {
                let key = (i + task * 7919) % 64;
                let value = map.get(&key).await.map(|v| *v.value());
                assert!(value.is_some());

                map.insert(1_000 + task * 1_000 + i, i).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert!(!map.is_empty().await);
    assert_eq!(map.len().await, 64 + 8 * 500);
}
