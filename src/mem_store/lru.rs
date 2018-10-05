use std::sync::{Arc, Mutex};
use lru::LruCache;
use mem_store::partition::ColumnKey;


#[derive(Clone)]
pub struct LRU {
    cache: Arc<Mutex<LruCache<ColumnKey, ()>>>,
}

impl LRU {
    pub fn touch(&self, column: &ColumnKey) {
        let mut cache = self.cache.lock().unwrap();
        cache.get(column);
    }

    pub fn put(&self, column: ColumnKey) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(column, ());
    }

    pub fn remove(&self, column: &ColumnKey) {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(column);
    }

    pub fn evict(&self) -> Option<ColumnKey> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop_lru().map(|x| x.0)
    }
}

impl Default for LRU {
    fn default() -> LRU {
        LRU {
            cache: Arc::new(Mutex::new(LruCache::unbounded()))
        }
    }
}