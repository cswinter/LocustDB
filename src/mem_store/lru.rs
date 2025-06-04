use crate::mem_store::partition::ColumnLocator;
use lru::LruCache;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Lru {
    cache: Arc<Mutex<LruCache<ColumnLocator, ()>>>,
}

impl Lru {
    pub fn touch(&self, column: &ColumnLocator) {
        let mut cache = self.cache.lock().unwrap();
        cache.get(column);
    }

    pub fn put(&self, column: ColumnLocator) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(column, ());
    }

    pub fn remove(&self, column: &ColumnLocator) {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(column);
    }

    pub fn evict(&self) -> Option<ColumnLocator> {
        let mut cache = self.cache.lock().unwrap();
        cache.pop_lru().map(|x| x.0)
    }
}

impl Default for Lru {
    fn default() -> Lru {
        Lru {
            cache: Arc::new(Mutex::new(LruCache::unbounded())),
        }
    }
}
