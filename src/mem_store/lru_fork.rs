// MIT License

// Copyright (c) 2016 Jerome Froelich

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#![allow(warnings)]
#[cfg(test)]
extern crate scoped_threadpool;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ptr;
use std::usize;

// Struct used to hold a reference to a key
struct KeyRef<K> {
    k: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

// Struct used to hold a key value pair. Also contains references to previous and next entries
// so we can maintain the entries in a linked list ordered by their use.
struct LruEntry<K, V> {
    key: K,
    val: V,
    prev: *mut LruEntry<K, V>,
    next: *mut LruEntry<K, V>,
}

impl<K, V> LruEntry<K, V> {
    fn new(key: K, val: V) -> Self {
        LruEntry {
            key: key,
            val: val,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

pub struct LruCache<K, V> {
    map: HashMap<KeyRef<K>, Box<LruEntry<K, V>>>,
    cap: usize,

    // head and tail are sigil nodes to faciliate inserting entries
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K: Hash + Eq, V> LruCache<K, V> {
    pub fn new(cap: usize) -> LruCache<K, V> {
        // NB: The compiler warns that cache does not need to be marked as mutable if we
        // declare it as such since we only mutate it inside the unsafe block.
        let cache = LruCache {
            map: HashMap::with_capacity(cap),
            cap,
            head: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K, V>>())) },
            tail: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K, V>>())) },
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    pub fn infinite_capacity() -> LruCache<K, V> {
        // NB: The compiler warns that cache does not need to be marked as mutable if we
        // declare it as such since we only mutate it inside the unsafe block.
        let cache = LruCache {
            map: HashMap::default(),
            cap: usize::MAX,
            head: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K, V>>())) },
            tail: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K, V>>())) },
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    pub fn put(&mut self, k: K, v: V) {
        let node_ptr = self.map.get_mut(&KeyRef { k: &k }).map(|node| {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;
            node_ptr
        });

        match node_ptr {
            Some(node_ptr) => {
                // if the key is already in the cache just update its value and move it to the
                // front of the list
                unsafe { (*node_ptr).val = v };
                self.detach(node_ptr);
                self.attach(node_ptr);
            }
            None => {
                let mut node = if self.len() == self.cap() {
                    // if the cache is full, remove the last entry so we can use it for the new key
                    let old_key = KeyRef {
                        k: unsafe { &(*(*self.tail).prev).key },
                    };
                    let mut old_node = self.map.remove(&old_key).unwrap();

                    old_node.key = k;
                    old_node.val = v;

                    let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                    self.detach(node_ptr);

                    old_node
                } else {
                    // if the cache is not full allocate a new LruEntry
                    Box::new(LruEntry::new(k, v))
                };

                let node_ptr: *mut LruEntry<K, V> = &mut *node;
                self.attach(node_ptr);

                let keyref = unsafe { &(*node_ptr).key };
                self.map.insert(KeyRef { k: keyref }, node);
            }
        }
    }

    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        let key = KeyRef {
            k: unsafe { &(*(*self.tail).prev).key },
        };
        let mut node = self.map.remove(&key)?;

        let node_ptr: *mut LruEntry<K, V> = &mut *node;
        self.detach(node_ptr);

        // Required because of https://github.com/rust-lang/rust/issues/28536
        let node = *node;
        let LruEntry { key, val, .. } = node;
        Some((key, val))
    }

    pub fn get<'a>(&'a mut self, k: &K) -> Option<&'a V> {
        let key = KeyRef { k: k };
        let (node_ptr, value) = match self.map.get_mut(&key) {
            None => (None, None),
            Some(node) => {
                let node_ptr: *mut LruEntry<K, V> = &mut **node;
                // we need to use node_ptr to get a reference to val here because
                // detach and attach require a mutable reference to self here which
                // would be disallowed if we set value equal to &node.val
                (Some(node_ptr), Some(unsafe { &(*node_ptr).val }))
            }
        };

        match node_ptr {
            None => (),
            Some(node_ptr) => {
                self.detach(node_ptr);
                self.attach(node_ptr);
            }
        }

        value
    }

    pub fn get_mut<'a>(&'a mut self, k: &K) -> Option<&'a mut V> {
        let key = KeyRef { k: k };
        let (node_ptr, value) = match self.map.get_mut(&key) {
            None => (None, None),
            Some(node) => {
                let node_ptr: *mut LruEntry<K, V> = &mut **node;
                // we need to use node_ptr to get a reference to val here because
                // detach and attach require a mutable reference to self here which
                // would be disallowed if we set value equal to &node.val
                (Some(node_ptr), Some(unsafe { &mut (*node_ptr).val }))
            }
        };

        match node_ptr {
            None => (),
            Some(node_ptr) => {
                self.detach(node_ptr);
                self.attach(node_ptr);
            }
        }

        value
    }

    pub fn peek<'a>(&'a self, k: &K) -> Option<&'a V> {
        let key = KeyRef { k: k };
        match self.map.get(&key) {
            None => None,
            Some(node) => Some(&node.val),
        }
    }

    pub fn contains(&self, k: &K) -> bool {
        let key = KeyRef { k: k };
        self.map.contains_key(&key)
    }

    pub fn pop(&mut self, k: &K) -> Option<V> {
        let key = KeyRef { k: k };
        match self.map.remove(&key) {
            None => None,
            Some(lru_entry) => Some(lru_entry.val),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn resize(&mut self, cap: usize) {
        // return early if capacity doesn't change
        if cap == self.cap {
            return;
        }

        let mut new_map: HashMap<KeyRef<K>, Box<LruEntry<K, V>>> = HashMap::with_capacity(cap);

        let mut current;
        unsafe { current = (*self.head).next };
        while current != self.tail {
            if new_map.len() < cap {
                let key = unsafe { &(*current).key };
                let keyref = KeyRef { k: key };

                // remove node from old map so its destructor isn't run
                let node = self.map.remove(&keyref).unwrap();
                new_map.insert(keyref, node);

                unsafe { current = (*current).next }
            } else {
                // we are at max capacity so we can just update the tail and break
                self.detach(current);
                unsafe {
                    (*(*current).prev).next = self.tail;
                    self.tail = (*current).prev;
                }
                break;
            }
        }

        self.map = new_map;
        self.cap = cap;
    }

    pub fn clear(&mut self) {
        loop {
            let prev;
            unsafe { prev = (*self.tail).prev }
            if prev != self.head {
                let old_key = KeyRef {
                    k: unsafe { &(*(*self.tail).prev).key },
                };
                let mut old_node = self.map.remove(&old_key).unwrap();
                let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                self.detach(node_ptr);
            } else {
                break;
            }
        }
    }

    fn detach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*self.head).next = node;
            (*(*node).next).prev = node;
        }
    }
}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        // Prevent compiler from trying to drop the un-initialized fields key and val in head
        // and tail
        unsafe {
            let head = *Box::from_raw(self.head);
            let tail = *Box::from_raw(self.tail);

            let LruEntry {
                next: _,
                prev: _,
                key: head_key,
                val: head_val,
            } = head;
            let LruEntry {
                next: _,
                prev: _,
                key: tail_key,
                val: tail_val,
            } = tail;

            mem::forget(head_key);
            mem::forget(head_val);
            mem::forget(tail_key);
            mem::forget(tail_val);
        }
    }
}

// The compiler does not automatically derive Send and Sync for LruCache because it contains
// raw pointers. The raw pointers are safely encapsulated by LruCache though so we can
// implement Send and Sync for it below.
unsafe impl<K: Sync + Send, V: Sync + Send> Send for LruCache<K, V> {}

unsafe impl<K: Sync + Send, V: Sync + Send> Sync for LruCache<K, V> {}
