use std::collections::BTreeMap;

use crate::ErrorResult;

#[derive(Debug, Clone, Copy)]
pub struct KeyDirEntry {
    pub file_id: u128,
    pub offset: u64,
    pub timestamp: u128,
}

pub struct KeyDir {
    entries: BTreeMap<Vec<u8>, KeyDirEntry>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        KeyDir {
            entries: BTreeMap::new(),
        }
    }

    pub fn set(
        &mut self,
        key: &[u8],
        file_id: u128,
        offset: u64,
        timestamp: u128,
    ) -> ErrorResult<()> {

        log::trace!("set key={} ts={} offset={} file_id={}", String::from_utf8(key.to_vec())?, timestamp, offset, file_id);

        // XXX: insert works as "upsert":
        self.entries.insert(
            key.to_vec(),
            KeyDirEntry {
                file_id: file_id,
                offset: offset,
                timestamp: timestamp,
            },
        );

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> ErrorResult<KeyDirEntry> {
        if !self.entries.contains_key(key) {
            return Err(string_error::new_err("key not found"));
        }

        let entry = self.entries.get(key).unwrap();

        let entry = entry.clone();

        Ok(entry)
    }

    pub fn remove(&mut self, key: &[u8]) -> ErrorResult<()> {
        self.entries.remove(&key.to_vec());

        Ok(())
    }

    pub fn iter(&self) -> ErrorResult<std::collections::btree_map::Iter<Vec<u8>, KeyDirEntry>> {
        Ok(self.entries.iter())
    }

    #[allow(dead_code)]
    pub fn iter_mut(
        &mut self,
    ) -> ErrorResult<std::collections::btree_map::IterMut<Vec<u8>, KeyDirEntry>> {
        Ok(self.entries.iter_mut())
    }

    #[allow(dead_code)]
    pub fn range(
        &mut self,
        min: &[u8],
        max: &[u8],
    ) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry> {
        use std::ops::Bound::Included;

        let range = self
            .entries
            .range::<[u8], _>((Included(min), Included(max)));
        range
    }

    #[allow(dead_code)]
    pub fn range_from(
        &mut self,
        min: &[u8],
    ) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry> {
        use std::ops::Bound::{Included, Unbounded};

        let range = self.entries.range::<[u8], _>((Included(min), Unbounded));
        range
    }
}
