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
        log::trace!(
            "set key={} ts={} offset={} file_id={}",
            String::from_utf8(key.to_vec())?,
            timestamp,
            offset,
            file_id
        );

        // XXX: insert works as "upsert":
        self.entries.insert(
            key.to_vec(),
            KeyDirEntry {
                file_id,
                offset,
                timestamp,
            },
        );

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> ErrorResult<KeyDirEntry> {
        if !self.entries.contains_key(key) {
            let key_str = format!("key not found: {}", String::from_utf8(key.to_vec())?);
            return Err(string_error::new_err(key_str.as_str()));
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

    pub fn keys(&self) -> std::collections::btree_map::Keys<Vec<u8>, KeyDirEntry> {
        self.entries.keys()
    }

    pub fn keys_range(
        &self,
        min: &[u8],
        max: &[u8],
    ) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry> {
        use std::ops::Bound::Included;

        let range = self
            .entries
            .range::<[u8], _>((Included(min), Included(max)));
        range
    }

    pub fn keys_range_min(
        &self,
        min: &[u8],
    ) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry> {
        use std::ops::Bound::Included;
        use std::ops::Bound::Unbounded;

        let range = self.entries.range::<[u8], _>((Included(min), Unbounded));
        range
    }

    pub fn keys_range_max(
        &self,
        max: &[u8],
    ) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry> {
        use std::ops::Bound::Included;
        use std::ops::Bound::Unbounded;

        let range = self.entries.range::<[u8], _>((Unbounded, Included(max)));
        range
    }
}
