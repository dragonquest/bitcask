use std::collections::BTreeMap;

use crate::ErrorResult;

#[derive(Debug, Clone, Copy)]
pub struct KeyDirEntry {
    pub file_id: u128,
    pub offset: u64,
    pub timestamp: u128,
}

#[derive(Default)]
pub struct KeyDir {
    entries: BTreeMap<Vec<u8>, KeyDirEntry>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        Self::default()
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

    // TODO this should probably return a reference to the KeyDirEntry
    pub fn get(&self, key: &[u8]) -> ErrorResult<KeyDirEntry> {
        // TODO this can just be an ok_or_else
        if !self.entries.contains_key(key) {
            let key_str = format!("key not found: {}", String::from_utf8(key.to_vec())?);
            return Err(string_error::new_err(key_str.as_str()));
        }
        let entry = self.entries.get(key).cloned().unwrap();
        Ok(entry)
    }

    // TODO this result is never made
    pub fn remove(&mut self, key: &[u8]) -> ErrorResult<()> {
        self.entries.remove(&key.to_vec());
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        self.entries.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.entries.keys()
    }

    pub fn keys_range(
        &self,
        min: &[u8],
        max: &[u8],
    ) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        use std::ops::Bound::Included;

        self.entries
            .range::<[u8], _>((Included(min), Included(max)))
    }

    pub fn keys_range_min(&self, min: &[u8]) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        use std::ops::Bound::Included;
        use std::ops::Bound::Unbounded;

        self.entries.range::<[u8], _>((Included(min), Unbounded))
    }

    pub fn keys_range_max(&self, max: &[u8]) -> impl Iterator<Item = (&Vec<u8>, &KeyDirEntry)> {
        use std::ops::Bound::Included;
        use std::ops::Bound::Unbounded;

        self.entries.range::<[u8], _>((Unbounded, Included(max)))
    }
}
