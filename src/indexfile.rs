use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::*;

#[derive(Debug)]
pub struct IndexFile {
    pub id: u128,
    pub is_readonly: bool,

    file: std::fs::File,
    pub path: std::path::PathBuf,
}

impl IndexFile {
    pub fn create(path: &std::path::Path, is_readonly: bool) -> ErrorResult<IndexFile> {
        let indexfile: std::fs::File;

        if is_readonly {
            indexfile = OpenOptions::new().read(true).open(&path)?;
        } else {
            indexfile = OpenOptions::new()
                .read(true)
                .append(true)
                .write(true)
                .create(true)
                .open(&path)?;
        }

        let id = crate::utils::extract_id_from_filename(&path.to_path_buf())?;

        let idxfile = IndexFile {
            id: id,
            file: indexfile,
            is_readonly: is_readonly,
            path: path.to_path_buf(),
        };

        Ok(idxfile)
    }

    // pub fn get_id(&self) -> u128 {
    //     (self.id as u128)
    // }

    pub fn write(
        &mut self,
        key: &[u8],
        file_id: u128,
        offset: u64,
        timestamp: u128,
    ) -> ErrorResult<u64> {
        let entry = IndexEntry {
            key: key.to_vec(),
            file_id: file_id,
            offset: offset,
            timestamp: timestamp,
        };

        let offset = self.file.seek(SeekFrom::Current(0))?;

        let encoded: Vec<u8> = bincode::serialize(&entry)?;

        let written = self.file.write(&encoded);
        if let Err(err_msg) = written {
            return Err(Box::new(err_msg));
        }

        Ok(offset)
    }

    #[allow(dead_code)]
    pub fn read(&mut self, offset: u64) -> ErrorResult<IndexEntry> {
        self.file.seek(SeekFrom::Start(offset))?;

        let decoded: IndexEntry = bincode::deserialize_from(&self.file)?;

        Ok(decoded)
    }

    pub fn iter(&mut self) -> IndexFileIterator {
        let file = std::fs::File::open(&self.path).unwrap();

        IndexFileIterator { file: file }
    }
}

pub struct IndexFileIterator {
    file: std::fs::File,
}

impl Iterator for IndexFileIterator {
    type Item = (u64, IndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.file.seek(SeekFrom::Current(0)).unwrap();

        let decoded_maybe = bincode::deserialize_from(&self.file);
        if let Err(_) = decoded_maybe {
            return None;
        }

        Some((offset, decoded_maybe.unwrap()))
    }
}

impl Drop for IndexFile {
    fn drop(&mut self) {
        self.file.sync_all().unwrap_or_default();
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct IndexEntry {
    pub key: Vec<u8>,

    // data file id
    pub file_id: u128,
    pub offset: u64,
    pub timestamp: u128,
}
