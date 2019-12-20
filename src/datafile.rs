use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;

use serde::{Deserialize, Serialize};

use crate::config::REMOVE_TOMBSTONE;
use crate::*;

#[derive(Clone, Debug)]
pub struct DataFileMetadata {
    pub id: u128,
    pub path: std::path::PathBuf,
}

/// CleanFile is a wrapper for File which deletes the file on close
/// if the file's size is 0:
#[derive(Debug)]
struct CleanFile {
    file: Option<std::fs::File>,
    path: std::path::PathBuf,
}

impl std::ops::Deref for CleanFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for CleanFile {
    fn deref_mut(&mut self) -> &mut std::fs::File {
        self.file.as_mut().unwrap()
    }
}

impl Drop for CleanFile {
    fn drop(&mut self) {
        self.file.take();

        let path = &self.path.as_path();
        let file_metadata = std::fs::metadata(path);
        if let Ok(metadata) = file_metadata {
            if metadata.len() == 0 {
                log::trace!("Datafile.drop: removing file since its empty {}", &path.display());
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[derive(Debug)]
pub struct DataFile {
    pub id: u128,
    pub is_readonly: bool,

    file: CleanFile,
    pub path: std::path::PathBuf,
}

impl DataFile {
    pub fn create(path: &std::path::Path, is_readonly: bool) -> ErrorResult<DataFile> {
        let datafile: std::fs::File;

        if is_readonly {
            datafile = OpenOptions::new().read(true).open(&path)?;
        } else {
            datafile = OpenOptions::new()
                .read(true)
                .append(true)
                .write(true)
                .create(true)
                .open(&path)?;
        }

        let id = crate::utils::extract_id_from_filename(&path.to_path_buf())?;

        let df = DataFile {
            id: id,
            file: CleanFile {
                file: Some(datafile),
                path: path.to_path_buf(),
            },
            is_readonly: is_readonly,
            path: path.to_path_buf(),
        };

        Ok(df)
    }

    pub fn get_id(&self) -> u128 {
        (self.id as u128)
    }

    pub fn write(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> ErrorResult<u64> {
        use std::io::Write;

        let entry = Entry {
            timestamp: timestamp,
            key: key.to_vec(),
            value: value.to_vec(),
        };

        let offset = self.file.seek(SeekFrom::Current(0))?;

        let encoded: Vec<u8> = bincode::serialize(&entry)?;

        let written = self.file.write(&encoded);
        if let Err(err_msg) = written {
            return Err(Box::new(err_msg));
        }

        Ok(offset)
    }

    pub fn remove(&mut self, key: &[u8], timestamp: u128) -> ErrorResult<u64> {
        self.write(key, REMOVE_TOMBSTONE, timestamp)
    }

    pub fn read(&mut self, offset: u64) -> ErrorResult<Entry> {
        let mmap = unsafe { memmap::MmapOptions::new().map(&self.file)? };
        let decoded: Entry = bincode::deserialize(&mmap[(offset as usize)..])?;

        Ok(decoded)
    }

    pub fn iter(&mut self) -> DataFileIterator {
        let file = std::fs::File::open(&self.path).unwrap();

        DataFileIterator { file: file }
    }

    pub fn sync(&mut self) -> ErrorResult<()> {
        let res = self.file.sync_all();
        if res.is_ok() {
            return Ok(());
        }

        Err(Box::new(res.err().unwrap()))
    }

    pub fn inspect(&mut self, with_header: bool) -> String {
        let mut list = String::new();

        if with_header {
            list.push_str(format!("Datafile {}:\n", self.id).as_str());
        }

        for (offset, entry) in self.iter() {
            let mut op = "S"; // Set

            if entry.value == crate::config::REMOVE_TOMBSTONE {
                op = "D" // Delete
            }

            let line = format!("{:0>8} | {: >1} | {} | {}\n", offset, op, String::from_utf8(entry.key.to_owned()).unwrap(), String::from_utf8(entry.value.to_owned()).unwrap());
            list.push_str(line.to_owned().as_str());
        }

        list.trim_end().to_string()
    }
}

pub struct DataFileIterator {
    file: std::fs::File,
}

impl Iterator for DataFileIterator {
    type Item = (u64, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.file.seek(SeekFrom::Current(0)).unwrap();

        let decoded_maybe = bincode::deserialize_from(&self.file);
        if let Err(_) = decoded_maybe {
            return None;
        }

        Some((offset, decoded_maybe.unwrap()))
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        self.file.sync_all().unwrap_or_default();
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Entry {
    // TODO: crc: impl later
    pub timestamp: u128,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
