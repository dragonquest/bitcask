extern crate env_logger;
use log::*;

use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use glob::glob;
use rayon::prelude::*;
use string_error::new_err;

use crate::datafile::DataFile;
use crate::datafile::DataFileMetadata;
use crate::error::*;
use crate::indexfile::IndexFile;
use crate::keydir::KeyDir;
use crate::ErrorResult;

#[derive(Clone, Debug)]
pub struct Options {
    pub base_dir: std::path::PathBuf,
    pub data_file_limit: u64,
}

pub struct Database {
    options: Options,

    keydir: KeyDir,

    // active file:
    current_data_file: DataFile,

    // datafiles
    data_files: Vec<DataFileMetadata>,

    // once the active DataFile has reached the threshold
    // defined in data_file_limit, it will open a new data_file:
    data_file_limit: u64,
}

pub fn new(options: Options) -> ErrorResult<Database> {
    // best effort:
    let _ = env_logger::try_init();

    create_dir_all(&options.base_dir).map_err(|source| Error::CreateDatabaseDir {
        path: options.base_dir.to_path_buf(),
        source,
    })?;

    let created_dir = create_dir_all(&options.base_dir.as_path());
    if let Err(err_msg) = created_dir {
        return Err(new_err(
            &format!(
                "Failed to create '{}': {}",
                &options.base_dir.display(),
                err_msg
            )
            .to_string(),
        ));
    }

    let path = std::path::Path::new(&options.base_dir);

    let filename = crate::config::data_file_format(crate::utils::time());
    let data_file = DataFile::create(&path.join(filename), false)?;

    let mut db = Database {
        options: options.clone(),
        keydir: KeyDir::new(),
        current_data_file: data_file,
        data_files: Vec::new(),
        data_file_limit: options.data_file_limit,
    };

    db.startup(&path)?;

    Ok(db)
}

pub struct Stats {
    pub num_immutable_datafiles: u64,
    pub num_keys: u64,
}

impl Database {
    pub fn stats(&self) -> Stats {
        trace!("Stats called number of data files: {:?}", self.data_files);

        Stats {
            num_immutable_datafiles: (self.data_files.len() as u64),
            num_keys: (self.keydir.iter().unwrap().count() as u64),
        }
    }

    // Startup Jobs:
    pub fn startup(&mut self, base_dir: &Path) -> ErrorResult<()> {
        let mut data_files_sorted = self.get_data_files_except_current(&base_dir)?;

        self.build_keydir(&mut data_files_sorted)
            .map_err(|source| Error::KeyDirFill {
                path: base_dir.to_path_buf(),
                source: source,
            })?;

        self.cleanup()?;
        //self.merge()?;

        Ok(())
    }

    /// call merge to reclaim some disk space
    pub fn merge(&mut self) -> ErrorResult<()> {
        let base_dir = &self.options.base_dir;

        let data_files: Vec<PathBuf> = self
            .get_data_files_except_current(&base_dir)?
            .iter()
            .rev()
            .cloned()
            .collect();
        trace!(
            "merge: found Data Files before merge operation: {:?}",
            data_files
        );

        if data_files.len() < 2 {
            // Nothing to merge, it does not make sense
            return Ok(());
        }

        // first removing all the startup indices:
        let indices_paths = self.glob_files(&base_dir, "index.*")?;
        for index_path in indices_paths {
            let _ = std::fs::remove_file(index_path.as_path());
        }

        let now = crate::utils::time();
        let merged_path = &base_dir.join(format!("merge.{}", now));
        let mut temp_datastore = DataFile::create(&merged_path.as_path(), false)?;

        let index_path = &self.options.base_dir.join(format!("index.{}", now));
        let mut index = IndexFile::create(&index_path, false)?;

        let keydir = &self.keydir;
        for (key, entry) in keydir.iter()? {
            let value = self.read(&key)?;

            let new_offset = temp_datastore.write(key, &value, entry.timestamp)?;

            index.write(key, now, new_offset, entry.timestamp)?;
        }
        drop(temp_datastore);
        drop(index);

        let new_datafile_path = &base_dir.join(crate::config::data_file_format(now));
        trace!(
            "trying to rename data file '{}' to '{}'",
            &merged_path.display(),
            &new_datafile_path.display()
        );
        std::fs::rename(&merged_path, new_datafile_path)?;

        // glob all data files except for the ones we have merged. We cannot delete them yet because the keydir is not rebuilt yet:
        let mut new_data_files: Vec<PathBuf> = self
            .glob_files(&base_dir, crate::config::DATA_FILE_GLOB_FORMAT)?
            .iter()
            .cloned()
            .filter(|item| !data_files.contains(&item))
            .collect();


        self.build_keydir(&mut new_data_files)?;

        for path in data_files {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn get_data_files_except_current(&self, base_dir: &Path) -> ErrorResult<Vec<PathBuf>> {
        let mut entries = self.glob_files(&base_dir, crate::config::DATA_FILE_GLOB_FORMAT)?;

        entries.sort_by(|a, b| natord::compare(&a.to_str().unwrap(), &b.to_str().unwrap()));

        // Remove current data file since the current data file is mutable:
        entries.retain(|x| {
            x.file_name().unwrap().to_str().unwrap()
                != self
                    .current_data_file
                    .path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
        });

        return Ok(entries);
    }

    fn glob_files(&self, base_dir: &Path, pattern: &'static str) -> ErrorResult<Vec<PathBuf>> {
        let glob_path = base_dir.join(pattern);
        let glob_result = glob(glob_path.to_str().unwrap());
        if let Err(err_msg) = glob_result {
            return Err(Box::new(err_msg));
        }

        let mut entries: Vec<PathBuf> = glob_result?.map(|x| x.unwrap()).collect();

        entries.sort_by(|a, b| natord::compare(&a.to_str().unwrap(), &b.to_str().unwrap()));
        return Ok(entries);
    }

    fn build_keydir(&mut self, datafiles_paths: &mut Vec<PathBuf>) -> ErrorResult<()> {

        trace!("rebuilding keydir now based on the files: {:?}", datafiles_paths);

        let mut new_keydir = KeyDir::new();
        let mut new_datafiles: Vec<DataFileMetadata> = Vec::new();
        let base_dir = self.options.base_dir.to_owned();

        let keydir = Arc::new(Mutex::new(&mut new_keydir));
        let data_files = Arc::new(Mutex::new(&mut new_datafiles));

        datafiles_paths.par_iter_mut().for_each({
            let keydir = Arc::clone(&keydir);

            move |entry| {
                let mut counter = 0;

                let file_id = crate::utils::extract_id_from_filename(&entry).unwrap();

                let index_path = base_dir.join(format!("index.{}", file_id));
                trace!("Database.build_keydir: check if index exist '{}'", index_path.display());

                if index_path.exists() {
                    trace!("Database.build_keydir: index found 'index.{}'. Importing data file No={} Path={} ...", file_id, file_id, &entry.display());

                    let mut index = IndexFile::create(&index_path.to_path_buf(), true).unwrap();

                    for (_, entry) in index.iter() {
                        let mut keydir = keydir.lock().unwrap();
                        let set_result = keydir.set(&entry.key, entry.file_id, entry.offset, entry.timestamp);
                        if set_result.is_err() {
                            trace!("Setting value into keydir has failed: {}", set_result.err().unwrap());
                        }

                        counter += 1;
                    }

                    trace!("Database.build_keydir: index 'index.{}' fully read. Imported index file No={} Path={} NumRecords={}", file_id, file_id, &entry.display(), counter);
                    drop(index);

                } else {
                    let mut df = DataFile::create(&entry, true).unwrap();

                    for (offset, record) in df.iter() {
                        let mut keydir = keydir.lock().unwrap();

                        if record.value == crate::config::REMOVE_TOMBSTONE {
                            keydir.remove(&record.key).unwrap_or_default();
                            continue;
                        }

                        let current_entry = keydir.get(&record.key).unwrap();
                        if record.timestamp > current_entry.timestamp {
                            keydir.set(record.key.as_slice(), file_id, offset, record.timestamp).unwrap();
                        }

                        counter += 1;
                    }

                    trace!("Database.build_keydir: loading datafile No={} Path={} NumRecords={}", file_id, &entry.display(), counter);
                }


                let mut data_files = data_files.lock().unwrap();
                &data_files.push(DataFileMetadata {
                    id: file_id,
                    path: entry.to_path_buf(),
                });
            }
        });

        // Removing the current file as the current one is not an immutable data file yet:
        let new_datafiles = new_datafiles.into_iter().filter(|df| df.id != self.current_data_file.id).collect();

        trace!("Assigning new data files to internal struct: {:?} => {:?}", &self.data_files, &new_datafiles);
        std::mem::replace(&mut self.data_files, new_datafiles);
        std::mem::replace(&mut self.keydir, new_keydir);

        self.cleanup()?;

        Ok(())
    }

    fn cleanup(&mut self) -> ErrorResult<()> {
        let entries =
            self.glob_files(&self.options.base_dir, crate::config::DATA_FILE_GLOB_FORMAT)?;

        for entry in entries {
            let file_id = crate::utils::extract_id_from_filename(&entry)?;

            // cleaning up old files with 0 bytes size:
            if self.current_data_file.get_id() != file_id {
                let info = std::fs::metadata(&entry).unwrap();
                if info.len() == 0 {
                    let remove = std::fs::remove_file(Path::new(&entry));
                    if remove.is_ok() {
                        trace!("... removing {} since it is zero bytes and its not the current data file id (this: {}, current: {})", &entry.to_str().unwrap(), file_id, &self.current_data_file.get_id());
                    }
                }
            }
        }

        Ok(())
    }

    fn switch_to_new_data_file(&mut self) -> ErrorResult<()> {
        let data_file_id = crate::utils::time();

        let new_path = std::path::Path::new(&self.options.base_dir)
            .join(crate::config::data_file_format(data_file_id));

        trace!(
            "Database.switch_to_new_data_file: New data file is {} (file_id={})",
            &new_path.display(),
            data_file_id
        );

        let new_data_file = DataFile::create(new_path.as_path(), false)?;
        let old_data_file = std::mem::replace(&mut self.current_data_file, new_data_file);

        let data_file_id = self.current_data_file.get_id();
        trace!(
            "Database.switch_to_new_data_file: Switched data file. Old_Id={} New_Id={}",
            &old_data_file.get_id(),
            data_file_id
        );

        self.data_files.push(DataFileMetadata {
            id: old_data_file.id,
            path: std::path::Path::new(&self.options.base_dir)
                .join(crate::config::data_file_format(old_data_file.id)),
        });

        Ok(())
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> ErrorResult<()> {
        let data_file_id = self.current_data_file.get_id();

        let timestamp = crate::utils::time();

        let offset = self.current_data_file.write(key, value, timestamp)?;
        self.keydir.set(&key, data_file_id, offset, timestamp)?;

        if offset >= self.data_file_limit {
            trace!("Database.write: Offset threshold reached for data file id '{}': {} < {}. Switching to new data file", data_file_id, offset, self.data_file_limit);
            return self.switch_to_new_data_file();
        }

        Ok(())
    }

    pub fn read(&self, key: &[u8]) -> ErrorResult<Vec<u8>> {
        let entry = self.keydir.get(key)?;

        let data_filename = crate::config::data_file_format(entry.file_id);
        let path = std::path::Path::new(&self.options.base_dir).join(data_filename);

        let mut data_file = DataFile::create(&path, true)?;
        trace!("Database.read: Trying to read from offset {} from file {}", entry.offset, &path.display());
        let found_entry = data_file.read(entry.offset)?;

        Ok(found_entry.value)
    }

    pub fn remove(&mut self, key: &[u8]) -> ErrorResult<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();

        self.current_data_file.remove(key, timestamp)?;
        self.keydir.remove(key)
    }

    pub fn close(&mut self) -> ErrorResult<()> {
        Ok(())
    }
}
