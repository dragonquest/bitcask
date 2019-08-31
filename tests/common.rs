extern crate bitcask;

use std::path::PathBuf;

pub struct DatabaseTesting {
    db: bitcask::Database,
    base_dir: std::path::PathBuf,

    cleanup_on_drop: bool,
}

impl DatabaseTesting {
    pub fn new(db_name: String, max_datafile_size_bytes: u64) -> DatabaseTesting {
        std::env::set_var("RUST_TEST_THREADS", "1");
        // std::env::set_var("RUST_LOG", "bitcask");

        let opts = bitcask::Options {
            base_dir: std::path::PathBuf::from(format!("./data/{}", db_name)),
            data_file_limit: max_datafile_size_bytes,
        };

        let _ = std::fs::remove_dir_all(&opts.base_dir);

        let base_dir = opts.base_dir.to_owned();

        let db = bitcask::new(opts).unwrap();

        DatabaseTesting {
            db: db,
            base_dir: base_dir,
            cleanup_on_drop: true,
        }
    }

    pub fn disable_cleanup(&mut self) {
        self.cleanup_on_drop = false;
    }

    pub fn open(db_name: String, max_datafile_size_bytes: u64) -> DatabaseTesting {
        std::env::set_var("RUST_TEST_THREADS", "1");
        // std::env::set_var("RUST_LOG", "bitcask");

        let opts = bitcask::Options {
            base_dir: std::path::PathBuf::from(format!("./data/{}", db_name)),
            data_file_limit: max_datafile_size_bytes,
        };

        let base_dir = opts.base_dir.to_owned();

        let db = bitcask::new(opts).unwrap();

        DatabaseTesting {
            db: db,
            base_dir: base_dir,
            cleanup_on_drop: true,
        }
    }

    /// all = mutable/active + immutable data files:
    pub fn count_all_data_files(&self) -> usize {
        self.glob_files("data.*").iter().count()
    }

    /// after compaction/merge, a index should be written:
    pub fn count_all_index_files(&self) -> usize {
        self.glob_files("index.*").iter().count()
    }

    pub fn size_all_data_files(&self) -> usize {
        let mut bytes: u64 = 0;

        let entries = self.glob_files("data.*");

        for entry in entries.iter() {
            bytes += std::fs::metadata(entry).unwrap().len();
        }

        (bytes as usize)
    }

    fn glob_files(&self, glob_pattern: &'static str) -> Vec<PathBuf> {
        use glob::glob;

        let glob_path = self.base_dir.join(glob_pattern);
        let glob_result = glob(glob_path.to_str().unwrap()).unwrap();

        let mut entries: Vec<PathBuf> = glob_result.map(|x| x.unwrap()).collect();

        entries.sort_by(|a, b| natord::compare(&a.to_str().unwrap(), &b.to_str().unwrap()));
        return entries;
    }
}

impl std::ops::Deref for DatabaseTesting {
    type Target = bitcask::Database;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl std::ops::DerefMut for DatabaseTesting {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

impl Drop for DatabaseTesting {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = std::fs::remove_dir_all(self.base_dir.to_string_lossy().to_string());
        }
    }
}
