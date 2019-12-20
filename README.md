# Bitcask implementation written in rust

Bitcast is a fast key-value store. The rust implementation has been written based on the docs provided by 
https://github.com/basho/bitcask/.

# Basic example

```rust
use bitcask;
use bytesize::ByteSize;

fn main() {
    let options = bitcask::Options {
        base_dir: std::path::PathBuf::from("./db1"),
        data_file_limit: ByteSize::mb(10).as_u64(),
    };

    let db = bitcask::new(options);
    if let Err(err_msg) = db {
        println!("{}", err_msg);
        std::process::exit(1);
    }
    let mut db = db.unwrap();

    for n in 0..1000 {
        let name = format!("Peter Nr. {}", n);
        let key = format!("name:{}", n);

        db.write(key.as_bytes(), name.as_bytes()).unwrap();
    }

    db.close().unwrap();
}
```
Or also check out: 'examples/simple.rs'.

# Bitcask API

| Function                                                      | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| ```new(options: Options) -> ErrorResult<Database>```                | Open a new or an existing bitcask file                 |
| ```write(&mut self, key: &[u8], value: &[u8]) -> ErrorResult<()>``` | Stores a key and a value in the datastore              |
| ```read(&mut self, key: &[u8]) -> ErrorResult<Vec<u8>>```           | Reads a value by key from a datastore                  |
| ```read_cache(&mut self, key: &[u8]) -> ErrorResult<Vec<u8>>```     | Reads a value by key from a datastore (incl. caching)  |
| ```remove(&mut self, key: &[u8]) -> ErrorResult<()>```              | Removes a key from the datastore                       |
| ```close(&mut self) -> ErrorResult<()>```                           | Close a bitcask data store and flushes all pending writes to disk |
| ```keys(&self) -> std::collections::btree_map::Keys<Vec<u8>, KeyDirEntry>``` | Returns iterator for all keys  |
| ```keys_range(&self, min: &[u8], max: &[u8]) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry>``` | Returns keys within a range (min, max) |
| ```keys_range_min(&self, min: &[u8]) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry>``` | Returns keys within a range (from a min key to open ended) |
| ```keys_range_max(&self, max: &[u8]) -> std::collections::btree_map::Range<Vec<u8>, KeyDirEntry>``` | Returns keys within a range (from start to a max key) |
| ```stats(&self) -> Stats```                                         | Returning stats such as num keys & number of datafiles |
| ```merge(&mut self) -> ErrorResult<()>```                           | Call to reclaim some disk space                        |

# Warning
Since this was a rust learning project and I am no expert regarding database design etc. 
please **DO NOT USE THIS IN PRODUCTION**

If you have any improvements, please create a Pull Request.