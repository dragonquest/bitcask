extern crate bitcask;

mod common;
use bytesize::ByteSize;

#[test]
fn writing_a_key_should_return_same_value() {
    let mut db = common::DatabaseTesting::new("db1".to_owned(), ByteSize::b(1).as_u64());

    db.write("name".as_bytes(), "Peter".as_bytes()).unwrap();

    let data = db.read("name".as_bytes()).unwrap();
    assert_eq!("Peter".as_bytes().to_vec(), data);

    let stats = db.stats();

    assert_eq!(0, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(1, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(1, count_all_data_files, "Number of mutable + immutable data files");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(0, count_all_indices_files, "Number of indices files");
}

#[test]
fn updating_a_key_should_return_new_value() {
    let mut db = common::DatabaseTesting::new("db2".to_owned(), ByteSize::b(1).as_u64());

    db.write("name".as_bytes(), "Peter".as_bytes()).unwrap();
    db.write("name".as_bytes(), "Susi".as_bytes()).unwrap();

    let data = db.read("name".as_bytes()).unwrap();
    assert_eq!("Susi".as_bytes().to_vec(), data);

    let stats = db.stats();

    // Writing the second name 'Susi' will cause a new datafile to be written
    assert_eq!(1, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(1, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(2, count_all_data_files, "Number of mutable + immutable data files");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(0, count_all_indices_files, "Number of indices files");
}

#[test]
fn compaction_should_delete_duplicate_values() {
    let mut db = common::DatabaseTesting::new("db3".to_owned(), ByteSize::b(1).as_u64());

    db.write("name".as_bytes(), "Peter".as_bytes()).unwrap();
    let before_size_data_files = db.size_all_data_files();

    db.write("name".as_bytes(), "Susi".as_bytes()).unwrap();
    db.write("name".as_bytes(), "Robert".as_bytes()).unwrap();
    db.write("name".as_bytes(), "Peter".as_bytes()).unwrap(); // <-- same as initial value

    db.merge().unwrap();
    let after_size_data_files = db.size_all_data_files();

    let data = db.read("name".as_bytes()).unwrap();
    assert_eq!("Peter".as_bytes().to_vec(), data);

    let stats = db.stats();

    // Writing the second name 'Susi' will cause a new datafile to be written
    assert_eq!(1, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(1, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(1, count_all_data_files, "Number of mutable + immutable data files");

    assert_eq!(before_size_data_files, after_size_data_files, "Writing 1 entry should be the size after compaction");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(1, count_all_indices_files, "Number of indices files");
}


/*
#[test]
fn blub() {
    common::cleanup();

    {
        let mut db = common::setup("db1", ByteSize::b(1).as_u64());

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();

        assert_eq!(8, db.num_datafiles());
        assert_eq!(1, db.num_keys());

        db.merge().unwrap();
        assert_eq!(2, db.num_datafiles(), "after wrapping");
        assert_eq!(1, db.num_keys());

        db.merge().unwrap();
        assert_eq!(2, db.num_datafiles(), "after wrapping");
        assert_eq!(1, db.num_keys());

        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("a".as_bytes(), "v".as_bytes()).unwrap();
        db.write("b".as_bytes(), "v".as_bytes()).unwrap();
        assert_eq!(2, db.num_keys());

        db.remove("a".as_bytes()).unwrap();
        assert_eq!(1, db.num_keys());
        db.merge().unwrap();

        assert_eq!(1, db.num_keys());
    }

    let db = common::setup("db1", ByteSize::b(1).as_u64());
    assert_eq!(1, db.num_keys());
    assert_eq!(2, db.num_datafiles(), "after wrapping");

    // db.merge().unwrap();
}
*/
