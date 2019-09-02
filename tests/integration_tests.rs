extern crate bitcask;

use bitcask::tests::common;

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

    db.write(b"name", b"Peter").unwrap();
    let before_size_data_files = db.size_all_data_files();

    db.write(b"name", b"Susi").unwrap();
    db.write(b"name", b"Robert").unwrap();
    db.write(b"name", b"Peter").unwrap(); // <-- same as initial value

    db.merge().unwrap();
    let after_size_data_files = db.size_all_data_files();

    let data = db.read(b"name").unwrap();
    assert_eq!(b"Peter".to_vec(), data);

    let stats = db.stats();

    // Writing the second name 'Susi' will cause a new datafile to be written
    assert_eq!(1, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(1, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(1, count_all_data_files, "Number of mutable + immutable data files");

    assert_eq!(before_size_data_files, after_size_data_files, "Writing 1 entry should be the size after compaction");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(1, count_all_indices_files, "Number of indices files");

    let mut df = db.get_datafile_at(0);
    assert_eq!("00000000 | S | name | Peter", df.inspect(false));
}

#[test]
fn compacting_multiple_times_should_delete_duplicate_values() {
    let mut db = common::DatabaseTesting::new("db4".to_owned(), ByteSize::b(1).as_u64());

    db.write(b"name", b"Peter").unwrap();
    let before_size_data_files = db.size_all_data_files();
    let mut after_size_data_files = before_size_data_files;

    for _ in 0..2 {
        db.write(b"name", b"Susi").unwrap();
        db.write(b"name", b"Robert").unwrap();
        db.write(b"name", b"Peter 2").unwrap();
        db.write(b"name", b"Peter").unwrap(); // <-- same as initial value

        db.merge().unwrap();

        after_size_data_files = db.size_all_data_files();

        let data = db.read(b"name").unwrap();
        assert_eq!(b"Peter".to_vec(), data);
    }

    let stats = db.stats();

    // Writing the second name 'Susi' will cause a new datafile to be written
    assert_eq!(0, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(1, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(1, count_all_data_files, "Number of mutable + immutable data files");

    assert_eq!(before_size_data_files, after_size_data_files, "Writing 1 entry should be the size after compaction");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(1, count_all_indices_files, "Number of indices files");
}

#[test]
fn compacting_multiple_times_should_old_delete_duplicate_values_multiple_values() {
    let mut db = common::DatabaseTesting::new("db5".to_owned(), ByteSize::b(1).as_u64());

    db.write(b"name", b"Peter").unwrap();
    let before_size_data_files = db.size_all_data_files();
    let mut after_size_data_files = before_size_data_files;

    for n in 0..10 {
        db.write(format!("name.{}", n).as_bytes(), format!("Susi {}", n).as_bytes()).unwrap();
        db.write(format!("name.{}", n + 1000).as_bytes(), format!("Susi {}", n).as_bytes()).unwrap();

        db.merge().unwrap();

        after_size_data_files = db.size_all_data_files();
    }

    let stats = db.stats();

    // Writing the second name 'Susi' will cause a new datafile to be written
    assert_eq!(1, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(21, stats.num_keys, "Number of keys"); // name + name.0-20 + name.(0-20)+1000

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(2, count_all_data_files, "Number of mutable + immutable data files");

    assert_eq!(true, (before_size_data_files < after_size_data_files), "the new compacted data file should not be null");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(1, count_all_indices_files, "Number of indices files");


    // Reading the files back again should work
    for n in 0..10 {
        let data = db.read(format!("name.{}", n).as_bytes()).unwrap();
        assert_eq!(format!("Susi {}", n).as_bytes().to_vec(), data);

        let data = db.read(format!("name.{}", (n + 1000)).as_bytes()).unwrap();
        assert_eq!(format!("Susi {}", n).as_bytes().to_vec(), data);
    }

    // The current data file (which is the only mutable datafile) should have
    // exactly 1 entry and the reason is that after the first write it will
    // create a new datafile and switch the writer to the new datafile:
    let mut db0 = db.get_current_datafile();
    assert_eq!("00000000 | S | name.1009 | Susi 9", db0.inspect(false));
    // println!(">>> {}", db0.inspect(true));

    let expected = r#"
00000000 | S | name | Peter
00000041 | S | name.0 | Susi 0
00000085 | S | name.1 | Susi 1
00000129 | S | name.1000 | Susi 0
00000176 | S | name.1001 | Susi 1
00000223 | S | name.1002 | Susi 2
00000270 | S | name.1003 | Susi 3
00000317 | S | name.1004 | Susi 4
00000364 | S | name.1005 | Susi 5
00000411 | S | name.1006 | Susi 6
00000458 | S | name.1007 | Susi 7
00000505 | S | name.1008 | Susi 8
00000552 | S | name.2 | Susi 2
00000596 | S | name.3 | Susi 3
00000640 | S | name.4 | Susi 4
00000684 | S | name.5 | Susi 5
00000728 | S | name.6 | Susi 6
00000772 | S | name.7 | Susi 7
00000816 | S | name.8 | Susi 8
00000860 | S | name.9 | Susi 9"#;

    let mut db1 = db.get_datafile_at(0);
    assert_eq!(expected.trim(), db1.inspect(false));
    // println!(">>> {}", db1.inspect(true));
}

#[test]
fn append_only_log_should_also_write_deletes() {
    let mut db = common::DatabaseTesting::new("db6".to_owned(), ByteSize::b(1).as_u64());

    db.write(b"name", b"Peter").unwrap();
    let before_size_data_files = db.size_all_data_files();

    db.remove(b"name").unwrap();

    db.disable_cleanup();
    drop(db);

    // reopen:
    db = common::DatabaseTesting::open("db6".to_owned(), ByteSize::b(1).as_u64());

    let after_size_data_files = db.size_all_data_files();

    let stats = db.stats();

    assert_eq!(1, stats.num_immutable_datafiles, "Number of immutable data files");
    assert_eq!(0, stats.num_keys, "Number of keys");

    let count_all_data_files = db.count_all_data_files();
    assert_eq!(2, count_all_data_files, "Number of mutable + immutable data files");

    assert_eq!(true, (before_size_data_files < after_size_data_files), "after deleting, the file gets bigger (due to append only system)");

    let count_all_indices_files = db.count_all_index_files();
    assert_eq!(0, count_all_indices_files, "Number of indices files");

    let expected = r#"
00000000 | S | name | Peter
00000041 | D | name | %_%_%_%<!(R|E|M|O|V|E|D)!>%_%_%_%_
"#;

    let mut db0 = db.get_datafile_at(0);
    assert_eq!(expected.trim(), db0.inspect(false));
    // println!(">>> {}", db0.inspect(true));

    // lets trigger a compaction:
    db.write(b"name", b"Peter").unwrap();
    db.remove(b"name").unwrap();
    db.write(b"name", b"Peter").unwrap();
    db.remove(b"name").unwrap();

    // After compaction:
    db.merge().unwrap(); 

    // current entry still has the 'REMOVED' tombstone,
    // because we didn't rewrite that yet (it's still "active"):
    let mut db1 = db.get_current_datafile();
    assert_eq!("00000000 | D | name | %_%_%_%<!(R|E|M|O|V|E|D)!>%_%_%_%_", db1.inspect(false));
    // println!(">>> {}", db1.inspect(true));


    // lets trigger only writes now. Checking if the db works correctly after some removals:
    db.write(b"name1", b"Peter").unwrap();
    db.write(b"name2", b"Peter").unwrap();
    db.write(b"name3", b"Peter").unwrap();
    db.write(b"name4", b"Peter").unwrap();
    db.write(b"name5", b"Peter").unwrap();

    // After compaction:
    db.merge().unwrap(); 

    db.write(b"test", b"123").unwrap();

    let expected = r#"
00000000 | S | name1 | Peter
00000042 | S | name2 | Peter
00000084 | S | name3 | Peter
00000126 | S | name4 | Peter
00000168 | S | name5 | Peter
    "#;

    let mut db2 = db.get_datafile_at(0);
    assert_eq!(expected.trim(), db2.inspect(false));
    // println!(">>> {}", db2.inspect(true));
}