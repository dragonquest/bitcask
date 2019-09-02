#![feature(test)]

extern crate test;
use test::Bencher;

extern crate bitcask;

use bitcask::tests::common;

use bytesize::ByteSize;

#[bench]
fn sequential_writes(b: &mut Bencher) {
    let mut db = common::DatabaseTesting::new("db.bench1".to_owned(), ByteSize::mb(50).as_u64());

    let val = vec![b'?'; 1024 * 1024];

    let mut n = 0;
    b.iter (|| {
        db.write(b"name", val.as_slice()).unwrap();
        n += 1;
        n
    })
}