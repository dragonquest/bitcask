#![feature(test)]

extern crate test;
use test::Bencher;

extern crate bitcask;

use bitcask::tests::common;

use bytesize::ByteSize;

fn bench_write(b: &mut Bencher, db_name: String, bytesize: usize) {
    let mut db = common::DatabaseTesting::new(db_name, ByteSize::mb(50).as_u64());

    let key = b"name".to_vec();
    let val = vec![b'?'; bytesize];

    let mut n = 0;
    b.iter (|| {
        db.write(&key, &val).unwrap();
        n += 1;
        n
    })
}

#[bench]
fn sequential_writes_2_k(b: &mut Bencher) {
    bench_write(b, "db.bench1".to_string(), 2048);
}

#[bench]
fn sequential_writes_4_k(b: &mut Bencher) {
    bench_write(b, "db.bench2".to_string(), 4096);
}

#[bench]
fn sequential_writes_8_k(b: &mut Bencher) {
    bench_write(b, "db.bench3".to_string(), 8192);
}

#[bench]
fn sequential_writes_16_k(b: &mut Bencher) {
    bench_write(b, "db.bench4".to_string(), 16384);
}

#[bench]
fn sequential_writes_32_k(b: &mut Bencher) {
    bench_write(b, "db.bench5".to_string(), 32768);
}
