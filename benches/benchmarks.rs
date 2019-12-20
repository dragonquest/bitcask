#![feature(test)]

extern crate test;
use test::Bencher;

extern crate bitcask;

use bitcask::tests::common;
use bytesize::ByteSize;

/* Benchmarks for writes */
#[bench]
fn writes_2_k(b: &mut Bencher) {
    bench_write(b, "db.bench1".to_string(), 2048);
}

#[bench]
fn writes_4_k(b: &mut Bencher) {
    bench_write(b, "db.bench2".to_string(), 4096);
}

#[bench]
fn writes_8_k(b: &mut Bencher) {
    bench_write(b, "db.bench3".to_string(), 8192);
}

#[bench]
fn writes_16_k(b: &mut Bencher) {
    bench_write(b, "db.bench4".to_string(), 16384);
}

#[bench]
fn writes_32_k(b: &mut Bencher) {
    bench_write(b, "db.bench5".to_string(), 32768);
}

/* Benchmarks for reads */
#[bench]
fn reads_2_k(b: &mut Bencher) {
    bench_read(b, "db.bench1".to_string(), 2048);
}

#[bench]
fn reads_4_k(b: &mut Bencher) {
    bench_read(b, "db.bench2".to_string(), 4096);
}

#[bench]
fn reads_8_k(b: &mut Bencher) {
    bench_read(b, "db.bench3".to_string(), 8192);
}

#[bench]
fn reads_16_k(b: &mut Bencher) {
    bench_read(b, "db.bench4".to_string(), 16384);
}

#[bench]
fn reads_32_k(b: &mut Bencher) {
    bench_read(b, "db.bench5".to_string(), 32768);
}

fn bench_write(b: &mut Bencher, db_name: String, bytesize: usize) {
    let mut db = common::DatabaseTesting::new(db_name, ByteSize::mb(50).as_u64());

    let key = b"name".to_vec();
    let val = vec![b'?'; bytesize];

    let mut n = 0;
    b.iter(|| {
        db.write(&key, &val).unwrap();
        n += 1;
        n
    })
}

fn bench_read(b: &mut Bencher, db_name: String, bytesize: usize) {
    let mut db = common::DatabaseTesting::new(db_name, ByteSize::mb(50).as_u64());

    let key = b"name".to_vec();
    let val = vec![b'?'; bytesize];

    db.write(&key, &val).unwrap();

    let mut n = 0;
    b.iter(|| {
        n += 1;
        let _ = db.read_cache(&key).unwrap();
        // let _ = db.read(&key).unwrap();
        n
    })
}
