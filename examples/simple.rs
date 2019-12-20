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

    db.keys()
        .cloned()
        .filter(|key| String::from_utf8_lossy(&key).ends_with("99"))
        .for_each(|key| {
            println!("key: {}", String::from_utf8_lossy(&key));
        });

    /*
    db.write("name".as_bytes(), "peter update".as_bytes()) .unwrap_or_default();
    db.write("age".as_bytes(), "20 update".as_bytes()) .unwrap_or_default();
    db.write("anything".as_bytes(), "value update".as_bytes()) .unwrap_or_default();
    db.remove("anything".as_bytes()) .unwrap_or_default();

    let mut rng = rand::thread_rng();
    let rand_value = format!("value {} update", rng.gen_range(0, 999999999));
    println!("Rand value written: {}", rand_value);
    db.write("random".as_bytes(), rand_value.as_bytes()).unwrap_or_default();

    let something = db.read("random".as_bytes()).unwrap();
    println!("read value: {}", String::from_utf8_lossy(something.as_slice()));
    */

    db.close().unwrap();

    std::process::exit(0);
}
