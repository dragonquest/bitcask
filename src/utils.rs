pub fn time() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn extract_id_from_filename(
    entry: &std::path::PathBuf,
) -> Result<u128, Box<dyn std::error::Error>> {
    entry
        .extension()
        .ok_or_else(|| string_error::new_err("Missing extension (ie. not in format: data.<id>)"))?
        .to_str()
        .unwrap()
        .parse()
        .map_err(Into::into) // cast the FromStr error to dyn std::error::Error
}
