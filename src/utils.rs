pub fn time() -> u128 {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ts
}

pub fn extract_id_from_filename(
    entry: &std::path::PathBuf,
) -> Result<u128, Box<dyn std::error::Error>> {
    let id = entry
        .extension()
        .ok_or(string_error::new_err(
            "Missing extension (ie. not in format: data.<id>)",
        ))?
        .to_str()
        .unwrap()
        .parse::<u128>()?;
    Ok(id)
}
