pub fn buffer_to_hex(buffer: Vec<u8>) -> String {
    let mut hex = String::with_capacity(buffer.len() * 2);
    for byte in buffer {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}
