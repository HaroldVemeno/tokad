// Max supported is 16 bytes / 128 bits
pub const ID_BYTES: usize = 4;
pub const ID_BITS: usize = ID_BYTES*8;
pub const ID_MASK: u128 = u128::MAX >> (128 - ID_BITS);

pub fn key_hash(value: &[u8]) -> u128 {
    let hash = blake3::hash(value);
    let value = u128::from_be_bytes(hash.as_slice().first_chunk().unwrap().clone());
    value & ID_MASK
}
