use bincode::{Decode, Encode, config::Configuration};
use std::io::{Read, Write};

use crate::IndexResult;

const CONFIG: Configuration = bincode::config::standard();

pub fn deserialize<T: Decode<()>>(slice: &[u8]) -> IndexResult<(T, usize)> {
    let decode_response: (T, usize) = bincode::decode_from_slice(slice, CONFIG)?;
    Ok(decode_response)
}

pub fn encode_into_writer<T: Encode, W: Write>(value: &T, writer: &mut W) -> IndexResult<usize> {
    let num_encoded_bytes = bincode::encode_into_std_write(value, writer, CONFIG)?;
    Ok(num_encoded_bytes)
}

pub fn decode_from_reader<T: Decode<()>, R: Read>(reader: &mut R) -> IndexResult<T> {
    let decoded_value = bincode::decode_from_std_read(reader, CONFIG)?;
    Ok(decoded_value)
}
