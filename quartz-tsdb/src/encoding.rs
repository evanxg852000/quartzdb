// https://www.timescale.com/blog/time-series-compression-algorithms-explained/

/// Encoding path: TODO: Check if *_to_64 maintains a good double delta
/// i64(value) -> i64_to_u64 -> double-delta -> yield i64 -> varint
/// f64(value) -> f64_to_u64 -> double-delta -> yield i64 -> varint
/// i64(timestamp) -> i64_to_u64 -> double-delta -> yield i64 -> varint
/// decoding is the reverse 

// 80 85 90 95 100
// 80 5  5  5  5
// 80 5  0  0  0  
// ---------------
// 80 85 

use integer_encoding::VarInt;

use crate::int_mapping::{i64_to_u64, u64_to_i64, f64_to_u64, u64_to_f64};


trait DataEncoder {
    type Input;
    type Output;

    fn initialize(&mut self, _initial: Self::Input) {}

    fn encode(&self, input: Self::Input) -> Self::Output;
    fn decode(&self, output: Self::Output) -> Self::Input;
}

// i64_to_u64/u64_to_i64
struct IntEncoder;

impl DataEncoder for IntEncoder {
    type Input = i64;
    type Output = u64;

    fn encode(&self, input: Self::Input) -> Self::Output {
        i64_to_u64(input)
    }

    fn decode(&self, output: Self::Output) -> Self::Input {
        u64_to_i64(output)
    }
}

// f64_to_u64/u64_to_f64
struct FloatEncoder;

impl DataEncoder for IntEncoder {
    type Input = f64;
    type Output = u64;

    fn encode(&self, input: Self::Input) -> Self::Output {
        f64_to_u64(input)
    }

    fn decode(&self, output: Self::Output) -> Self::Input {
        u64_to_f64(output)
    }
}


struct DeltaEncoder(u64); // initial value

//May not implemented now as i don't fully understand.
struct DoubleDeltaEncoder(u64, u64); // initial values







struct VarIntSerializer;

impl VarIntSerializer {

    fn serialize(self, value: i64, output: &mut [u8]) -> Result<usize, String> {
        Ok(value.encode_var(output))
    }

    fn deserialize(&self, input: &[u8]) -> Result<(i64, usize), String> {
        i64::decode_var(self.buffer)
            .ok_or_else(|| "error decoding".to_string())
    }
}

