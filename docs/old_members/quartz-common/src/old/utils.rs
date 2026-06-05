
const HIGHEST_BIT: u64 = 1 << 63;

/// Maps a `i64` to `u64`
///
/// For simplicity, tantivy internally handles `i64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `i64` to `u64` so that
/// `-2^63 .. 2^63-1` is mapped
///     to
/// `0 .. 2^64-1`
/// in that order.
///
/// This is more suited than simply casting (`val as u64`)
/// because of bitpacking.
///
/// Imagine a list of `i64` ranging from -10 to 10.
/// When casting negative values, the negative values are projected
/// to values over 2^63, and all values end up requiring 64 bits.
///
/// # See also
/// The reverse mapping is [`u64_to_i64()`].
#[inline]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT
}

/// Reverse the mapping given by [`i64_to_u64()`].
#[inline]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT) as i64
}

/// Maps a `f64` to `u64`
///
/// For simplicity, tantivy internally handles `f64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `f64` to `u64` in a monotonic manner, so that bytes lexical order is preserved.
///
/// This is more suited than simply casting (`val as u64`)
/// which would truncate the result
///
/// # Reference
///
/// Daniel Lemire's [blog post](https://lemire.me/blog/2020/12/14/converting-floating-point-numbers-to-integers-while-preserving-order/)
/// explains the mapping in a clear manner.
///
/// # See also
/// The reverse mapping is [`u64_to_f64()`].
#[inline]
pub fn f64_to_u64(val: f64) -> u64 {
    let bits = val.to_bits();
    if val.is_sign_positive() {
        bits ^ HIGHEST_BIT
    } else {
        !bits
    }
}

/// Reverse the mapping given by [`f64_to_u64()`].
#[inline]
pub fn u64_to_f64(val: u64) -> f64 {
    f64::from_bits(if val & HIGHEST_BIT != 0 {
        val ^ HIGHEST_BIT
    } else {
        !val
    })
}
