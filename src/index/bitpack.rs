#![allow(dead_code)]

#[inline]
pub fn required_bits_u32(input: &[u32]) -> usize {
    let mut max = 0u32;
    for &v in input {
        max |= v;
    }

    if max == 0 {
        1
    } else {
        32 - max.leading_zeros() as usize
    }
}

pub fn pack_u32_dynamic(input: &[u32], out: &mut [u32]) -> (usize, usize) {
    let w = required_bits_u32(input);
    let words = match w {
        1 => pack_u32_scalar::<1>(input, out),
        2 => pack_u32_scalar::<2>(input, out),
        3 => pack_u32_scalar::<3>(input, out),
        4 => pack_u32_scalar::<4>(input, out),
        5 => pack_u32_scalar::<5>(input, out),
        6 => pack_u32_scalar::<6>(input, out),
        7 => pack_u32_scalar::<7>(input, out),
        8 => pack_u32_scalar::<8>(input, out),
        9 => pack_u32_scalar::<9>(input, out),
        10 => pack_u32_scalar::<10>(input, out),
        11 => pack_u32_scalar::<11>(input, out),
        12 => pack_u32_scalar::<12>(input, out),
        13 => pack_u32_scalar::<13>(input, out),
        14 => pack_u32_scalar::<14>(input, out),
        15 => pack_u32_scalar::<15>(input, out),
        16 => pack_u32_scalar::<16>(input, out),
        17 => pack_u32_scalar::<17>(input, out),
        18 => pack_u32_scalar::<18>(input, out),
        19 => pack_u32_scalar::<19>(input, out),
        20 => pack_u32_scalar::<20>(input, out),
        21 => pack_u32_scalar::<21>(input, out),
        22 => pack_u32_scalar::<22>(input, out),
        23 => pack_u32_scalar::<23>(input, out),
        24 => pack_u32_scalar::<24>(input, out),
        25 => pack_u32_scalar::<25>(input, out),
        26 => pack_u32_scalar::<26>(input, out),
        27 => pack_u32_scalar::<27>(input, out),
        28 => pack_u32_scalar::<28>(input, out),
        29 => pack_u32_scalar::<29>(input, out),
        30 => pack_u32_scalar::<30>(input, out),
        31 => pack_u32_scalar::<31>(input, out),
        32 => pack_u32_scalar::<32>(input, out),
        _ => unreachable!(),
    };

    (w, words)
}

pub fn unpack_u32_dynamic(packed: &[u32], n: usize, w: usize, out: &mut [u32]) {
    match w {
        1 => unpack_u32_scalar::<1>(packed, n, out),
        2 => unpack_u32_scalar::<2>(packed, n, out),
        3 => unpack_u32_scalar::<3>(packed, n, out),
        4 => unpack_u32_scalar::<4>(packed, n, out),
        5 => unpack_u32_scalar::<5>(packed, n, out),
        6 => unpack_u32_scalar::<6>(packed, n, out),
        7 => unpack_u32_scalar::<7>(packed, n, out),
        8 => unpack_u32_scalar::<8>(packed, n, out),
        9 => unpack_u32_scalar::<9>(packed, n, out),
        10 => unpack_u32_scalar::<10>(packed, n, out),
        11 => unpack_u32_scalar::<11>(packed, n, out),
        12 => unpack_u32_scalar::<12>(packed, n, out),
        13 => unpack_u32_scalar::<13>(packed, n, out),
        14 => unpack_u32_scalar::<14>(packed, n, out),
        15 => unpack_u32_scalar::<15>(packed, n, out),
        16 => unpack_u32_scalar::<16>(packed, n, out),
        17 => unpack_u32_scalar::<17>(packed, n, out),
        18 => unpack_u32_scalar::<18>(packed, n, out),
        19 => unpack_u32_scalar::<19>(packed, n, out),
        20 => unpack_u32_scalar::<20>(packed, n, out),
        21 => unpack_u32_scalar::<21>(packed, n, out),
        22 => unpack_u32_scalar::<22>(packed, n, out),
        23 => unpack_u32_scalar::<23>(packed, n, out),
        24 => unpack_u32_scalar::<24>(packed, n, out),
        25 => unpack_u32_scalar::<25>(packed, n, out),
        26 => unpack_u32_scalar::<26>(packed, n, out),
        27 => unpack_u32_scalar::<27>(packed, n, out),
        28 => unpack_u32_scalar::<28>(packed, n, out),
        29 => unpack_u32_scalar::<29>(packed, n, out),
        30 => unpack_u32_scalar::<30>(packed, n, out),
        31 => unpack_u32_scalar::<31>(packed, n, out),
        32 => unpack_u32_scalar::<32>(packed, n, out),
        _ => panic!("invalid bit width"),
    }
}

/*
    Scalar bit-packing / unpacking for u32 values.

    Design goals:
    - Very fast scalar implementation
    - Supports arbitrary N
    - Constant-bit-width per block
    - Suitable as a tail path next to SIMD / FastLanes

    Public API:
        pack_u32_scalar::<W>(input, out) -> words_written
        unpack_u32_scalar::<W>(input, n, out)
*/

/// Packs `input.len()` u32 values into a linear bitstream.
/// Each value uses exactly `W` bits.
///
/// Output is written as u32 words into `out`.
/// Returns the number of u32 words written.
///
/// SAFETY / CONTRACT:
/// - `W` must be in 1..=32
/// - All input values must fit in `W` bits
/// - `out` must be large enough: ceil(input.len() * W / 32)
pub fn pack_u32_scalar<const W: usize>(input: &[u32], out: &mut [u32]) -> usize {
    assert!(W > 0 && W <= 32);

    let mask: u64 = if W == 32 { u64::MAX } else { (1u64 << W) - 1 };

    let mut bitbuf: u64 = 0;
    let mut bits: usize = 0;
    let mut out_idx: usize = 0;

    for &v in input {
        bitbuf |= ((v as u64) & mask) << bits;
        bits += W;

        if bits >= 32 {
            out[out_idx] = bitbuf as u32;
            out_idx += 1;

            bitbuf >>= 32;
            bits -= 32;
        }
    }

    if bits > 0 {
        out[out_idx] = bitbuf as u32;
        out_idx += 1;
    }

    out_idx
}

/// Unpacks `n` u32 values from a linear bitstream.
/// Each value uses exactly `W` bits.
///
/// SAFETY / CONTRACT:
/// - `W` must be in 1..=32
/// - `input` must contain enough words
/// - `out.len() >= n`
pub fn unpack_u32_scalar<const W: usize>(input: &[u32], n: usize, out: &mut [u32]) {
    assert!(W > 0 && W <= 32);
    assert!(out.len() >= n);

    let mask: u64 = if W == 32 { u64::MAX } else { (1u64 << W) - 1 };

    let mut bitbuf: u64 = 0;
    let mut bits: usize = 0;
    let mut in_idx: usize = 0;

    for out_item in out.iter_mut().take(n) {
        while bits < W {
            bitbuf |= (input[in_idx] as u64) << bits;
            bits += 32;
            in_idx += 1;
        }

        *out_item = (bitbuf & mask) as u32;
        bitbuf >>= W;
        bits -= W;
    }
}

/* ===========================================================
   u64 Bitpacking
=========================================================== */

/// Calculates the number of bits required to store the largest value in the input.
#[inline]
pub fn required_bits_u64(input: &[u64]) -> usize {
    let mut max = 0u64;
    for &v in input {
        max |= v;
    }

    if max == 0 {
        1
    } else {
        64 - max.leading_zeros() as usize
    }
}

/// A macro to generate the 64-arm match statement for dynamic dispatch.
macro_rules! dispatch_w {
    ($w:expr, $func:ident, $($args:expr),*) => {
        match $w {
             1 => $func::< 1>($($args),*),  2 => $func::< 2>($($args),*),
             3 => $func::< 3>($($args),*),  4 => $func::< 4>($($args),*),
             5 => $func::< 5>($($args),*),  6 => $func::< 6>($($args),*),
             7 => $func::< 7>($($args),*),  8 => $func::< 8>($($args),*),
             9 => $func::< 9>($($args),*), 10 => $func::<10>($($args),*),
            11 => $func::<11>($($args),*), 12 => $func::<12>($($args),*),
            13 => $func::<13>($($args),*), 14 => $func::<14>($($args),*),
            15 => $func::<15>($($args),*), 16 => $func::<16>($($args),*),
            17 => $func::<17>($($args),*), 18 => $func::<18>($($args),*),
            19 => $func::<19>($($args),*), 20 => $func::<20>($($args),*),
            21 => $func::<21>($($args),*), 22 => $func::<22>($($args),*),
            23 => $func::<23>($($args),*), 24 => $func::<24>($($args),*),
            25 => $func::<25>($($args),*), 26 => $func::<26>($($args),*),
            27 => $func::<27>($($args),*), 28 => $func::<28>($($args),*),
            29 => $func::<29>($($args),*), 30 => $func::<30>($($args),*),
            31 => $func::<31>($($args),*), 32 => $func::<32>($($args),*),
            33 => $func::<33>($($args),*), 34 => $func::<34>($($args),*),
            35 => $func::<35>($($args),*), 36 => $func::<36>($($args),*),
            37 => $func::<37>($($args),*), 38 => $func::<38>($($args),*),
            39 => $func::<39>($($args),*), 40 => $func::<40>($($args),*),
            41 => $func::<41>($($args),*), 42 => $func::<42>($($args),*),
            43 => $func::<43>($($args),*), 44 => $func::<44>($($args),*),
            45 => $func::<45>($($args),*), 46 => $func::<46>($($args),*),
            47 => $func::<47>($($args),*), 48 => $func::<48>($($args),*),
            49 => $func::<49>($($args),*), 50 => $func::<50>($($args),*),
            51 => $func::<51>($($args),*), 52 => $func::<52>($($args),*),
            53 => $func::<53>($($args),*), 54 => $func::<54>($($args),*),
            55 => $func::<55>($($args),*), 56 => $func::<56>($($args),*),
            57 => $func::<57>($($args),*), 58 => $func::<58>($($args),*),
            59 => $func::<59>($($args),*), 60 => $func::<60>($($args),*),
            61 => $func::<61>($($args),*), 62 => $func::<62>($($args),*),
            63 => $func::<63>($($args),*), 64 => $func::<64>($($args),*),
            _ => unreachable!(),
        }
    };
}

pub fn pack_u64_dynamic(input: &[u64], out: &mut [u64]) -> (usize, usize) {
    let w = required_bits_u64(input);
    let words = dispatch_w!(w, pack_u64_scalar, input, out);
    (w, words)
}

pub fn unpack_u64_dynamic(packed: &[u64], n: usize, w: usize, out: &mut [u64]) {
    dispatch_w!(w, unpack_u64_scalar, packed, n, out);
}

/// Packs `input.len()` u64 values into a linear bitstream.
/// Each value uses exactly `W` bits.
pub fn pack_u64_scalar<const W: usize>(input: &[u64], out: &mut [u64]) -> usize {
    assert!(W > 0 && W <= 64);

    // Mask for the input values
    let mask: u128 = if W == 64 { u64::MAX as u128 } else { (1u128 << W) - 1 };

    let mut bitbuf: u128 = 0;
    let mut bits: usize = 0;
    let mut out_idx: usize = 0;

    for &v in input {
        // Accumulate bits into u128 buffer
        bitbuf |= ((v as u128) & mask) << bits;
        bits += W;

        // Flush full 64-bit words
        if bits >= 64 {
            out[out_idx] = bitbuf as u64;
            out_idx += 1;

            bitbuf >>= 64;
            bits -= 64;
        }
    }

    // Flush remaining bits
    if bits > 0 {
        out[out_idx] = bitbuf as u64;
        out_idx += 1;
    }

    out_idx
}

/// Unpacks `n` u64 values from a linear bitstream.
pub fn unpack_u64_scalar<const W: usize>(input: &[u64], n: usize, out: &mut [u64]) {
    assert!(W > 0 && W <= 64);
    assert!(out.len() >= n);

    let mask: u128 = if W == 64 { u64::MAX as u128 } else { (1u128 << W) - 1 };

    let mut bitbuf: u128 = 0;
    let mut bits: usize = 0;
    let mut in_idx: usize = 0;

    for out_item in out.iter_mut().take(n) {
        // Refill buffer until we have at least W bits
        while bits < W {
            bitbuf |= (input[in_idx] as u128) << bits;
            bits += 64;
            in_idx += 1;
        }

        // Extract value
        *out_item = (bitbuf & mask) as u64;

        // Consume bits
        bitbuf >>= W;
        bits -= W;
    }
}

/* -----------------------------------------------------------
   Helper utilities (used by tests)
----------------------------------------------------------- */

fn packed_len(n: usize, w: usize) -> usize {
    if n == 0 {
        0
    } else {
        (n * w).div_ceil(32)
    }
}

fn packed_len_u64(n: usize, w: usize) -> usize {
    if n == 0 {
        0
    } else {
        (n * w).div_ceil(64)
    }
}

/* -----------------------------------------------------------
   Tests
----------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_small_all_widths() {
        for n in 0..=64 {
            for pattern in 0..4 {
                let input: Vec<u32> = (0..n)
                    .map(|i| match pattern {
                        0 => i as u32,
                        1 => (i * 3) as u32,
                        2 => (i * 17) as u32,
                        _ => (i * 7919) as u32,
                    })
                    .collect();

                let mut packed = vec![0u32; packed_len(n, 32)];

                let (w, words) = pack_u32_dynamic(&input, &mut packed);

                let mut output = vec![0u32; n];
                unpack_u32_dynamic(&packed[..words], n, w, &mut output);

                assert_eq!(input, output, "n={n}, pattern={pattern}, w={w}");
            }
        }
    }

    #[test]
    fn boundary_values() {
        for &value in &[0u32, u32::MAX] {
            for n in [0, 1, 2, 7, 32, 100] {
                let input = vec![value; n];
                let mut packed = vec![0u32; packed_len(n, 32)];

                let (w, words) = pack_u32_dynamic(&input, &mut packed);

                let mut output = vec![0u32; n];
                unpack_u32_dynamic(&packed[..words], n, w, &mut output);

                assert_eq!(input, output, "value={value}, n={n}, w={w}");
            }
        }
    }

    #[test]
    fn word_boundary_cases() {
        for n in [1, 2, 3, 7, 31, 32, 33, 63, 64, 65, 127, 128] {
            let input: Vec<u32> = (0..n).map(|i| ((i * 7919) ^ (i << 5)) as u32).collect();

            let mut packed = vec![0u32; packed_len(n, 32)];
            let (w, words) = pack_u32_dynamic(&input, &mut packed);

            let mut output = vec![0u32; n];
            unpack_u32_dynamic(&packed[..words], n, w, &mut output);

            assert_eq!(input, output, "n={n}, w={w}");
        }
    }

    #[test]
    fn randomized_fuzz() {
        let mut seed: u64 = 0x1234_5678_9ABC_DEF0;

        fn next_u32(seed: &mut u64) -> u32 {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed as u32
        }

        for _ in 0..500 {
            let n = (next_u32(&mut seed) % 500) as usize;

            let mut input = vec![0u32; n];
            for v in &mut input {
                *v = next_u32(&mut seed);
            }

            let mut packed = vec![0u32; packed_len(n, 32)];
            let (w, words) = pack_u32_dynamic(&input, &mut packed);

            let mut output = vec![0u32; n];
            unpack_u32_dynamic(&packed[..words], n, w, &mut output);

            assert_eq!(input, output, "fuzz n={n}, w={w}");
        }
    }

    #[test]
    fn packed_size_is_correct() {
        for n in 0..=1000 {
            let input: Vec<u32> = (0..n).map(|i| (i * 13) as u32).collect();

            let mut packed = vec![0u32; packed_len(n, 32)];
            let (w, words) = pack_u32_dynamic(&input, &mut packed);

            let expected = packed_len(n, w);
            assert_eq!(words, expected, "n={n}, w={w}");
        }
    }

    #[test]
    fn roundtrip_u64_all_widths() {
        for n in 0..=64 {
            for pattern in 0..3 {
                let input: Vec<u64> = (0..n)
                    .map(|i| match pattern {
                        0 => i as u64,
                        1 => (i * 3) as u64,
                        _ => (i * 7919) as u64,
                    })
                    .collect();

                let mut packed = vec![0u64; packed_len_u64(n, 64).max(1)];
                let (w, words) = pack_u64_dynamic(&input, &mut packed);
                packed.truncate(words);

                let mut output = vec![0u64; n];
                unpack_u64_dynamic(&packed, n, w, &mut output);

                assert_eq!(input, output, "n={n}, pattern={pattern}, w={w}");
            }
        }
    }

    #[test]
    fn u64_boundary_values() {
        for &value in &[0u64, u64::MAX] {
            for n in [0, 1, 2, 7, 32, 100] {
                let input = vec![value; n];
                let mut packed = vec![0u64; packed_len_u64(n, 64).max(1)];

                let (w, words) = pack_u64_dynamic(&input, &mut packed);
                packed.truncate(words);

                let mut output = vec![0u64; n];
                unpack_u64_dynamic(&packed, n, w, &mut output);

                assert_eq!(input, output, "value={value}, n={n}, w={w}");
            }
        }
    }

    #[test]
    fn u64_randomized_fuzz() {
        let mut seed: u64 = 0xFEED_FACE_CAFE_BEEF;

        fn next_u64(seed: &mut u64) -> u64 {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed
        }

        for _ in 0..300 {
            let n = (next_u64(&mut seed) % 500) as usize;
            let mut input = vec![0u64; n];
            for v in &mut input {
                *v = next_u64(&mut seed);
            }

            let mut packed = vec![0u64; packed_len_u64(n, 64).max(1)];
            let (w, words) = pack_u64_dynamic(&input, &mut packed);
            packed.truncate(words);

            let mut output = vec![0u64; n];
            unpack_u64_dynamic(&packed, n, w, &mut output);

            assert_eq!(input, output, "fuzz n={n}, w={w}");
        }
    }
}
