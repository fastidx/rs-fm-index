use crate::index::external_sa;
use cdivsufsort::sort as div_sort;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

fn assert_sa_matches(text: &[u8], mem_limit: usize) {
    let sa_stream = external_sa::build_sa_external(text, mem_limit).unwrap();
    let sa_ext: Vec<u64> = sa_stream
        .iter()
        .unwrap()
        .map(|v| v.unwrap())
        .collect();

    let (_, sa_ref) = div_sort(text).into_parts();
    let sa_ref: Vec<u64> = sa_ref.iter().map(|&v| v as u64).collect();

    assert_eq!(sa_ext, sa_ref);
}

fn random_text_with_sentinel(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(len + 1);
    for _ in 0..len {
        let b = rng.random_range(1u8..=255u8);
        data.push(b);
    }
    data.push(0);
    data
}

#[test]
fn test_external_sa_matches_cdivsufsort_small_cases() {
    let mem_limit = 128;

    let cases = [
        b"a\0".to_vec(),
        b"banana\0".to_vec(),
        b"mississippi\0".to_vec(),
        b"abracadabra\0".to_vec(),
    ];

    for case in cases {
        assert_sa_matches(&case, mem_limit);
    }
}

#[test]
fn test_external_sa_matches_random() {
    let mem_limit = 128;
    let lengths = [1usize, 2, 5, 16, 64, 127];

    for (i, &len) in lengths.iter().enumerate() {
        let text = random_text_with_sentinel(1234 + i as u64, len);
        assert_sa_matches(&text, mem_limit);
    }
}
