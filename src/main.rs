//! Shopping Sessions pipeline. Rules: rules.md. Flow:
//! links -> union-find; events parsed into eid-partitions -> dedup/validate ->
//! person-partitions; clicks parsed straight into person-partitions; then each
//! person-partition is sorted, sessionized, attributed and aggregated.
//! IDs are identified by a 128-bit bijection of their bytes (exact up to 15 bytes; xxh3-128 beyond);
//! the one byte-order comparison the rules need (click_id ties, person names)
//! uses the original bytes.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rustc_hash::FxHashMap;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const L: usize = 512; // link sub-tables

// ---------- small utilities ----------

/// A &'static [u8] packed into 8 bytes: pointer in the low 48 bits, length in the high 16.
#[derive(Clone, Copy, Default)]
struct SRef(u64);
fn sref(s: &'static [u8]) -> SRef {
    // ponytail: IDs over 64 KiB abort; the generator writes ~10-byte IDs.
    assert!(s.len() < 1 << 16, "id too long");
    SRef(s.as_ptr() as u64 & ((1 << 48) - 1) | (s.len() as u64) << 48)
}
impl SRef {
    fn get(self) -> &'static [u8] {
        let len = (self.0 >> 48) as usize;
        if len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts((self.0 & ((1 << 48) - 1)) as *const u8, len) }
    }
}

/// 128-bit ID identity. IDs of up to 15 bytes (all the generator writes) are packed with their
/// length and put through a bijection, so distinct IDs never collide; longer IDs use xxh3-128
/// with the length byte set to 0xFF, keeping the two kinds disjoint. The bijection also mixes
/// the bits, so any slice of the result works as a partition or table index.
#[inline]
fn h128(s: &[u8]) -> (u64, u64) {
    let x = if s.len() <= 15 {
        let mut b = [0u8; 16];
        b[..s.len()].copy_from_slice(s);
        b[15] = s.len() as u8;
        u128::from_le_bytes(b)
    } else {
        xxhash_rust::xxh3::xxh3_128(s) | 0xFF << 120
    };
    let h = mix128(x);
    ((h >> 64) as u64, h as u64)
}

/// h128 of a field inside an input mapping, without the copy: every input file starts with a
/// header line of at least 16 bytes (checked in `chunks`), so the 16 bytes ending at a data
/// field's end are always mapped. Same result as h128.
#[inline]
fn h128_mapped(s: &[u8]) -> (u64, u64) {
    let n = s.len();
    if n == 0 || n > 15 {
        return h128(s);
    }
    let v = unsafe { std::ptr::read_unaligned(s.as_ptr().add(n).sub(16) as *const u128) };
    let h = mix128(u128::from_le(v) >> ((16 - n) * 8) | (n as u128) << 120);
    ((h >> 64) as u64, h as u64)
}

/// Invertible: xor-shifts right and multiplications by odd constants are bijections on u128.
const K1: u128 = 0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835;
const K2: u128 = 0xD6E8_FEB8_6659_FD93_A5A3_5625_3AD5_B9C7;
#[inline]
fn mix128(mut x: u128) -> u128 {
    x ^= x >> 67;
    x = x.wrapping_mul(K1);
    x ^= x >> 59;
    x = x.wrapping_mul(K2);
    x ^ x >> 64
}
/// Inverse of an odd number mod 2^128 (Newton's iteration doubles the correct bits each step).
const fn inv128(k: u128) -> u128 {
    let mut x = k;
    let mut i = 0;
    while i < 7 {
        x = x.wrapping_mul(2u128.wrapping_sub(k.wrapping_mul(x)));
        i += 1;
    }
    x
}
fn unmix128(mut x: u128) -> u128 {
    x ^= x >> 64;
    x = x.wrapping_mul(inv128(K2));
    x ^= x >> 59 ^ x >> 118;
    x = x.wrapping_mul(inv128(K1));
    x ^ x >> 67
}
/// The ID behind a person key: decoded when it is up to 15 bytes, else from `long`.
fn name_of(
    hi: u64,
    lo: u64,
    long: &FxHashMap<u128, SRef>,
    arena: &mut Vec<u8>,
) -> Result<(usize, usize), &'static [u8]> {
    let raw = unmix128(key(hi, lo));
    let len = (raw >> 120) as usize;
    if len <= 15 {
        let at = arena.len();
        arena.extend_from_slice(&raw.to_le_bytes()[..len]);
        Ok((at, len))
    } else {
        Err(long[&key(hi, lo)].get())
    }
}

/// Hasher for keys that are already random hashes.
#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = (self.0.rotate_left(8)) ^ x as u64;
        }
    }
    fn write_u128(&mut self, i: u128) {
        self.0 = i as u64;
    }
}
type IdMap<V> = HashMap<u128, V, BuildHasherDefault<IdHasher>>;
#[inline]
fn key(hi: u64, lo: u64) -> u128 {
    (hi as u128) << 64 | lo as u128
}

fn par<T: Send>(
    nt: usize,
    n: usize,
    init: impl Fn() -> T + Sync,
    work: impl Fn(&mut T, usize) + Sync,
) -> Vec<T> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt.min(n.max(1)))
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        work(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Concatenates `src` into `out` sorted by `key` (whose first part is a random hash): one
/// counting-sort pass on its top bits, then tiny sorts per bucket.
fn group<'a, T: Copy + Default + 'a>(
    src: impl Iterator<Item = &'a [T]> + Clone,
    out: &mut Vec<T>,
    hist: &mut Vec<u32>,
    key: impl Fn(&T) -> (u64, u64, i64),
) {
    let n: usize = src.clone().map(|s| s.len()).sum();
    let bits = (usize::BITS - n.max(2).leading_zeros()).clamp(4, 22);
    let shift = 64 - bits;
    hist.clear();
    hist.resize((1 << bits) + 1, 0);
    for s in src.clone() {
        for x in s {
            hist[(key(x).0 >> shift) as usize + 1] += 1;
        }
    }
    for i in 1..hist.len() {
        hist[i] += hist[i - 1];
    }
    out.clear();
    out.reserve(n);
    let ptr = out.as_mut_ptr();
    for s in src {
        for x in s {
            let b = (key(x).0 >> shift) as usize;
            unsafe { ptr.add(hist[b] as usize).write(*x) };
            hist[b] += 1;
        }
    }
    unsafe { out.set_len(n) }; // every slot written: the counts sum to n
    // hist[b] is now the end of bucket b
    let mut st = 0;
    for b in 0..hist.len() - 1 {
        let e = hist[b] as usize;
        if e - st > 1 {
            out[st..e].sort_unstable_by_key(&key);
        }
        st = e;
    }
}

/// Records counting-sorted into partitions, in one exact-size buffer.
struct Parted<T> {
    data: Vec<T>,
    off: Vec<u32>,
}
impl<T: Copy> Parted<T> {
    fn new(src: &[T], np: usize, hist: &mut Vec<u32>, part: impl Fn(&T) -> usize) -> Self {
        hist.clear();
        hist.resize(np + 1, 0);
        for x in src {
            hist[part(x) + 1] += 1;
        }
        for i in 1..=np {
            hist[i] += hist[i - 1];
        }
        let off = hist.clone();
        let mut data: Vec<T> = Vec::with_capacity(src.len());
        let ptr = data.as_mut_ptr();
        for x in src {
            let b = part(x);
            unsafe { ptr.add(hist[b] as usize).write(*x) };
            hist[b] += 1;
        }
        unsafe { data.set_len(src.len()) }; // every slot written: the counts sum to src.len()
        Parted { data, off }
    }
    #[inline]
    fn get(&self, p: usize) -> &[T] {
        &self.data[self.off[p] as usize..self.off[p + 1] as usize]
    }
}

/// Bitmasks of '\n', ',' and '"' in 64 bytes at p.
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let (nl, cm, qt) = (
        _mm_set1_epi8(b'\n' as i8),
        _mm_set1_epi8(b',' as i8),
        _mm_set1_epi8(b'"' as i8),
    );
    let (mut a, mut b, mut c) = (0u64, 0u64, 0u64);
    for k in 0..4 {
        let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
        a |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, nl)) as u16 as u64) << (16 * k);
        b |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, cm)) as u16 as u64) << (16 * k);
        c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, qt)) as u16 as u64) << (16 * k);
    }
    (a, b, c)
}

/// Calls f with the N fields of every well-formed line of chunk (the rules' dialect; malformed
/// lines are skipped). Walks only the structural bytes, found 64 at a time.
#[inline(always)]
fn for_records<const N: usize>(chunk: &'static [u8], mut f: impl FnMut([&'static [u8]; N])) {
    struct St<const N: usize> {
        fields: [&'static [u8]; N],
        nf: usize,
        fstart: usize,
        state: u8, // 0 field start/unquoted, 1 inside quotes, 2 after closing quote
        qclose: usize,
        bad: bool,
    }
    #[inline(always)]
    fn end_field<const N: usize>(st: &mut St<N>, chunk: &'static [u8], pos: usize) {
        if !st.bad {
            let r = match st.state {
                0 => Some((st.fstart, pos)),
                2 if st.qclose + 1 == pos => Some((st.fstart + 1, st.qclose)),
                _ => None,
            };
            match r {
                Some((a, b)) if st.nf < N => {
                    st.fields[st.nf] = unsafe { chunk.get_unchecked(a..b) };
                    st.nf += 1;
                }
                _ => st.bad = true,
            }
        }
        st.fstart = pos + 1;
        st.state = 0;
    }
    let n = chunk.len();
    let mut st = St::<N> {
        fields: [&[]; N],
        nf: 0,
        fstart: 0,
        state: 0,
        qclose: 0,
        bad: false,
    };
    let mut blk = 0;
    let mut tail = [0u8; 64];
    while blk < n {
        let (nl, cm, qt) = unsafe {
            if blk + 64 <= n {
                masks(chunk.as_ptr().add(blk))
            } else {
                tail[..n - blk].copy_from_slice(&chunk[blk..]);
                tail[n - blk..].fill(0);
                masks(tail.as_ptr())
            }
        };
        let mut bits = nl | cm | qt;
        while bits != 0 {
            let tz = bits.trailing_zeros();
            bits &= bits - 1;
            let pos = blk + tz as usize;
            let bit = 1u64 << tz;
            if qt & bit != 0 {
                if !st.bad {
                    match st.state {
                        0 if pos == st.fstart => st.state = 1,
                        1 => {
                            st.state = 2;
                            st.qclose = pos;
                        }
                        _ => st.bad = true,
                    }
                }
            } else {
                end_field(&mut st, chunk, pos);
                if nl & bit != 0 {
                    if !st.bad && st.nf == N {
                        f(st.fields);
                    }
                    st.nf = 0;
                    st.bad = false;
                }
            }
        }
        blk += 64;
    }
    if st.fstart < n || st.nf > 0 {
        // last line without a trailing '\n'
        end_field(&mut st, chunk, n);
        if !st.bad && st.nf == N {
            f(st.fields);
        }
    }
}

// ---------- CSV ----------

/// Splits one line (no '\n') into exactly N fields per the rules' dialect, or None if malformed.
#[inline]
fn split<const N: usize>(line: &'static [u8]) -> Option<[&'static [u8]; N]> {
    let n = line.len();
    if n == 0 {
        return None;
    }
    let mut out: [&'static [u8]; N] = [&[]; N];
    let mut f = 0;
    let mut i = 0;
    loop {
        if f == N {
            return None;
        }
        if i < n && line[i] == b'"' {
            let st = i + 1;
            let mut j = st;
            while j < n && line[j] != b'"' {
                if line[j] == b',' {
                    return None;
                }
                j += 1;
            }
            if j == n {
                return None;
            }
            out[f] = &line[st..j];
            f += 1;
            i = j + 1;
            if i == n {
                break;
            }
            if line[i] != b',' {
                return None;
            }
            i += 1;
        } else {
            let st = i;
            while i < n && line[i] != b',' {
                if line[i] == b'"' {
                    return None;
                }
                i += 1;
            }
            out[f] = &line[st..i];
            f += 1;
            if i == n {
                break;
            }
            i += 1;
        }
    }
    if f == N {
        Some(out)
    } else {
        None
    }
}

#[inline]
fn for_lines(chunk: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut s = 0;
    while s < chunk.len() {
        let e = memchr::memchr(b'\n', &chunk[s..]).map_or(chunk.len(), |i| s + i);
        f(&chunk[s..e]);
        s = e + 1;
    }
}

fn map_file(path: &Path) -> &'static [u8] {
    let f = std::fs::File::open(path).unwrap();
    if f.metadata().unwrap().len() == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f).unwrap() };
    Box::leak(Box::new(m))
}

/// Data lines (header dropped) of every *.csv in dir, cut on line boundaries into pieces of at
/// least 4 MiB and about 24 per thread: few enough that the later fan-in over chunks stays cheap.
fn chunks(dir: &Path, nt: usize) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    let files: Vec<&'static [u8]> = rd
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
        .map(|p| map_file(&p))
        .collect();
    let chunk = (files.iter().map(|d| d.len()).sum::<usize>() / (nt * 24)).max(4 << 20);
    for d in files {
        let Some(h) = memchr::memchr(b'\n', d) else {
            continue;
        };
        assert!(h >= 16, "header too short for h128_mapped"); // the rules fix every header at 19+ bytes
        let mut s = h + 1;
        while s < d.len() {
            let mut e = (s + chunk).min(d.len());
            if e < d.len() {
                e = memchr::memchr(b'\n', &d[e..]).map_or(d.len(), |i| e + i + 1);
            }
            out.push(&d[s..e]);
            s = e;
        }
    }
    out.sort_by_key(|c| std::cmp::Reverse(c.len()));
    out
}

fn lines_of(path: &Path) -> Vec<&'static [u8]> {
    let d = map_file(path);
    let mut v = Vec::new();
    for_lines(d, |l| v.push(l));
    if !v.is_empty() {
        v.remove(0);
    }
    v
}

// ---------- values ----------

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a * 10 + b) as i64)
    } else {
        None
    }
}

const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

// Calendar tables for years Y0..Y0+400 (others take the arithmetic path).
const Y0: i64 = 1900;
const fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}
const LEAP: [bool; 400] = {
    let mut t = [false; 400];
    let mut i = 0;
    while i < 400 {
        t[i] = is_leap(Y0 + i as i64);
        i += 1;
    }
    t
};
const YDAYS: [i64; 400] = {
    let mut t = [0; 400];
    let mut i = 0;
    while i < 400 {
        t[i] = days_from_civil(Y0 + i as i64, 1, 1);
        i += 1;
    }
    t
};
const DIM: [[i64; 12]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];
const MCUM: [[i64; 12]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];

#[inline]
fn parse_dt(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let (mo, d, h, mi, se) = (d2(s, 5)?, d2(s, 8)?, d2(s, 11)?, d2(s, 14)?, d2(s, 17)?);
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    if (Y0..Y0 + 400).contains(&y) {
        let yi = (y - Y0) as usize;
        let lp = LEAP[yi] as usize;
        if d > DIM[lp][mo as usize - 1] {
            return None;
        }
        let days = YDAYS[yi] + MCUM[lp][mo as usize - 1] + d - 1;
        return Some((days * 86400 + h * 3600 + mi * 60 + se) * 1000);
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ][mo as usize - 1];
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000)
}

/// F1 / F2 / F3 to epoch ms; None for a bad timestamp (an F3 beyond i64 is out of every window anyway).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    match s.len() {
        0 => None,
        20 if s[19] == b'Z' => parse_dt(s),
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let base = parse_dt(s)?;
            let (oh, om) = (d2(s, 20)?, d2(s, 23)?);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            Some(if s[19] == b'+' {
                base - off
            } else {
                base + off
            })
        }
        _ => {
            let mut v: u64 = 0;
            for &c in s {
                let d = c.wrapping_sub(b'0');
                if d > 9 {
                    return None;
                }
                v = v.checked_mul(10)?.checked_add(d as u64)?;
            }
            i64::try_from(v).ok()
        }
    }
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(10_000_001);
    }
    (1..=10_000_000).contains(&v).then_some(v as u32)
}

// ---------- records ----------

/// Times in E2/C2 are ms since the click window start (window_start - 7 days): 21 days fit u32.
#[derive(Clone, Copy)]
struct E1 {
    ehi: u64,
    elo: u64,
    uhi: u64,
    ulo: u64,
    ingest: i32, // seconds from window_start (ingest_ts is whole-second F1); only compared
    ts: u32,
    price: u32, // >0 only for a valid purchase
    cat: u16,
    valid: bool,
}
#[derive(Clone, Copy, Default)]
#[repr(C, packed(2))]
struct E2 {
    phi: u64,
    plo: u64,
    ts: u32,
    price: u32,
    cat: u16,
}
#[derive(Clone, Copy, Default)]
struct C2 {
    phi: u64,
    plo: u64,
    cref: SRef,
    ts: u32,
    camp: u32,
}
impl E2 {
    #[inline]
    fn t(&self) -> i64 {
        self.ts as i64
    }
}
impl C2 {
    #[inline]
    fn t(&self) -> i64 {
        self.ts as i64
    }
}
struct URow {
    phi: u64,
    plo: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u16, // u16::MAX = null
}

type Person = (u64, u64);
#[derive(Clone, Copy, Default)]
struct Slot {
    hi: u64,
    lo: u64, // hi = lo = 0 marks an empty slot: only the empty ID hashes to it, and those are dropped
    phi: u64,
    plo: u64,
}
/// Linked IDs -> person, as L flat linear-probing tables picked by `hi`, indexed by `lo`.
struct Links(Vec<(usize, Vec<Slot>)>, Vec<(u64, u64, SRef)>); // tables; person names over 15 bytes
impl Links {
    #[inline]
    fn prefetch(&self, hi: u64, lo: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let (mask, t) = &self.0[hi as usize & (L - 1)];
            _mm_prefetch::<_MM_HINT_T0>(t.as_ptr().add(lo as usize & mask) as *const i8);
        }
    }
    #[inline]
    fn person(&self, hi: u64, lo: u64) -> Person {
        let (mask, t) = &self.0[hi as usize & (L - 1)];
        let mut i = lo as usize & mask;
        loop {
            let s = &t[i];
            if s.hi == hi && s.lo == lo {
                return (s.phi, s.plo);
            }
            if s.hi == 0 && s.lo == 0 {
                return (hi, lo);
            }
            i = (i + 1) & mask;
        }
    }
}

fn build_links(nt: usize, dir: &Path) -> Links {
    let ch = chunks(dir, nt);
    let parts = par(
        nt,
        ch.len(),
        Vec::new,
        |v: &mut Vec<(SRef, u64, u64, SRef, u64, u64)>, i| {
            for_records(ch[i], |[a, b]: [&'static [u8]; 2]| {
                if !a.is_empty() && !b.is_empty() {
                    let (ah, al) = h128_mapped(a);
                    let (bh, bl) = h128_mapped(b);
                    v.push((sref(a), ah, al, sref(b), bh, bl));
                }
            })
        },
    );
    let mut maps: Vec<IdMap<u32>> = (0..L).map(|_| IdMap::default()).collect();
    let mut refs: Vec<SRef> = Vec::new();
    let mut hs: Vec<(u64, u64)> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    let mut intern = |r: SRef, h: u64, l: u64, parent: &mut Vec<u32>| -> u32 {
        *maps[h as usize & (L - 1)]
            .entry(key(h, l))
            .or_insert_with(|| {
                let i = refs.len() as u32;
                refs.push(r);
                hs.push((h, l));
                parent.push(i);
                i
            })
    };
    for v in &parts {
        for &(a, ah, al, b, bh, bl) in v {
            let ia = intern(a, ah, al, &mut parent);
            let ib = intern(b, bh, bl, &mut parent);
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = refs.len();
    let root: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = root[i] as usize;
        if refs[i].get() < refs[best[r] as usize].get() {
            best[r] = i as u32;
        }
    }
    let tables: Vec<(usize, Vec<Slot>)> = par(
        nt,
        L,
        Vec::new,
        |v: &mut Vec<(usize, (usize, Vec<Slot>))>, u| {
            let cap = (maps[u].len() * 2).next_power_of_two().max(2);
            let mask = cap - 1;
            let mut t = vec![Slot::default(); cap];
            for (&k, &i) in &maps[u] {
                let b = best[root[i as usize] as usize] as usize;
                let (hi, lo) = ((k >> 64) as u64, k as u64);
                let mut j = lo as usize & mask;
                while t[j].hi != 0 || t[j].lo != 0 {
                    j = (j + 1) & mask;
                }
                t[j] = Slot {
                    hi,
                    lo,
                    phi: hs[b].0,
                    plo: hs[b].1,
                };
            }
            v.push((u, (mask, t)));
        },
    )
    .into_iter()
    .flatten()
    .collect::<std::collections::BTreeMap<_, _>>()
    .into_values()
    .collect();
    let long = (0..n)
        .filter(|&i| root[i] as usize == i && refs[best[i] as usize].get().len() > 15)
        .map(|i| {
            let b = best[i] as usize;
            (hs[b].0, hs[b].1, refs[b])
        })
        .collect();
    Links(tables, long)
}

fn json_field(s: &str, name: &str) -> i64 {
    let at = s.find(&format!("\"{name}\"")).unwrap() + name.len() + 2;
    let rest = &s[at..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).expect("manifest timestamp")
}

fn main() {
    let t0 = std::time::Instant::now();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = std::path::PathBuf::from(env("DATA_DIR", "/data"));
    let out = std::path::PathBuf::from(env("OUTPUT_DIR", "/output"));
    let nt: usize = env("NCPU", "0")
        .parse()
        .ok()
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    let log = |m: &str| eprintln!("[{:7.3}s] {m}", t0.elapsed().as_secs_f64());

    let manifest = std::fs::read_to_string(data.join("manifest.json")).unwrap();
    let ws = json_field(&manifest, "window_start");
    let we = json_field(&manifest, "window_end");
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;

    // products -> category index, categories sorted byte-wise with "unknown" among them
    let prod_rows: Vec<(&[u8], &[u8])> = lines_of(&data.join("products.csv"))
        .into_iter()
        .filter_map(|l| split::<2>(l).map(|[a, b]| (a, b)))
        .collect();
    let mut cat_names: Vec<&[u8]> = prod_rows
        .iter()
        .map(|r| r.1)
        .chain([&b"unknown"[..]])
        .collect();
    cat_names.sort();
    cat_names.dedup();
    let cat_idx: FxHashMap<&[u8], u16> = cat_names
        .iter()
        .enumerate()
        .map(|(i, c)| (*c, i as u16))
        .collect();
    let unknown = cat_idx[&b"unknown"[..]];
    let prod: FxHashMap<&[u8], u16> = prod_rows.iter().map(|r| (r.0, cat_idx[r.1])).collect();

    let mut camp_names: Vec<&'static [u8]> = Vec::new();
    let mut channels: Vec<&'static [u8]> = Vec::new();
    for l in lines_of(&data.join("campaigns.csv")) {
        if let Some([a, b]) = split::<2>(l) {
            camp_names.push(a);
            channels.push(b);
        }
    }
    let camp: FxHashMap<&[u8], u32> = camp_names
        .iter()
        .enumerate()
        .map(|(i, c)| (*c, i as u32))
        .collect();
    let organic = camp_names.len() as u32;
    camp_names.push(b"organic");
    channels.push(b"organic");
    let ncamp = camp_names.len();

    // stage 1, concurrent with building links: each chunk's events become one exact-size buffer,
    // counting-sorted into np eid partitions; each chunk's valid clicks one exact-size buffer.
    // Exact sizes matter: every freshly touched page is zeroed by the kernel.
    // (click dedup is a no-op: copies are identical, and credit/assist results don't change with repeats)
    let ev_chunks = chunks(&data.join("events"), nt);
    let ck_chunks = chunks(&data.join("clicks"), nt);
    let ne = ev_chunks.len();
    // partitions (eid and person) scale with the input so each stays cache-sized
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.len()).sum();
    let np: usize = std::env::var("PQ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            (ev_bytes / 75 / 20_000)
                .next_power_of_two()
                .clamp(256, 4096)
        });
    let nq = np;
    let links_dir = data.join("links");
    let cws = ws - WEEK;
    let links_cell = std::sync::OnceLock::<Links>::new();
    let s1 = std::thread::scope(|sc| {
        sc.spawn(|| {
            let _ = links_cell.set(build_links(1.max(nt / 4), &links_dir));
        });
        par(
            nt,
            ne + ck_chunks.len(),
            || {
                (
                    Vec::<E1>::new(),
                    Vec::<C2>::new(),
                    Vec::<u32>::new(),
                    Vec::<Parted<E1>>::new(),
                    Vec::<Parted<C2>>::new(),
                    Vec::<(u64, u64, SRef)>::new(),
                )
            },
            |(tmp, tmpc, hist, evp, cpp, longs), i| {
                if i >= ne {
                    // click chunks come last; links are long done by then
                    let links = links_cell.wait();
                    tmpc.clear();
                    for_records(
                        ck_chunks[i - ne],
                        |[cid, uid, cp, ts]: [&'static [u8]; 4]| {
                            if cid.is_empty() || uid.is_empty() {
                                return;
                            }
                            let Some(t) = parse_ts(ts).filter(|&t| t >= cws && t < we) else {
                                return;
                            };
                            let Some(&c) = camp.get(cp) else { return };
                            let (h, lo) = h128_mapped(uid);
                            tmpc.push(C2 {
                                phi: h,
                                plo: lo,
                                ts: (t - cws) as u32,
                                cref: sref(cid),
                                camp: c,
                            });
                        },
                    );
                    // ponytail: no prefetch here; clicks are a fifth of the rows
                    for c in tmpc.iter_mut() {
                        let (phi, plo) = links.person(c.phi, c.plo);
                        c.phi = phi;
                        c.plo = plo;
                    }
                    cpp.push(Parted::new(tmpc, nq, hist, |c| c.phi as usize & (nq - 1)));
                    return;
                }
                tmp.clear();
                for_records(
                    ev_chunks[i],
                    |[eid, uid, ts, ing, typ, pr, price]: [&'static [u8]; 7]| {
                        if eid.is_empty() {
                            return;
                        }
                        let ingest = parse_ts(ing).unwrap_or(i64::MAX);
                        let mut valid = !uid.is_empty() && !pr.is_empty();
                        let t = match parse_ts(ts) {
                            Some(v) if v >= ws && v < we => v,
                            _ => {
                                valid = false;
                                0
                            }
                        };
                        let purchase = match typ {
                            b"view" | b"cart" => false,
                            b"purchase" => true,
                            _ => {
                                valid = false;
                                false
                            }
                        };
                        let (mut p, mut cat) = (0, 0);
                        if valid && purchase {
                            match parse_price(price) {
                                Some(v) => {
                                    p = v;
                                    cat = prod.get(pr).copied().unwrap_or(unknown);
                                }
                                None => valid = false,
                            }
                        }
                        if valid && ingest - t > DAY {
                            valid = false;
                        }
                        let (ehi, elo) = h128_mapped(eid);
                        let (uhi, ulo) = if valid { h128_mapped(uid) } else { (0, 0) };
                        if valid && uid.len() > 15 {
                            longs.push((uhi, ulo, sref(uid))); // may name an unlinked person
                        }
                        tmp.push(E1 {
                            ehi,
                            elo,
                            ts: if valid { (t - cws) as u32 } else { 0 },
                            // ponytail: saturates beyond +-68 years of the window, where no copy lives
                            ingest: ((ingest.saturating_sub(ws)) / 1000)
                                .clamp(i32::MIN as i64, i32::MAX as i64)
                                as i32,
                            uhi,
                            ulo,
                            price: p,
                            cat,
                            valid,
                        });
                    },
                );
                evp.push(Parted::new(tmp, np, hist, |r| r.ehi as usize & (np - 1)));
            },
        )
    });
    let links = links_cell.into_inner().unwrap();
    let (mut evp, mut cp, mut longs) = (Vec::new(), Vec::new(), Vec::new());
    for st in s1 {
        evp.extend(st.3);
        cp.extend(st.4);
        longs.extend(st.5);
    }
    log("parsed, links built");

    // stage 2: per eid partition, dedup (smallest ingest wins, before validation) and send valid
    // survivors to person partitions
    const D: usize = 16; // prefetch distance
                         // partitions go in batches so each batch emits one buffer (cheap fan-in for stage 3)
    let per = np.div_ceil(nt * 8);
    let nb = np.div_ceil(per);
    let s2 = par(
        nt,
        nb,
        || {
            (
                IdMap::<(u32, i32)>::default(),
                Vec::<bool>::new(),
                Vec::<&E1>::new(),
                Vec::<u32>::new(),
                (Vec::<E2>::new(), Vec::<u32>::new()),
                Vec::<Parted<E2>>::new(),
            )
        },
        |(best, dead, all, surv, (te, hist), eo), bi| {
            te.clear();
            for p in bi * per..((bi + 1) * per).min(np) {
                all.clear();
                for e in &evp {
                    all.extend(e.get(p).iter());
                }
                best.clear();
                dead.clear();
                dead.resize(all.len(), false);
                for (g, r) in all.iter().enumerate() {
                    match best.entry(key(r.ehi, r.elo)) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert((g as u32, r.ingest));
                        }
                        std::collections::hash_map::Entry::Occupied(mut o) => {
                            let (og, oi) = *o.get();
                            if r.ingest < oi {
                                dead[og as usize] = true;
                                o.insert((g as u32, r.ingest));
                            } else {
                                dead[g] = true;
                            }
                        }
                    }
                }
                surv.clear();
                surv.extend(
                    (0..all.len() as u32).filter(|&g| all[g as usize].valid && !dead[g as usize]),
                );
                // uid hashes (prefetching uid bytes ahead), then link lookups (prefetching slots ahead)
                let m = surv.len();
                for k in 0..m {
                    if k + D < m {
                        let r = all[surv[k + D] as usize];
                        links.prefetch(r.uhi, r.ulo);
                    }
                    let r = all[surv[k] as usize];
                    let (phi, plo) = links.person(r.uhi, r.ulo);
                    te.push(E2 {
                        phi,
                        plo,
                        ts: r.ts,
                        price: r.price,
                        cat: r.cat,
                    });
                }
            }
            eo.push(Parted::new(te, nq, hist, |e| e.phi as usize & (nq - 1)));
        },
    );
    let mut ep = Vec::new();
    let long: FxHashMap<u128, SRef> = links
        .1
        .iter()
        .chain(&longs)
        .map(|&(h, l, r)| (key(h, l), r))
        .collect();
    for st in s2 {
        ep.extend(st.5);
    }
    log("deduped, mapped to persons");
    std::mem::forget(evp); // ponytail: freeing GBs costs time; the process exits soon

    // stage 3: per person partition
    let ncat = cat_names.len();
    let s3 = par(
        nt,
        nq,
        || {
            (
                vec![[0i64; 4]; ncamp * ndays],
                Vec::<URow>::new(),
                Vec::<E2>::new(),
                Vec::<C2>::new(),
                Vec::<u32>::new(),
            )
        },
        |(cc, rows, ev, ck, hist), q| {
            group(ep.iter().map(|x| x.get(q)), ev, hist, |e| {
                (e.phi, e.plo, e.t())
            });
            group(cp.iter().map(|x| x.get(q)), ck, hist, |c| {
                (c.phi, c.plo, c.t())
            });
            let (ev, ck) = (&ev[..], &ck[..]);
            let mut catsum = vec![0i64; ncat];
            let mut starts: Vec<i64> = Vec::new();
            let mut sdays: Vec<u32> = Vec::new();
            let mut byc: Vec<C2> = Vec::new();
            let (mut a, mut c) = (0, 0);
            while a < ev.len() {
                let (phi, plo) = (ev[a].phi, ev[a].plo);
                let mut b = a;
                while b < ev.len() && ev[b].phi == phi && ev[b].plo == plo {
                    b += 1;
                }
                while c < ck.len() && (ck[c].phi, ck[c].plo) < (phi, plo) {
                    c += 1;
                }
                let mut d = c;
                while d < ck.len() && ck[d].phi == phi && ck[d].plo == plo {
                    d += 1;
                }
                let evs = &ev[a..b];
                let cks = &ck[c..d];
                // sessions + user_daily
                starts.clear();
                sdays.clear();
                let mut i = 0;
                let mut kp = 0; // click pointer for credit
                let mut day_row: Option<URow> = None;
                let mut day_has_purchase = false;
                while i < evs.len() {
                    let start = evs[i].t();
                    let mut last = start;
                    let mut j = i;
                    let mut rev = 0i64;
                    while j < evs.len() && evs[j].t() - last <= GAP {
                        last = evs[j].t();
                        if evs[j].price > 0 {
                            rev += evs[j].price as i64;
                            catsum[evs[j].cat as usize] += evs[j].price as i64;
                            day_has_purchase = true;
                        }
                        j += 1;
                    }
                    let eday = (start + cws).div_euclid(DAY);
                    let di = (eday - ws_day) as usize;
                    // credit
                    while kp < cks.len() && cks[kp].t() <= start {
                        kp += 1;
                    }
                    let cid = if kp > 0 && start - cks[kp - 1].t() <= WEEK {
                        let t = cks[kp - 1].t();
                        let mut bi = kp - 1;
                        let mut k = kp - 1;
                        while k > 0 && cks[k - 1].t() == t {
                            k -= 1;
                            if cks[k].cref.get() < cks[bi].cref.get() {
                                bi = k;
                            }
                        }
                        cks[bi].camp
                    } else {
                        organic
                    } as usize;
                    let cell = &mut cc[cid * ndays + di];
                    cell[0] += 1;
                    cell[1] += (rev > 0) as i64;
                    cell[2] += rev;
                    starts.push(start);
                    sdays.push(di as u32);
                    match &mut day_row {
                        Some(r) if r.day == eday as i32 => {
                            r.sessions += 1;
                            r.events += (j - i) as u32;
                            r.revenue += rev;
                        }
                        _ => {
                            day_row = Some(URow {
                                phi,
                                plo,
                                day: eday as i32,
                                sessions: 1,
                                events: (j - i) as u32,
                                revenue: rev,
                                top: u16::MAX,
                            });
                        }
                    }
                    // flush the day if the next session starts on another day
                    let next_day = (j < evs.len()).then(|| (evs[j].t() + cws).div_euclid(DAY));
                    if next_day != Some(eday) {
                        let mut r = day_row.take().unwrap();
                        if day_has_purchase {
                            let mut bc = 0;
                            for (k, &s) in catsum.iter().enumerate() {
                                if s > catsum[bc] {
                                    bc = k;
                                }
                            }
                            r.top = bc as u16;
                            catsum.iter_mut().for_each(|x| *x = 0);
                            day_has_purchase = false;
                        }
                        rows.push(r);
                    }
                    i = j;
                }
                // assists: per campaign, merged [t, t+WEEK] intervals over session starts
                if !cks.is_empty() {
                    byc.clear();
                    byc.extend_from_slice(cks);
                    byc.sort_unstable_by_key(|c| (c.camp, c.t()));
                    let mut x = 0;
                    while x < byc.len() {
                        let cp = byc[x].camp as usize;
                        let mut lo = byc[x].t();
                        let mut hi = lo + WEEK;
                        x += 1;
                        loop {
                            let more = x < byc.len() && byc[x].camp as usize == cp;
                            if more && byc[x].t() <= hi {
                                hi = byc[x].t() + WEEK;
                                x += 1;
                                continue;
                            }
                            let mut s = starts.partition_point(|&v| v < lo);
                            let e = starts.partition_point(|&v| v <= hi);
                            while s < e {
                                let dd = sdays[s];
                                let mut f = s;
                                while f < e && sdays[f] == dd {
                                    f += 1;
                                }
                                cc[cp * ndays + dd as usize][3] += (f - s) as i64;
                                s = f;
                            }
                            if !more {
                                break;
                            }
                            lo = byc[x].t();
                            hi = lo + WEEK;
                            x += 1;
                        }
                    }
                }
                a = b;
                c = d;
            }
        },
    );
    log("sessions done");

    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    let mut cc = vec![[0i64; 4]; ncamp * ndays];
    for (c, ..) in &s3 {
        for (x, y) in cc.iter_mut().zip(c) {
            for k in 0..4 {
                x[k] += y[k];
            }
        }
    }
    let crow: Vec<(usize, usize, [i64; 4])> = (0..ncamp)
        .flat_map(|c| (0..ndays).map(move |d| (c, d)))
        .map(|(c, d)| (c, d, cc[c * ndays + d]))
        .filter(|r| r.2[0] > 0 || r.2[3] > 0)
        .collect();
    write_campaign(
        &out.join("campaign_daily/part-0.parquet"),
        &crow,
        &camp_names,
        &channels,
        ws_day,
    );
    let cats: Vec<&[u8]> = cat_names.clone();
    par(
        nt,
        s3.len(),
        || (),
        |_, i| {
            write_user(
                &out.join(format!("user_daily/part-{i}.parquet")),
                &s3[i].1,
                &cats,
                &long,
            );
        },
    );
    log("written");
}

// ---------- parquet ----------

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn ba(s: &'static [u8]) -> ByteArray {
    ByteArray::from(bytes::Bytes::from_static(s))
}

type Col<'a> = Box<dyn FnMut(&mut parquet::file::writer::SerializedColumnWriter<'_>) + 'a>;
fn write_table(path: &Path, schema: &str, mut cols: Vec<Col<'_>>) {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build(),
    );
    let f = std::fs::File::create(path).unwrap();
    let mut w =
        SerializedFileWriter::new(std::io::BufWriter::with_capacity(1 << 20, f), schema, props)
            .unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut i = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        cols[i](&mut col);
        col.close().unwrap();
        i += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_user(path: &Path, rows: &[URow], cats: &[&'static [u8]], long: &FxHashMap<u128, SRef>) {
    let cats: Vec<&'static [u8]> = cats.to_vec();
    write_table(
        path,
        "message s { required binary user_id (UTF8); required int32 day (DATE); required int32 sessions; required int32 events; required int64 revenue_cents; optional binary top_category (UTF8); }",
        vec![
            Box::new(|c| {
                let mut arena = Vec::with_capacity(rows.len() * 12);
                let spans: Vec<_> = rows.iter().map(|r| name_of(r.phi, r.plo, long, &mut arena)).collect();
                let arena = bytes::Bytes::from(arena);
                let v: Vec<ByteArray> = spans
                    .into_iter()
                    .map(|n| match n {
                        Ok((at, len)) => ByteArray::from(arena.slice(at..at + len)),
                        Err(s) => ba(s),
                    })
                    .collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(|c| {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(|c| {
                let v: Vec<i32> = rows.iter().map(|r| r.sessions as i32).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(|c| {
                let v: Vec<i32> = rows.iter().map(|r| r.events as i32).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(|c| {
                let v: Vec<i64> = rows.iter().map(|r| r.revenue).collect();
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(move |c| {
                let v: Vec<ByteArray> = rows.iter().filter(|r| r.top != u16::MAX).map(|r| ba(cats[r.top as usize])).collect();
                let d: Vec<i16> = rows.iter().map(|r| (r.top != u16::MAX) as i16).collect();
                c.typed::<ByteArrayType>().write_batch(&v, Some(&d), None).unwrap();
            }),
        ],
    );
}

fn write_campaign(
    path: &Path,
    rows: &[(usize, usize, [i64; 4])],
    names: &[&'static [u8]],
    channels: &[&'static [u8]],
    ws_day: i64,
) {
    let int = |k: usize| -> Col<'_> {
        Box::new(move |c| {
            let v: Vec<i64> = rows.iter().map(|r| r.2[k]).collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
        })
    };
    write_table(
        path,
        "message s { required binary campaign_id (UTF8); required binary channel (UTF8); required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions; required int64 revenue_cents; required int64 assisted_sessions; }",
        vec![
            Box::new(|c| {
                let v: Vec<ByteArray> = rows.iter().map(|r| ba(names[r.0])).collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(|c| {
                let v: Vec<ByteArray> = rows.iter().map(|r| ba(channels[r.0])).collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }),
            Box::new(move |c| {
                let v: Vec<i32> = rows.iter().map(|r| (ws_day + r.1 as i64) as i32).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }),
            int(0),
            int(1),
            int(2),
            int(3),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ts() {
        assert_eq!(
            parse_ts(b"2026-10-03T23:58:10Z"),
            parse_ts(b"2026-10-03T19:58:10-04:00")
        );
        assert_eq!(parse_ts(b"1791071890000"), parse_ts(b"0001791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        for bad in [
            &b"2028-02-30T00:00:00Z"[..],
            b"2027-02-29T00:00:00Z",
            b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:10+0200",
            b"2026-10-03T23:58:10+24:00",
            b"1.5",
            b"-5",
            b"",
        ] {
            assert_eq!(parse_ts(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        let mut x: u128 = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210;
        for _ in 0..1000 {
            assert_eq!(unmix128(mix128(x)), x);
            x = x.wrapping_mul(K1).wrapping_add(12345);
        }
        let buf: Vec<u8> = (0..200u32).map(|i| (i * 37 % 251) as u8).collect();
        for st in 16..100 {
            for n in 0..40 {
                assert_eq!(h128_mapped(&buf[st..st + n]), h128(&buf[st..st + n]));
            }
        }
        let none = FxHashMap::default();
        for id in [&b"u1"[..], b"uduffvac", b"A	B", b"exactly15bytes!", b""] {
            let (h, l) = h128(id);
            let mut arena = Vec::new();
            let (at, len) = name_of(h, l, &none, &mut arena).unwrap();
            assert_eq!(&arena[at..at + len], id);
        }
        for y in [
            1899i64, 1900, 1999, 2000, 2025, 2028, 2100, 2299, 2300, 9999,
        ] {
            for m in 1..=12i64 {
                for d in 1..=31i64 {
                    let s = format!("{y:04}-{m:02}-{d:02}T01:02:03Z");
                    let ok = d
                        <= [
                            31,
                            if is_leap(y) { 29 } else { 28 },
                            31,
                            30,
                            31,
                            30,
                            31,
                            31,
                            30,
                            31,
                            30,
                            31,
                        ][m as usize - 1];
                    let want = ok.then(|| (days_from_civil(y, m, d) * 86400 + 3723) * 1000);
                    assert_eq!(parse_ts(s.as_bytes()), want, "{s}");
                }
            }
        }
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
    }
}

#[cfg(test)]
mod tok_tests {
    use super::*;
    /// for_records must agree with the line-by-line split on every tricky shape, at any offset.
    #[test]
    fn tokenizer_matches_split() {
        let cases: &[&str] = &[
            "a,b",
            "\"a\",b",
            "a,\"b\"",
            "\"\",\"\"",
            ",",
            "a,b,c",
            "a",
            "",
            "\"a,b\"",
            "a\"b,c",
            "\"a\"b,c",
            "\"a,b",
            "a,\"b",
            "\"a\"\"\",b",
            " a , b ",
            "a,b\r",
            "\"a\",\"b\"\r",
            "a,",
            ",b",
            "\"a\" ,b",
            "a,\"\"",
            "x".repeat(70).leak(),
            "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",b",
        ];
        for pad in 0..70 {
            let mut text = String::new();
            text.push_str(&"q".repeat(pad));
            text.push_str(",z\n");
            for c in cases {
                text.push_str(c);
                text.push('\n');
            }
            for last in ["a,b", "\"a\",", "a,\"b"] {
                let t: &'static [u8] = format!("{text}{last}").leak().as_bytes();
                let mut want = Vec::new();
                for_lines(t, |l| {
                    if let Some(f) = split::<2>(l) {
                        want.push(f)
                    }
                });
                let mut got = Vec::new();
                for_records(t, |f: [&'static [u8]; 2]| got.push(f));
                assert_eq!(want, got, "pad {pad} last {last}");
            }
        }
    }
}
