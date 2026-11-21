//! Shopping Sessions pipeline. Rules: rules.md. Flow:
//! links -> union-find; events parsed into eid-partitions -> dedup/validate ->
//! person-partitions; clicks parsed straight into person-partitions; then each
//! person-partition is sorted, sessionized, attributed and aggregated.
//! IDs are identified by xxh3-128 hashes (collision odds ~1e-20 at 60M rows);
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
const P: usize = 512; // eid partitions
const Q: usize = 512; // person partitions
const CHUNK: usize = 4 << 20;

// ---------- small utilities ----------

/// A &'static [u8] packed into 8 bytes: pointer in the low 48 bits, length in the high 16.
#[derive(Clone, Copy)]
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

#[inline]
fn h128(s: &[u8]) -> (u64, u64) {
    let h = xxhash_rust::xxh3::xxh3_128(s);
    ((h >> 64) as u64, h as u64)
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

fn par<T: Send>(nt: usize, n: usize, init: impl Fn() -> T + Sync, work: impl Fn(&mut T, usize) + Sync) -> Vec<T> {
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

/// Data lines (header dropped) of every *.csv in dir, cut into ~CHUNK pieces on line boundaries.
fn chunks(dir: &Path) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else { return out };
    for e in rd {
        let p = e.unwrap().path();
        if p.extension().map_or(true, |x| x != "csv") || !p.is_file() {
            continue;
        }
        let d = map_file(&p);
        let Some(h) = memchr::memchr(b'\n', d) else { continue };
        let mut s = h + 1;
        while s < d.len() {
            let mut e = (s + CHUNK).min(d.len());
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

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

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
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
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
            Some(if s[19] == b'+' { base - off } else { base + off })
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

struct E1 {
    ehi: u64,
    elo: u64,
    uhi: u64,
    ulo: u64,
    ts: i64,
    ingest: i64,
    uref: SRef,
    price: u32, // >0 only for a valid purchase
    cat: u16,
    valid: bool,
}
#[derive(Clone, Copy)]
struct E2 {
    phi: u64,
    plo: u64,
    ts: i64,
    nref: SRef,
    price: u32,
    cat: u16,
}
#[derive(Clone, Copy)]
struct C2 {
    phi: u64,
    plo: u64,
    ts: i64,
    cref: SRef,
    camp: u32,
}
struct URow {
    nref: SRef,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u16, // u16::MAX = null
}

type Person = (u64, u64, SRef);
/// Linked IDs, partitioned like the person partitions (uid hi & (Q-1)), so each lookup stays cache-local.
struct Links(Vec<IdMap<Person>>);
impl Links {
    #[inline]
    fn person(&self, hi: u64, lo: u64, r: SRef) -> Person {
        match self.0[hi as usize & (Q - 1)].get(&key(hi, lo)) {
            Some(&p) => p,
            None => (hi, lo, r),
        }
    }
}

fn build_links(nt: usize, dir: &Path) -> Links {
    let ch = chunks(dir);
    let parts = par(nt, ch.len(), Vec::new, |v: &mut Vec<(SRef, u64, u64, SRef, u64, u64)>, i| {
        for_lines(ch[i], |l| {
            if let Some([a, b]) = split::<2>(l) {
                if !a.is_empty() && !b.is_empty() {
                    let (ah, al) = h128(a);
                    let (bh, bl) = h128(b);
                    v.push((sref(a), ah, al, sref(b), bh, bl));
                }
            }
        })
    });
    let mut maps: Vec<IdMap<u32>> = (0..Q).map(|_| IdMap::default()).collect();
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
        *maps[h as usize & (Q - 1)].entry(key(h, l)).or_insert_with(|| {
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
    let maps: Vec<IdMap<Person>> = par(nt, Q, Vec::new, |v: &mut Vec<(usize, IdMap<Person>)>, u| {
        let m = maps[u]
            .iter()
            .map(|(&k, &i)| {
                let b = best[root[i as usize] as usize] as usize;
                (k, (hs[b].0, hs[b].1, refs[b]))
            })
            .collect();
        v.push((u, m));
    })
    .into_iter()
    .flatten()
    .collect::<std::collections::BTreeMap<_, _>>()
    .into_values()
    .collect();
    Links(maps)
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
    let nt: usize = env("NCPU", "0").parse().ok().filter(|&n| n > 0).unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(8, usize::from)
    });
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
    let mut cat_names: Vec<&[u8]> = prod_rows.iter().map(|r| r.1).chain([&b"unknown"[..]]).collect();
    cat_names.sort();
    cat_names.dedup();
    let cat_idx: FxHashMap<&[u8], u16> = cat_names.iter().enumerate().map(|(i, c)| (*c, i as u16)).collect();
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
    let camp: FxHashMap<&[u8], u32> = camp_names.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let organic = camp_names.len() as u32;
    camp_names.push(b"organic");
    channels.push(b"organic");
    let ncamp = camp_names.len();

    // stage 1, concurrent with building links: events into eid partitions, valid clicks into uid partitions
    // (click dedup is a no-op: copies are identical, and credit/assist results don't change with repeats)
    let ev_chunks = chunks(&data.join("events"));
    let ck_chunks = chunks(&data.join("clicks"));
    let ne = ev_chunks.len();
    let links_dir = data.join("links");
    let cws = ws - WEEK;
    let (s1, links) = std::thread::scope(|sc| {
        let lh = sc.spawn(|| build_links(1.max(nt / 4), &links_dir));
        let s1 = par(
            nt,
            ne + ck_chunks.len(),
            || ((0..P).map(|_| Vec::<E1>::new()).collect::<Vec<_>>(), (0..Q).map(|_| Vec::<C2>::new()).collect::<Vec<_>>()),
            |(parts, cparts), i| {
                if i >= ne {
                    for_lines(ck_chunks[i - ne], |l| {
                        let Some([cid, uid, cp, ts]) = split::<4>(l) else { return };
                        if cid.is_empty() || uid.is_empty() {
                            return;
                        }
                        let Some(t) = parse_ts(ts).filter(|&t| t >= cws && t < we) else { return };
                        let Some(&c) = camp.get(cp) else { return };
                        let (h, lo) = h128(uid);
                        cparts[h as usize & (Q - 1)].push(C2 { phi: h, plo: lo, ts: t, cref: sref(cid), camp: c });
                    });
                    return;
                }
                for_lines(ev_chunks[i], |l| {
                    let Some([eid, uid, ts, ing, typ, pr, price]) = split::<7>(l) else { return };
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
                    let (ehi, elo) = h128(eid);
                    let (uhi, ulo) = if valid { h128(uid) } else { (0, 0) };
                    parts[ehi as usize & (P - 1)].push(E1 {
                        ehi,
                        elo,
                        uhi,
                        ulo,
                        ts: t,
                        ingest,
                        uref: if valid { sref(uid) } else { SRef(0) },
                        price: p,
                        cat,
                        valid,
                    });
                })
            },
        );
        (s1, lh.join().unwrap())
    });
    log("parsed, links built");

    // stage 2: dedup per eid partition (smallest ingest wins, before validation); valid survivors
    // go to uid partitions as E2 with the uid still in phi/plo
    let s2 = par(
        nt,
        P,
        || (IdMap::<(u32, i64)>::default(), Vec::<bool>::new(), (0..Q).map(|_| Vec::<E2>::new()).collect::<Vec<_>>()),
        |(best, dead, parts), p| {
            best.clear();
            dead.clear();
            for st in &s1 {
                for r in &st.0[p] {
                    let g = dead.len() as u32;
                    dead.push(false);
                    match best.entry(key(r.ehi, r.elo)) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert((g, r.ingest));
                        }
                        std::collections::hash_map::Entry::Occupied(mut o) => {
                            let (og, oi) = *o.get();
                            if r.ingest < oi {
                                dead[og as usize] = true;
                                o.insert((g, r.ingest));
                            } else {
                                dead[g as usize] = true;
                            }
                        }
                    }
                }
            }
            let mut g = 0;
            for st in &s1 {
                for r in &st.0[p] {
                    if r.valid && !dead[g] {
                        parts[r.uhi as usize & (Q - 1)].push(E2 { phi: r.uhi, plo: r.ulo, ts: r.ts, nref: r.uref, price: r.price, cat: r.cat });
                    }
                    g += 1;
                }
            }
        },
    );
    log("deduped");

    // stage 2.5: per uid partition, map IDs to persons with that partition's small link map
    let s25 = par(
        nt,
        Q,
        || ((0..Q).map(|_| Vec::<E2>::new()).collect::<Vec<_>>(), (0..Q).map(|_| Vec::<C2>::new()).collect::<Vec<_>>()),
        |(ep, cp), u| {
            for st in &s2 {
                for e in &st.2[u] {
                    let (phi, plo, nref) = links.person(e.phi, e.plo, e.nref);
                    ep[phi as usize & (Q - 1)].push(E2 { phi, plo, nref, ..*e });
                }
            }
            for st in &s1 {
                for c in &st.1[u] {
                    let (phi, plo, _) = links.person(c.phi, c.plo, SRef(0));
                    cp[phi as usize & (Q - 1)].push(C2 { phi, plo, ..*c });
                }
            }
        },
    );
    std::mem::forget(s1); // ponytail: freeing GBs costs time; the process exits soon
    std::mem::forget(s2);
    log("mapped to persons");

    // stage 3: per person partition
    let ncat = cat_names.len();
    let s3 = par(
        nt,
        Q,
        || (vec![[0i64; 4]; ncamp * ndays], Vec::<URow>::new()),
        |(cc, rows), q| {
            let mut ev: Vec<&E2> = s25.iter().flat_map(|s| s.0[q].iter()).collect();
            let mut ck: Vec<&C2> = s25.iter().flat_map(|s| s.1[q].iter()).collect();
            ev.sort_unstable_by_key(|e| (e.phi, e.plo, e.ts));
            ck.sort_unstable_by_key(|c| (c.phi, c.plo, c.ts));
            let mut catsum = vec![0i64; ncat];
            let mut starts: Vec<i64> = Vec::new();
            let mut sdays: Vec<u32> = Vec::new();
            let mut byc: Vec<&C2> = Vec::new();
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
                let nref = evs[0].nref;
                // sessions + user_daily
                starts.clear();
                sdays.clear();
                let mut i = 0;
                let mut kp = 0; // click pointer for credit
                let mut day_row: Option<URow> = None;
                let mut day_has_purchase = false;
                while i < evs.len() {
                    let start = evs[i].ts;
                    let mut last = start;
                    let mut j = i;
                    let mut rev = 0i64;
                    while j < evs.len() && evs[j].ts - last <= GAP {
                        last = evs[j].ts;
                        if evs[j].price > 0 {
                            rev += evs[j].price as i64;
                            catsum[evs[j].cat as usize] += evs[j].price as i64;
                            day_has_purchase = true;
                        }
                        j += 1;
                    }
                    // note: catsum was updated before we know the day flushes; handle by
                    // flushing the previous day *before* processing (see below)
                    let eday = start.div_euclid(DAY);
                    let di = (eday - ws_day) as usize;
                    // credit
                    while kp < cks.len() && cks[kp].ts <= start {
                        kp += 1;
                    }
                    let cid = if kp > 0 && start - cks[kp - 1].ts <= WEEK {
                        let t = cks[kp - 1].ts;
                        let mut bi = kp - 1;
                        let mut k = kp - 1;
                        while k > 0 && cks[k - 1].ts == t {
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
                            day_row = Some(URow { nref, day: eday as i32, sessions: 1, events: (j - i) as u32, revenue: rev, top: u16::MAX });
                        }
                    }
                    // flush the day if the next session starts on another day
                    let next_day = (j < evs.len()).then(|| evs[j].ts.div_euclid(DAY));
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
                    byc.sort_unstable_by_key(|c| (c.camp, c.ts));
                    let mut x = 0;
                    while x < byc.len() {
                        let cp = byc[x].camp as usize;
                        let mut lo = byc[x].ts;
                        let mut hi = lo + WEEK;
                        x += 1;
                        loop {
                            let more = x < byc.len() && byc[x].camp as usize == cp;
                            if more && byc[x].ts <= hi {
                                hi = byc[x].ts + WEEK;
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
                            lo = byc[x].ts;
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
    for (c, _) in &s3 {
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
    write_campaign(&out.join("campaign_daily/part-0.parquet"), &crow, &camp_names, &channels, ws_day);
    let cats: Vec<&[u8]> = cat_names.clone();
    par(nt, s3.len(), || (), |_, i| {
        write_user(&out.join(format!("user_daily/part-{i}.parquet")), &s3[i].1, &cats);
    });
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
    let props = Arc::new(WriterProperties::builder().set_dictionary_enabled(false).build());
    let f = std::fs::File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(std::io::BufWriter::with_capacity(1 << 20, f), schema, props).unwrap();
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

fn write_user(path: &Path, rows: &[URow], cats: &[&'static [u8]]) {
    let cats: Vec<&'static [u8]> = cats.to_vec();
    write_table(
        path,
        "message s { required binary user_id (UTF8); required int32 day (DATE); required int32 sessions; required int32 events; required int64 revenue_cents; optional binary top_category (UTF8); }",
        vec![
            Box::new(|c| {
                let v: Vec<ByteArray> = rows.iter().map(|r| ba(r.nref.get())).collect();
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

fn write_campaign(path: &Path, rows: &[(usize, usize, [i64; 4])], names: &[&'static [u8]], channels: &[&'static [u8]], ws_day: i64) {
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
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts(b"1791071890000"), parse_ts(b"0001791071890000"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        for bad in [&b"2028-02-30T00:00:00Z"[..], b"2027-02-29T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10+24:00", b"1.5", b"-5", b""] {
            assert_eq!(parse_ts(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
    }
}
