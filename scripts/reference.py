#!/usr/bin/env python3
"""Slow, straightforward reference for rules.md, independent of the Rust pipeline.
Usage: reference.py DATA_DIR OUT_DIR  -> writes OUT_DIR/user_daily.csv and campaign_daily.csv
(the format compare.py reads). For cross-checking on small generated datasets."""
import bisect, glob, json, os, re, sys
from collections import defaultdict
from datetime import datetime, timezone

DAY = 86_400_000
F1 = re.compile(rb'^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$')
F2 = re.compile(rb'^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$')
F3 = re.compile(rb'^[0-9]+$')


def ts(b):
    m = F1.match(b) or F2.match(b)
    if m:
        y, mo, d, h, mi, s = (int(x) for x in m.groups()[:6])
        try:
            dt = datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
        except ValueError:
            return None
        v = int(dt.timestamp()) * 1000
        if len(m.groups()) > 6:
            oh, om = int(m.group(8)), int(m.group(9))
            if oh > 23 or om > 59:
                return None
            off = (oh * 60 + om) * 60_000
            v = v - off if m.group(7) == b'+' else v + off
        return v
    if F3.match(b):
        v = int(b)
        return v if v < 2**63 else None
    return None


def split(line, n):
    if line == b'':
        return None
    parts = line.split(b',')
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if len(p) >= 2 and p[:1] == b'"' and p[-1:] == b'"' and b'"' not in p[1:-1]:
            out.append(p[1:-1])
        elif b'"' in p:
            return None
        else:
            out.append(p)
    return out


def rows(path, n):
    data = open(path, 'rb').read()
    lines = data.split(b'\n')
    if data.endswith(b'\n'):
        lines.pop()
    for line in lines[1:]:
        r = split(line, n)
        if r is not None:
            yield r


def folder(d, n):
    for p in sorted(glob.glob(os.path.join(d, '*.csv'))):
        yield from rows(p, n)


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, 'manifest.json')))
    ws, we = ts(man['window_start'].encode()), ts(man['window_end'].encode())
    cat = {p: c for p, c in rows(os.path.join(data, 'products.csv'), 2)}
    chan = {c: ch for c, ch in rows(os.path.join(data, 'campaigns.csv'), 2)}

    parent = {}
    def find(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x
    for a, b in folder(os.path.join(data, 'links'), 2):
        if a and b:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)
    comps = defaultdict(list)
    for x in list(parent):
        comps[find(x)].append(x)
    name = {}
    for r, xs in comps.items():
        m = min(xs + [r])
        for x in xs + [r]:
            name[x] = m
    person = lambda u: name.get(u, u)

    best = {}
    for r in folder(os.path.join(data, 'events'), 7):
        if not r[0]:
            continue
        ing = ts(r[3])
        if r[0] not in best or ing < best[r[0]][0]:
            best[r[0]] = (ing, r)
    evs = defaultdict(list)
    for ing, (eid, uid, t, _, typ, prod, price) in best.values():
        t = ts(t)
        if not uid or t is None or not (ws <= t < we) or typ not in (b'view', b'cart', b'purchase') or not prod:
            continue
        p = 0
        if typ == b'purchase':
            if not re.fullmatch(rb'[0-9]+', price) or not (1 <= int(price) <= 10_000_000):
                continue
            p = int(price)
        if ing - t > DAY:
            continue
        evs[person(uid)].append((t, p, cat.get(prod, b'unknown')))

    clicks = defaultdict(list)
    for cid, uid, cp, t in folder(os.path.join(data, 'clicks'), 4):
        t = ts(t) if cid and uid else None
        if t is None or not (ws - 7 * DAY <= t < we) or cp not in chan:
            continue
        clicks[person(uid)].append((t, cid, cp))

    ud = {}
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for per, es in evs.items():
        es.sort()
        cs = sorted(clicks.get(per, []))
        cts = [c[0] for c in cs]
        sessions = []
        for e in es:
            if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        for s in sessions:
            st = s[0][0]
            day = st // DAY
            rev = sum(e[1] for e in s)
            u = ud.setdefault((per, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[1]:
                    u[3][e[2]] += e[1]
            lo, hi = bisect.bisect_left(cts, st - 7 * DAY), bisect.bisect_right(cts, st)
            win = cs[lo:hi]
            if win:
                tmax = max(c[0] for c in win)
                camp = min(c[1:] for c in win if c[0] == tmax)[1]
            else:
                camp = b'organic'
            c = cd[(camp, day)]
            c[0] += 1
            c[1] += rev > 0
            c[2] += rev
            for cp in {c[2] for c in win}:
                cd[(cp, day)][3] += 1

    d = lambda day: datetime.fromtimestamp(day * 86400, tz=timezone.utc).strftime('%Y-%m-%d')
    s = lambda b: b.decode('utf-8', 'surrogateescape')
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, 'user_daily.csv'), 'w', encoding='utf-8', newline='') as f:
        f.write('user_id,day,sessions,events,revenue_cents,top_category\n')
        for (per, day), (n, ne, rev, cats) in ud.items():
            top = min(cats, key=lambda c: (-cats[c], c)) if cats else b''
            f.write(f'{s(per)},{d(day)},{n},{ne},{rev},{s(top)}\n')
    with open(os.path.join(out, 'campaign_daily.csv'), 'w', encoding='utf-8', newline='') as f:
        f.write('campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n')
        for (cp, day), v in cd.items():
            f.write(f'{s(cp)},{s(chan.get(cp, b"organic"))},{d(day)},{v[0]},{v[1]},{v[2]},{v[3]}\n')


if __name__ == '__main__':
    main()
