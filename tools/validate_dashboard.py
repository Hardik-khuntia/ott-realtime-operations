"""
validate_dashboard.py - checks the dashboard against simulated data, no Kafka
or Atlas needed.

    python tools/validate_dashboard.py

Simulates 40 minutes of events through the REAL producer.py generators and
consumer.py normalize_event(), with a CDN incident on edge-mum-01 during the
last 10 minutes. Then, for every tile: evaluates its calculated fields
(including the $$NOW time windows), applies its filters, checks every field
exists and every filter value occurs, and computes the number(s) the tile
would show. Also checks the 8-column grid has no overlaps or gaps.
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import producer as p
import consumer as c

DASH = os.path.join(HERE, "..", "dashboards", "OTT_Media_Final_Dashboard.charts")
MINUTES, TICK = 40, 0.1          # 40 simulated minutes, 0.1 s per tick
INCIDENT = (30, 39)              # minutes during which edge-mum-01 is down


# ------------------------------------------------------------ simulation
def simulate():
    start = datetime.now(timezone.utc) - timedelta(minutes=MINUTES)
    clock = {"t": start}
    p.now_iso = lambda: clock["t"].isoformat()          # fast-forwardable clock
    gens = {n: (cls(seed=i), topic) for i, (n, (cls, topic)) in enumerate(p.GENERATORS.items())}
    docs, ticks = [], int(MINUTES * 60 / TICK)
    for t in range(ticks):
        minute = t * TICK / 60
        if INCIDENT[0] <= minute < INCIDENT[1]:
            p.STATE.degrade("edge-mum-01", 9999)
        else:
            p.STATE.clear()
        clock["t"] = start + timedelta(seconds=t * TICK)
        for name, (gen, topic) in gens.items():
            if t % max(1, round(p.INTERVALS[name] / TICK)) == 0:
                _, ev = gen.next_event()
                ev = json.loads(json.dumps(ev))          # exactly what Kafka carries
                d = c.normalize_event(topic, ev)
                d["ingested_at"] = d["timestamp"] + timedelta(milliseconds=300)  # assumed network lag
                docs.append(d)
    p.STATE.clear()
    return docs, start + timedelta(minutes=MINUTES)


# ------------------------------------------------------------ tiny MQL evaluator
def ev(e, doc, now):
    if isinstance(e, str):
        if e == "$$NOW":
            return now
        return doc.get(e[1:]) if e.startswith("$") else e
    if isinstance(e, list):
        return [ev(x, doc, now) for x in e]
    if not isinstance(e, dict):
        return e
    (op, a), = e.items()
    if op == "$cond":
        return ev(a[1], doc, now) if ev(a[0], doc, now) else ev(a[2], doc, now)
    if op == "$and":
        return all(ev(x, doc, now) for x in a)
    if op == "$eq":
        x, y = ev(a, doc, now); return x == y
    if op == "$in":
        x, y = ev(a, doc, now); return x in y
    if op == "$gte":
        x, y = ev(a, doc, now); return x is not None and x >= y
    if op == "$dateSubtract":
        return ev(a["startDate"], doc, now) - timedelta(**{a["unit"] + "s": a["amount"]})
    if op == "$subtract":
        x, y = ev(a, doc, now); return (x - y).total_seconds() * 1000
    if op == "$divide":
        x, y = ev(a, doc, now); return x / y
    if op == "$dateToString":
        d = ev(a["date"], doc, now) + timedelta(hours=5, minutes=30)   # Asia/Kolkata
        return d.strftime(a["format"])
    raise ValueError(f"evaluator does not know {op}")


def aggregate(vals, agg):
    vals = [v for v in vals if v is not None]
    if agg == "count":
        return len(vals)
    if not vals:
        return None
    return sum(vals) / len(vals) if agg == "mean" else sum(vals)


def main():
    docs, now = simulate()
    d = json.load(open(DASH))
    problems = 0
    print(f"Simulated {len(docs)} events over {MINUTES} min; incident on edge-mum-01 "
          f"(IN-West) in minutes {INCIDENT[0]}-{INCIDENT[1]}\n")
    for key, it in sorted(d["items"].items(), key=lambda kv: int(kv[0].split("-")[1])):
        issues = []
        rows = []
        for doc in docs:
            r = dict(doc)
            for cf in it["calculatedFields"]:
                r[cf["fieldPath"]] = ev(json.loads(cf["derivedMQL"]), doc, now)
            rows.append(r)
        calc = {cf["fieldPath"] for cf in it["calculatedFields"]}
        for ch in it["channels"].values():
            f = ch["field"]
            if f != "_id" and not any(f in r for r in rows):
                issues.append(f"field '{f}' not found")
        keep = rows
        for flt in it["filters"]:
            f, st = flt["fieldPath"], flt["settings"]
            present = {r.get(f) for r in rows}
            missing = [v for v in st["values"] if v not in present]
            if missing:
                issues.append(f"filter values {missing} never occur in '{f}'")
            keep = [r for r in keep if (r.get(f) in st["values"]) != st["allOthers"]]
        if not keep:
            issues.append("no documents left after filters")

        chs = it["channels"]
        vch = chs.get("value") or chs.get("x" if it["chartType"].endswith("Bar") else "y") or chs.get("color")
        if it["chartType"] == "Heatmap":
            vch = chs["color"]
        vf = None if vch["field"] == "_id" else vch["field"]
        if it["chartType"] == "Number":
            out = aggregate([r.get(vf) if vf else 1 for r in keep], vch["aggregate"])
            shown = f"{out:,.2f}" if isinstance(out, float) else f"{out}"
            if out is None:
                issues.append("Number tile would be empty")
        else:
            gfield = (chs.get("y") if it["chartType"].endswith("Bar") else chs.get("x") or chs.get("label"))["field"]
            if it["chartType"] == "Donut":
                gfield = chs["label"]["field"]
            groups = defaultdict(list)
            for r in keep:
                if r.get(gfield) is not None:
                    groups[r[gfield]].append(r.get(vf) if vf else 1)
            res = {g: aggregate(v, vch["aggregate"]) for g, v in groups.items()}
            res = {g: v for g, v in res.items() if v not in (None, 0)}
            if not res:
                issues.append("chart would be empty")
            top = sorted(res.items(), key=lambda kv: -kv[1])[:3]
            shown = ", ".join(f"{g}={v:.1f}" if isinstance(v, float) else f"{g}={v}" for g, v in top)
            if it["chartType"] == "Heatmap":
                node = defaultdict(list)
                for r in keep:
                    node[r["cdn_node"]].append(r["buffering_rate_pct"])
                shown = "max per-node avg: " + ", ".join(
                    f"{n}={sum(v)/len(v):.1f}" for n, v in sorted(node.items(), key=lambda kv: -sum(kv[1])/len(kv[1]))[:2])
        status = "PASS" if not issues else "FAIL"
        print(f"{status}  {it['title'][:58]:58s} | {shown[:70]}")
        for i in issues:
            print("        -", i)
        problems += len(issues)

    cells = {}
    lay = d["dashboards"]["dashboard-1"]["layout"]
    for l in lay:
        for x in range(l["x"], l["x"] + l["w"]):
            for y in range(l["y"], l["y"] + l["h"]):
                if (x, y) in cells:
                    print(f"FAIL  layout overlap at {(x, y)}"); problems += 1
                cells[(x, y)] = l["i"]
    height = max(l["y"] + l["h"] for l in lay)
    holes = [(x, y) for x in range(8) for y in range(height) if (x, y) not in cells]
    if holes:
        print(f"FAIL  layout gaps {holes[:5]}"); problems += 1
    print(f"\nLayout: {len(lay)} tiles, 8 x {height} grid, no overlaps/gaps" if not holes else "")
    print("RESULT:", "DASHBOARD VALID" if not problems else f"{problems} PROBLEMS")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
