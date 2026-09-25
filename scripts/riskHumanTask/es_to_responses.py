#!/usr/bin/env python3
"""RiskHumanTask_FINISHED consumer errors (ES) → {applicationReferenceId: riskHumanTaskResponse} canonical EJSON.

  frag-query heads.json > fragments_query.es      Q2 response → Q3 Kibana query
  convert parsed.json fragments.json -o out.json  Q1 + Q3 responses → rem_human_task_responses.json format
"""
import argparse, json, re, sys
from datetime import datetime, timezone

LOGGER = "com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer"
ERR_MSG = "Error generated while processing Step Completion on Filter Enabled consumer"
EVENT_KV = '"eventType":"RiskHumanTask_FINISHED"'
UW_KEYS = ("reason", "comment")
DROP_KEYS = {"blazeDeviations", "blazeRetriggeredByUW"}
EXCLUDE_DECISION = "APPROVED"
CHUNK = 16384          # docker json-file split size
WINDOW_MS = 2000       # tail chunks ∈ [head ts, head ts + WINDOW_MS]
CLAUSES = 400          # should-clauses per Q3 request (< max_clause_count 1024)
INT32 = 2 ** 31
OID = re.compile(r"^[0-9a-f]{24}$")
REF_RE = re.compile(r'"applicationReferenceId":"([^"]+)"')


def load(path):
    raw = open(path, encoding="utf-8").read()
    # Kibana Dev Tools renders long strings as """...""" (raw, unescaped) → JSON strings
    # closing """ must be followed by , } ] or newline → content may itself start/end with '"' (split chunks)
    raw = re.sub(r'"""(.*?)"""(?=\s*[,}\]\n])', lambda m: json.dumps(m.group(1)), raw, flags=re.S)
    d = json.loads(raw)
    return d.get("response", d)  # _async_search wraps result in "response"


def ts_ns(s):
    """ISO ts (≤9 fraction digits, any offset) → epoch ns; unparsable → -1."""
    if not s:
        return -1
    m = re.match(r"^(.*?T\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)?$", s)
    if not m:
        return -1
    base, frac, tz = m.group(1), (m.group(2) or ""), (m.group(3) or "Z")
    tz = "+00:00" if tz == "Z" else tz
    try:
        dt = datetime.fromisoformat(base + tz)
    except ValueError:
        return -1
    return int(dt.timestamp()) * 10 ** 9 + int((frac + "000000000")[:9])


def build(p, event_type):
    """≡ painless script_field."""
    tasks = {}
    for k, t in (p.get("tasks") or {}).items():
        if t is None:
            continue
        d = t.get("details") or {}
        ud = d.get("userDetails") or {}
        if k == "UNDERWRITING_DECISION":
            ud_out = {x: ud[x] for x in UW_KEYS if x in ud}
        else:
            ud_out = {x: v for x, v in ud.items() if x not in DROP_KEYS}
        tasks[k] = {
            "status": t.get("status"),
            "details": {"userDetails": ud_out, "lastModifiedByUser": d.get("lastModifiedByUser"), "lastModifiedOn": d.get("lastModifiedOn")},
        }
    return {
        "uniqueIdentifier": "UNAVAILABLE",
        "stepStatus": "FINISHED" if (event_type or "").endswith("_FINISHED") else event_type,
        "applicationId": p.get("applicationId"),
        "decision": p.get("decision"),
        "tasks": tasks,
        "_class": "HumanTaskResponse",
    }


def ejson(v):
    # int∈int32 → $numberInt, int∉int32 → $numberLong, float → $numberDouble (≡ existing file typing)
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, int):
        return {"$numberInt" if -INT32 <= v < INT32 else "$numberLong": str(v)}
    if isinstance(v, float):
        return {"$numberDouble": repr(v)}
    if isinstance(v, list):
        return [ejson(x) for x in v]
    return {k: ejson(x) for k, x in v.items()}


def validate(r):
    errs = []
    if not OID.match(str(r.get("applicationId") or "")):
        errs.append("applicationId")
    if not r.get("decision") or r["decision"] == EXCLUDE_DECISION:
        errs.append("decision=%s" % r.get("decision"))
    if r.get("stepStatus") != "FINISHED":
        errs.append("stepStatus=%s" % r.get("stepStatus"))
    if not r.get("tasks"):
        errs.append("tasks empty")
    return errs


HEAD_RE = re.compile(r'"logger"\s*:\s*"')


def is_head(m):
    h = HEAD_RE.search(m, 0, 600)
    return m.startswith("{") and h is not None and h.start() < 512


def hits_of(d):
    h = d.get("hits") or {}
    total = (h.get("total") or {}).get("value", 0)
    return h.get("hits") or [], total


def cid_of(src):
    return str(((src.get("docker") or {}).get("container_id")) or "")


# ---------------------------------------------------------------- frag-query
def cmd_frag_query(a):
    hits, total = hits_of(load(a.heads))
    if total > len(hits):
        print("WARN heads total=%d > returned=%d → narrow the time range" % (total, len(hits)), file=sys.stderr)
    heads = sorted({(cid_of(h["_source"]), ts_ns(h["_source"].get("@timestamp"))) for h in hits})
    heads = [(c, t) for c, t in heads if c and t >= 0]
    if not heads:
        sys.exit("no heads in %s" % a.heads)
    for i in range(0, len(heads), CLAUSES):
        part = heads[i:i + CLAUSES]
        lo = min(t for _, t in part) // 10 ** 6
        hi = max(t for _, t in part) // 10 ** 6 + WINDOW_MS
        should = [{"bool": {"filter": [
            {"match_phrase": {"docker.container_id": c}},
            {"range": {"@timestamp": {"gte": t // 10 ** 6, "lte": t // 10 ** 6 + WINDOW_MS, "format": "epoch_millis"}}}]}} for c, t in part]
        q = {
            "size": 10000,
            "track_total_hits": True,
            "_source": ["message", "docker.container_id", "@timestamp"],
            "query": {"bool": {
                "filter": [
                    {"range": {"@timestamp": {"gte": lo, "lte": hi, "format": "epoch_millis"}}},
                    {"match_phrase": {"kubernetes.container_name": "personal-loan-orchestrator"}}],
                "must_not": [{"exists": {"field": "logger"}}],
                "should": should,
                "minimum_should_match": 1}},
        }
        print("# Q3 part %d/%d: %d heads → save response as fragments_%d.json" % (i // CLAUSES + 1, (len(heads) - 1) // CLAUSES + 1, len(part), i // CLAUSES + 1))
        print("POST personal-loan-prod-*/_search")
        print(json.dumps(q, separators=(",", ":")))
        print()
    print("heads=%d → %d request(s)" % (len(heads), (len(heads) - 1) // CLAUSES + 1), file=sys.stderr)


# ---------------------------------------------------------------- convert
def stitch(docs, rep):
    """docs: [_source] fragments → [(ref, ts, rawLog, chunks)] for consumer-error lines."""
    by_c = {}
    for s in docs:
        m = s.get("message")
        c = cid_of(s)
        if m is None or not c:
            continue
        rank = 0 if is_head(m) else (1 if len(m) >= CHUNK else 2)
        by_c.setdefault(c, []).append((ts_ns(s.get("@timestamp")), rank, m, s.get("@timestamp")))
    out = []
    for c, fr in by_c.items():
        fr.sort(key=lambda x: (x[0], x[1]))
        cur = None
        for t, rank, m, raw_ts in fr:
            if rank == 0:
                if cur:
                    out.append(cur)
                cur = {"c": c, "ts": raw_ts, "parts": [m]}
            elif cur:
                cur["parts"].append(m)
            elif EVENT_KV in m:
                r = REF_RE.search(m)
                rep["orphanTails"].append("%s container=%s ts=%s" % (r.group(1) if r else None, c[:12], raw_ts))
        if cur:
            out.append(cur)
    res = []
    for r in out:
        raw = "".join(r["parts"])
        if LOGGER not in raw or ERR_MSG not in raw:
            continue
        m = REF_RE.search(raw)
        if EVENT_KV not in raw:
            if not raw.rstrip().endswith("}"):  # tail lost → eventType unknown → may be RiskHumanTask
                rep["incomplete"].append("%s container=%s ts=%s chunks=%d len=%d" % (m.group(1) if m else None, r["c"][:12], r["ts"], len(r["parts"]), len(raw)))
            continue
        res.append((m.group(1) if m else None, r["ts"], raw, len(r["parts"]), r["c"]))
    return res


def cmd_convert(a):
    picked = {}
    rep = {"parsed": 0, "fragments": 0, "invalid": [], "parseFailed": [], "incomplete": [], "orphanTails": [], "warn": []}

    def put(ref, ts, resp, src):
        errs = validate(resp)
        if errs:
            if resp.get("decision") != EXCLUDE_DECISION:
                rep["invalid"].append("%s(%s): %s" % (ref, src, ",".join(errs)))
            return
        prev = picked.get(ref)
        if prev is None or ts_ns(ts) > ts_ns(prev[0]):
            picked[ref] = (ts, resp, src)

    frag_docs, seen = [], set()
    for f in a.es_response:
        d = load(f)
        aggs = d.get("aggregations") or {}
        # Q1: parsed by_app buckets (script_field already shaped)
        for b in (aggs.get("parsed", aggs).get("by_app") or {}).get("buckets", []):
            h = b["latest"]["hits"]["hits"][0]
            ref = h["_source"]["maximusLogDetails"]["applicationReferenceId"]
            resp = (h.get("fields", {}).get("riskHumanTaskResponse") or [None])[0]
            if resp is None:
                rep["invalid"].append("%s(parsed): no script field" % ref)
                continue
            rep["parsed"] += 1
            put(ref, h["_source"].get("timestamp"), resp, "parsed")
        # Q3: raw fragment hits
        hits, total = hits_of(d)
        if total > len(hits):
            rep["warn"].append("%s: hits total=%d > returned=%d → split heads / narrow range" % (f, total, len(hits)))
        for h in hits:
            if h.get("_id") in seen or "message" not in (h.get("_source") or {}):
                continue
            seen.add(h.get("_id"))
            frag_docs.append(h["_source"])

    for ref, ts, raw, n, c in stitch(frag_docs, rep):
        try:
            ad = json.loads(raw)["maximusLogDetails"]["additionalDetails"]
            resp = build(ad["payload"], ad.get("eventType"))
        except (ValueError, KeyError, TypeError) as e:
            rep["parseFailed"].append("%s container=%s ts=%s chunks=%d len=%d: %s" % (ref, c[:12], ts, n, len(raw), str(e)[:80]))
            continue
        rep["fragments"] += 1
        if n >= 4:  # ≥2 middle chunks with equal ts: order not guaranteed
            rep["warn"].append("%s: chunks=%d parsed OK, spot-check" % (ref, n))
        put(ref, ts, resp, "fragments")

    rep["parseFailed"] = [x for x in rep["parseFailed"] if x.split(" ")[0] not in picked]

    out = {ref: ejson(picked[ref][1]) for ref in sorted(picked)}
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    log = sys.stderr
    by_src = {}
    for ref in picked:
        by_src[picked[ref][2]] = by_src.get(picked[ref][2], 0) + 1
    print("written %s: %d apps %s" % (a.out, len(out), json.dumps(by_src)), file=log)
    print("read: parsed=%d fragments(stitched)=%d fragmentDocs=%d" % (rep["parsed"], rep["fragments"], len(frag_docs)), file=log)
    rep["incomplete"] = [x for x in rep["incomplete"] if x.split(" ")[0] not in picked]
    for k in ("warn", "invalid", "parseFailed", "incomplete", "orphanTails"):
        print("%s (%d)%s" % (k, len(rep[k]), "".join("\n  " + x for x in rep[k])), file=log)

    if a.compare:
        old = json.load(open(a.compare, encoding="utf-8"))
        same = [r for r in out if r in old and old[r] == out[r]]
        changed = [r for r in out if r in old and old[r] != out[r]]
        new = [r for r in out if r not in old]
        print("compare vs %s: same=%d changed=%d new=%d" % (a.compare, len(same), len(changed), len(new)), file=log)
        print("  changed: %s\n  new: %s" % (changed, new), file=log)

    sys.exit(1 if rep["invalid"] or rep["parseFailed"] or rep["incomplete"] else 0)


def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    fq = sp.add_parser("frag-query", help="Q2 heads response → Q3 fragments query (Kibana)")
    fq.add_argument("heads")
    cv = sp.add_parser("convert", help="Q1 parsed + Q3 fragments responses → responses json")
    cv.add_argument("es_response", nargs="+")
    cv.add_argument("-o", "--out", required=True)
    cv.add_argument("--compare", help="existing responses json → same/changed/new")
    a = ap.parse_args()
    cmd_frag_query(a) if a.cmd == "frag-query" else cmd_convert(a)


if __name__ == "__main__":
    main()
