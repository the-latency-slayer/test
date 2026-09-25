#!/usr/bin/env python3
"""ES risk_human_task_errors response → {applicationReferenceId: riskHumanTaskResponse} canonical EJSON (rem_human_task_responses.json format)."""
import argparse, json, re, sys
from datetime import datetime

UW_KEYS = ("reason", "comment")
DROP_KEYS = {"blazeDeviations", "blazeRetriggeredByUW"}
EXCLUDE_DECISION = "APPROVED"
INT32 = 2 ** 31
OID = re.compile(r"^[0-9a-f]{24}$")


def load(path):
    raw = open(path, encoding="utf-8").read()
    # Kibana Dev Tools renders long strings as """...""" (raw, unescaped) → proper JSON strings
    raw = re.sub(r'"""(.*?)"""', lambda m: json.dumps(m.group(1)), raw, flags=re.S)
    d = json.loads(raw)
    return d.get("response", d)  # _async_search wraps result in "response"


def ts_key(s):
    # "2026-09-25T14:18:17.035545718+05:30" → aware datetime; ns → µs
    if not s:
        return datetime.min
    s = re.sub(r"(\.\d{6})\d+", r"\1", s).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.min


def build(p, event_type):
    """≡ painless script_field / fragments_to_response.jq."""
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("es_response", nargs="+", help="saved ES/Kibana response(s); later files win on same ref only if newer")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--compare", help="existing responses json (e.g. rem_human_task_responses.json) → report same/changed/new")
    a = ap.parse_args()

    picked, rep = {}, {"parsed": 0, "fragments": 0, "parseFailed": [], "invalid": [], "unresolved": [], "orphanTails": []}

    def put(ref, ts, resp, src):
        errs = validate(resp)
        if errs:
            rep["invalid"].append("%s(%s): %s" % (ref, src, ",".join(errs)))
            return
        prev = picked.get(ref)
        if prev is None or ts_key(ts) > ts_key(prev[0]):
            picked[ref] = (ts, resp, src)

    for f in a.es_response:
        aggs = load(f).get("aggregations", {})
        # parsed: by_app buckets (script_field already shaped)
        for b in (aggs.get("parsed", aggs).get("by_app") or {}).get("buckets", []):
            h = b["latest"]["hits"]["hits"][0]
            ref = h["_source"]["maximusLogDetails"]["applicationReferenceId"]
            resp = (h.get("fields", {}).get("riskHumanTaskResponse") or [None])[0]
            if resp is None:
                rep["invalid"].append("%s(parsed): no script field" % ref)
                continue
            rep["parsed"] += 1
            put(ref, h["_source"].get("timestamp"), resp, "parsed")
        # fragments: stitched rawLog
        rec = ((aggs.get("fragments") or {}).get("recovered") or {}).get("value") or {}
        for app in rec.get("apps", []):
            ref = app.get("applicationReferenceId")
            try:
                ad = json.loads(app["rawLog"])["maximusLogDetails"]["additionalDetails"]
                resp = build(ad["payload"], ad.get("eventType"))
            except (ValueError, KeyError, TypeError) as e:
                rep["parseFailed"].append("%s: %s" % (ref, str(e)[:120]))
                continue
            rep["fragments"] += 1
            if (app.get("chunks") or 0) >= 4:  # ≥2 middle chunks: relative order not guaranteed → verify
                rep["invalid"].append("%s(fragments): chunks=%s, verify manually" % (ref, app["chunks"]))
                continue
            put(ref, app.get("timestamp"), resp, "fragments")
        rep["unresolved"] += ["%s complete=%s len=%s container=%s ts=%s" % (u.get("applicationReferenceId"), u.get("complete"), u.get("rawLength"), (u.get("containerId") or "")[:12], u.get("timestamp")) for u in rec.get("unresolved", [])]
        rep["orphanTails"] += ["%s trace=%s" % (o.get("applicationReferenceId"), o.get("traceId")) for o in rec.get("orphanTails", [])]

    # a ref that was stitched successfully elsewhere is not unresolved
    rep["unresolved"] = [u for u in rep["unresolved"] if u.split(" ")[0] not in picked]

    out = {ref: ejson(picked[ref][1]) for ref in sorted(picked)}
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    log = sys.stderr
    by_src = {}
    for ref in picked:
        by_src[picked[ref][2]] = by_src.get(picked[ref][2], 0) + 1
    print("written %s: %d apps %s" % (a.out, len(out), json.dumps(by_src)), file=log)
    print("read: parsed=%d fragments=%d" % (rep["parsed"], rep["fragments"]), file=log)
    for k in ("invalid", "parseFailed", "unresolved", "orphanTails"):
        print("%s (%d)%s" % (k, len(rep[k]), "".join("\n  " + x for x in rep[k])), file=log)

    if a.compare:
        old = json.load(open(a.compare, encoding="utf-8"))
        same = [r for r in out if r in old and old[r] == out[r]]
        changed = [r for r in out if r in old and old[r] != out[r]]
        new = [r for r in out if r not in old]
        print("compare vs %s: same=%d changed=%d new=%d" % (a.compare, len(same), len(changed), len(new)), file=log)
        print("  changed: %s\n  new: %s" % (changed, new), file=log)

    sys.exit(1 if rep["invalid"] or rep["parseFailed"] else 0)


if __name__ == "__main__":
    main()
