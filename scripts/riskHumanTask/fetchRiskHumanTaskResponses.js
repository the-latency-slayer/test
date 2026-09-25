#!/usr/bin/env node
// Fetch riskHumanTaskResponse per applicationReferenceId from ES (parsed log → else stitched docker fragments)
// → canonical EJSON {ref: riskHumanTaskResponse}, ≤ BATCH refs per file (rem_human_task_responses.json format).
//
// Usage (Node ≥18, no deps):
//   node fetchRiskHumanTaskResponses.js refs.txt [outDir]
//   refs.txt: any text; every MLP\d{12} in it is used (one per line, JSON, logs … all fine)
// Transport (one of):
//   ES_URL=https://es-host:9200  ES_AUTH="Basic <b64>" | "ApiKey <key>"
//   KIBANA_URL=https://kibana-host  KIBANA_AUTH="Basic <b64>"  or  KIBANA_COOKIE="sid=..."   (Dev Tools console proxy)
// Optional: FROM=now-3d  TO=now  INDEX=personal-loan-prod-*  BATCH=150  CONCURRENCY=3
'use strict';
const fs = require('fs');
const path = require('path');

const E = process.env;
const INDEX = E.INDEX || 'personal-loan-prod-*';
const FROM = E.FROM || 'now-3d';
const TO = E.TO || 'now';
const BATCH = +(E.BATCH || 150);
const CONCURRENCY = +(E.CONCURRENCY || 3);
const LOGGER = 'com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer';
const ERR_MSG = 'Error generated while processing Step Completion on Filter Enabled consumer';
const EVENT = 'RiskHumanTask_FINISHED';
const EVENT_KV = '"eventType":"' + EVENT + '"';
const CONTAINER = 'personal-loan-orchestrator';
const UW_KEYS = ['reason', 'comment'];
const DROP_KEYS = new Set(['blazeDeviations', 'blazeRetriggeredByUW']);
const CHUNK = 16384;
const WINDOW_MS = 2000;
const HEAD_RE = /"logger"\s*:\s*"/;
const INT32 = 2 ** 31;

// ---------------------------------------------------------------- transport
async function search(body) {
  let url, headers = { 'Content-Type': 'application/json' };
  if (E.ES_URL) {
    url = E.ES_URL.replace(/\/$/, '') + '/' + INDEX + '/_search';
    if (E.ES_AUTH) headers.Authorization = E.ES_AUTH;
  } else if (E.KIBANA_URL) {
    url = E.KIBANA_URL.replace(/\/$/, '') + '/api/console/proxy?path=' + encodeURIComponent(INDEX + '/_search') + '&method=POST';
    headers['kbn-xsrf'] = 'true';
    if (E.KIBANA_AUTH) headers.Authorization = E.KIBANA_AUTH;
    if (E.KIBANA_COOKIE) headers.Cookie = E.KIBANA_COOKIE;
  } else throw new Error('set ES_URL or KIBANA_URL');
  for (let i = 0; ; i++) {
    try {
      const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
      const txt = await res.text();
      if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + txt.slice(0, 300));
      const d = JSON.parse(txt);
      if (d.error) throw new Error(JSON.stringify(d.error).slice(0, 300));
      return d;
    } catch (e) {
      if (i >= 3) throw e;
      await new Promise(r => setTimeout(r, 1000 * 2 ** i));
    }
  }
}

const range = (gte, lte, fmt) => ({ range: { '@timestamp': fmt ? { gte, lte, format: fmt } : { gte, lte } } });
const phrase = (f, v) => ({ match_phrase: { [f]: v } });

// ---------------------------------------------------------------- shaping (≡ painless script_field)
function build(p, eventType) {
  const tasks = {};
  for (const [k, t] of Object.entries(p.tasks || {})) {
    if (t == null) continue;
    const d = t.details || {};
    const ud = d.userDetails || {};
    const udOut = {};
    if (k === 'UNDERWRITING_DECISION') { for (const x of UW_KEYS) if (x in ud) udOut[x] = ud[x]; }
    else for (const [x, v] of Object.entries(ud)) if (!DROP_KEYS.has(x)) udOut[x] = v;
    tasks[k] = { status: t.status, details: { userDetails: udOut, lastModifiedByUser: d.lastModifiedByUser ?? null, lastModifiedOn: d.lastModifiedOn ?? null } };
  }
  return {
    uniqueIdentifier: 'UNAVAILABLE',
    stepStatus: (eventType || '').endsWith('_FINISHED') ? 'FINISHED' : eventType,
    applicationId: p.applicationId,
    decision: p.decision,
    tasks,
    _class: 'HumanTaskResponse',
  };
}

// int∈int32 → $numberInt, int∉int32 → $numberLong, fraction → $numberDouble (x.0 doubles become Int: JSON.parse loses it)
function ejson(v) {
  if (v === null || typeof v !== 'object') {
    if (typeof v !== 'number') return v;
    if (!Number.isInteger(v)) return { $numberDouble: String(v) };
    return v >= -INT32 && v < INT32 ? { $numberInt: String(v) } : { $numberLong: String(v) };
  }
  if (Array.isArray(v)) return v.map(ejson);
  const o = {};
  for (const k of Object.keys(v)) o[k] = ejson(v[k]);
  return o;
}

function validate(r) {
  const e = [];
  if (!/^[0-9a-f]{24}$/.test(String(r.applicationId || ''))) e.push('applicationId');
  if (!r.decision) e.push('decision');
  if (r.stepStatus !== 'FINISHED') e.push('stepStatus=' + r.stepStatus);
  if (!r.tasks || !Object.keys(r.tasks).length) e.push('tasks empty');
  return e;
}

// "2026-09-25T09:22:27.887996632+00:00" → BigInt epoch ns
function tsNs(s) {
  const m = /^(.*?T\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)?$/.exec(s || '');
  if (!m) return -1n;
  const ms = Date.parse(m[1] + (m[3] || 'Z'));
  if (isNaN(ms)) return -1n;
  return BigInt(ms) * 1000000n + BigInt(((m[2] || '') + '000000000').slice(0, 9));
}

// ---------------------------------------------------------------- per ref
async function fromParsed(ref) {
  const d = await search({
    size: 1,
    sort: [{ '@timestamp': { order: 'desc' } }],
    _source: ['maximusLogDetails.additionalDetails', 'timestamp'],
    query: { bool: { filter: [range(FROM, TO), phrase('logger', LOGGER), phrase('maximusLogDetails.additionalDetails.eventType', EVENT),
      phrase('message', ERR_MSG), phrase('maximusLogDetails.applicationReferenceId', ref)] } },
  });
  const h = (d.hits.hits || [])[0];
  const ad = h && h._source.maximusLogDetails && h._source.maximusLogDetails.additionalDetails;
  return ad && ad.payload ? { ts: h._source.timestamp, resp: build(ad.payload, ad.eventType), src: 'parsed' } : null;
}

function stitch(docs) {
  const fr = docs.map(s => {
    const m = s.message;
    const hm = HEAD_RE.exec(m.slice(0, 600));
    const head = m.startsWith('{') && hm && hm.index < 512;
    return { t: tsNs(s['@timestamp']), r: head ? 0 : (m.length >= CHUNK ? 1 : 2), m };
  }).sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : a.r - b.r));
  const recs = [];
  let cur = null;
  for (const f of fr) {
    if (f.r === 0) { if (cur) recs.push(cur); cur = [f.m]; }
    else if (cur) cur.push(f.m);
  }
  if (cur) recs.push(cur);
  return recs.map(p => ({ raw: p.join(''), chunks: p.length }));
}

async function fromFragments(ref) {
  // tail chunk carries applicationReferenceId (end of line)
  const tails = await search({
    size: 50,
    sort: [{ '@timestamp': { order: 'desc' } }],
    _source: ['docker.container_id', '@timestamp'],
    query: { bool: { filter: [range(FROM, TO), phrase('kubernetes.container_name', CONTAINER), phrase('message', ref)], must_not: [{ exists: { field: 'logger' } }] } },
  });
  let best = null;
  const problems = [];
  const seen = new Set();
  for (const h of tails.hits.hits || []) {
    const cid = h._source.docker && h._source.docker.container_id;
    const ms = Number(tsNs(h._source['@timestamp']) / 1000000n);
    if (!cid || ms < 0 || seen.has(cid + ms)) continue;
    seen.add(cid + ms);
    const w = await search({
      size: 500,
      _source: ['message', 'docker.container_id', '@timestamp'],
      query: { bool: { filter: [range(ms - WINDOW_MS, ms + WINDOW_MS, 'epoch_millis'), phrase('kubernetes.container_name', CONTAINER), phrase('docker.container_id', cid)], must_not: [{ exists: { field: 'logger' } }] } },
    });
    const docs = (w.hits.hits || []).map(x => x._source).filter(s => typeof s.message === 'string');
    for (const { raw, chunks } of stitch(docs)) {
      if (!raw.includes(ref) || !raw.includes(LOGGER) || !raw.includes(ERR_MSG) || !raw.includes(EVENT_KV)) continue;
      let log;
      try { log = JSON.parse(raw); } catch (e) { problems.push('parse ' + e.message.slice(0, 60) + ' chunks=' + chunks + ' len=' + raw.length); continue; }
      const ml = log.maximusLogDetails || {};
      if (ml.applicationReferenceId !== ref) continue;
      const ad = ml.additionalDetails || {};
      if (!ad.payload) continue;
      const cand = { ts: log.timestamp, resp: build(ad.payload, ad.eventType), src: 'fragments', chunks };
      if (!best || tsNs(cand.ts) > tsNs(best.ts)) best = cand;
    }
  }
  return best || (problems.length ? { problems } : null);
}

async function one(ref) {
  const p = await fromParsed(ref);
  const f = await fromFragments(ref).catch(e => ({ problems: ['fragments: ' + e.message] }));
  const c = [p, f && f.resp ? f : null].filter(Boolean);
  if (!c.length) return f && f.problems ? { ref, err: f.problems.join('; ') } : { ref, err: 'not found' };
  c.sort((a, b) => (tsNs(b.ts) > tsNs(a.ts) ? 1 : -1));
  const best = c[0];
  if (best.resp.decision === 'APPROVED') return { ref, skip: 'APPROVED' };
  const errs = validate(best.resp);
  return errs.length ? { ref, err: 'invalid: ' + errs.join(',') } : { ref, ...best };
}

// ---------------------------------------------------------------- main
(async () => {
  const [, , refsFile, outDir = '.'] = process.argv;
  if (!refsFile) { console.error('usage: node fetchRiskHumanTaskResponses.js refs.txt [outDir]'); process.exit(2); }
  const refs = [...new Set(fs.readFileSync(refsFile, 'utf8').match(/MLP\d{12}/g) || [])].sort();
  if (!refs.length) { console.error('no MLP refs in ' + refsFile); process.exit(2); }
  fs.mkdirSync(outDir, { recursive: true });
  console.error(`refs=${refs.length} range=${FROM}..${TO} via ${E.ES_URL ? 'ES' : 'Kibana'}`);

  const results = new Array(refs.length);
  let next = 0, done = 0;
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, refs.length) }, async () => {
    while (next < refs.length) {
      const i = next++;
      // ∀ ref isolated: one failure ≠ effect on others
      results[i] = await one(refs[i]).catch(e => ({ ref: refs[i], err: e.message }));
      if (++done % 10 === 0 || done === refs.length) console.error(`  ${done}/${refs.length}`);
    }
  }));

  const ok = results.filter(r => r.resp);
  const files = [];
  for (let i = 0; i < ok.length; i += BATCH) {
    const part = {};
    for (const r of ok.slice(i, i + BATCH)) part[r.ref] = ejson(r.resp);
    const f = path.join(outDir, `risk_human_task_responses_${files.length + 1}.json`);
    fs.writeFileSync(f, JSON.stringify(part, null, 2) + '\n');
    files.push(f + ' (' + Object.keys(part).length + ')');
  }
  const report = {
    requested: refs.length,
    found: ok.length,
    bySource: ok.reduce((a, r) => ((a[r.src] = (a[r.src] || 0) + 1), a), {}),
    files,
    skippedApproved: results.filter(r => r.skip).map(r => r.ref),
    failed: results.filter(r => r.err).map(r => r.ref + ': ' + r.err),
  };
  fs.writeFileSync(path.join(outDir, 'fetch_report.json'), JSON.stringify(report, null, 2) + '\n');
  console.error(JSON.stringify(report, null, 2));
  process.exit(report.failed.length ? 1 : 0);
})();
