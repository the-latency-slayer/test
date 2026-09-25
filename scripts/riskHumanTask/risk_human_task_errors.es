# RiskHumanTask_FINISHED consumer errors (decision != APPROVED), last 24h
#   aggs.parsed    -> by_app.latest.riskHumanTaskResponse (parsed log lines)
#   aggs.fragments -> docker-split (>16KB) lines, stitched per container
#                     recovered.value.apps[].rawLog  -> es_to_responses.py
#                     recovered.value.unresolved     -> head matched, stitched line incomplete -> manual check
#                     recovered.value.orphanTails    -> tail w/ appRef, head outside window/lost -> manual check
# v2: chunks of one docker line share @timestamp -> order by (container, @timestamp, head first, full 16KB chunk before last chunk)
#     fragments without docker.container_id excluded; non-consumer heads truncated (memory); chunk count per record
#
# Run async (Kibana proxy times out ~30s):
#   1. POST below -> copy "id"
#   2. GET _async_search/<id>        (repeat until "is_running": false)
#   3. DELETE _async_search/<id>
POST personal-loan-prod-*/_async_search?wait_for_completion_timeout=10s&keep_on_completion=true&keep_alive=1h
{
  "size": 0,
  "track_total_hits": true,
  "query": {
    "bool": {
      "filter": [
        { "range": { "@timestamp": { "gte": "now-24h", "lte": "now" } } }
      ],
      "should": [
        {
          "bool": {
            "filter": [
              { "match_phrase": { "logger": "com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer" } },
              { "match_phrase": { "maximusLogDetails.additionalDetails.eventType": "RiskHumanTask_FINISHED" } },
              { "match_phrase": { "message": "Error generated while processing Step Completion on Filter Enabled consumer" } }
            ]
          }
        },
        {
          "bool": {
            "filter": [
              { "match_phrase": { "kubernetes.container_name": "personal-loan-orchestrator" } },
              { "exists": { "field": "docker.container_id" } }
            ],
            "must_not": [
              { "exists": { "field": "logger" } }
            ]
          }
        }
      ],
      "minimum_should_match": 1
    }
  },
  "aggs": {
    "parsed": {
      "filter": {
        "bool": {
          "filter": [
            { "match_phrase": { "logger": "com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer" } },
            { "match_phrase": { "maximusLogDetails.additionalDetails.eventType": "RiskHumanTask_FINISHED" } },
            { "match_phrase": { "message": "Error generated while processing Step Completion on Filter Enabled consumer" } }
          ]
        }
      },
      "aggs": {
        "unique_app_ref_count": {
          "cardinality": {
            "script": {
              "lang": "painless",
              "source": """
                Map src = params['_source'];
                if (src == null) return null;
                Map mld = src.get('maximusLogDetails');
                if (mld == null) return null;
                Map ad = mld.get('additionalDetails');
                if (ad != null) {
                  Map p = ad.get('payload');
                  if (p != null && 'APPROVED'.equals(p.get('decision'))) return null;
                }
                def id = mld.get('applicationReferenceId');
                return id == null ? null : id.toString();
              """
            }
          }
        },
        "by_app": {
          "terms": {
            "size": 10000,
            "order": { "_key": "asc" },
            "script": {
              "lang": "painless",
              "source": """
                Map src = params['_source'];
                if (src == null) return null;
                Map mld = src.get('maximusLogDetails');
                if (mld == null) return null;
                Map ad = mld.get('additionalDetails');
                if (ad != null) {
                  Map p = ad.get('payload');
                  if (p != null && 'APPROVED'.equals(p.get('decision'))) return null;
                }
                def id = mld.get('applicationReferenceId');
                return id == null ? null : id.toString();
              """
            }
          },
          "aggs": {
            "latest": {
              "top_hits": {
                "size": 1,
                "sort": [{ "@timestamp": { "order": "desc" } }],
                "_source": ["maximusLogDetails.applicationReferenceId", "timestamp"],
                "script_fields": {
                  "riskHumanTaskResponse": {
                    "script": {
                      "lang": "painless",
                      "params": {
                        "uwKeys": ["reason", "comment"],
                        "dropKeys": ["blazeDeviations", "blazeRetriggeredByUW"]
                      },
                      "source": """
                        Map src = params['_source'];
                        if (src == null) return null;
                        Map mld = src.get('maximusLogDetails');
                        if (mld == null) return null;
                        Map ad = mld.get('additionalDetails');
                        if (ad == null || ad.get('payload') == null) return null;
                        Map p = ad.get('payload');

                        Map out = new LinkedHashMap();
                        out.put('uniqueIdentifier', 'UNAVAILABLE');
                        def et = ad.get('eventType');
                        out.put('stepStatus', (et != null && et.endsWith('_FINISHED')) ? 'FINISHED' : et);
                        out.put('applicationId', p.get('applicationId'));
                        out.put('decision', p.get('decision'));

                        Map tasksOut = new LinkedHashMap();
                        Map tasksIn = p.get('tasks');
                        if (tasksIn != null) {
                          for (def e : tasksIn.entrySet()) {
                            Map t = e.getValue();
                            if (t == null) continue;
                            Map d = t.get('details');
                            Map ud = (d == null || d.get('userDetails') == null) ? new HashMap() : d.get('userDetails');

                            Map udOut = new LinkedHashMap();
                            if (e.getKey() == 'UNDERWRITING_DECISION') {
                              for (String k : params.uwKeys) {
                                if (ud.containsKey(k)) udOut.put(k, ud.get(k));
                              }
                            } else {
                              for (def k : ud.keySet()) {
                                if (!params.dropKeys.contains(k)) udOut.put(k, ud.get(k));
                              }
                            }

                            Map dOut = new LinkedHashMap();
                            dOut.put('userDetails', udOut);
                            dOut.put('lastModifiedByUser', d == null ? null : d.get('lastModifiedByUser'));
                            dOut.put('lastModifiedOn', d == null ? null : d.get('lastModifiedOn'));

                            Map tOut = new LinkedHashMap();
                            tOut.put('status', t.get('status'));
                            tOut.put('details', dOut);
                            tasksOut.put(e.getKey(), tOut);
                          }
                        }
                        out.put('tasks', tasksOut);
                        out.put('_class', 'HumanTaskResponse');
                        return out;
                      """
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "fragments": {
      "filter": {
        "bool": {
          "filter": [
            { "match_phrase": { "kubernetes.container_name": "personal-loan-orchestrator" } },
            { "exists": { "field": "docker.container_id" } }
          ],
          "must_not": [
            { "exists": { "field": "logger" } }
          ]
        }
      },
      "aggs": {
        "recovered": {
          "scripted_metric": {
            "params": {
              "logger": "com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer",
              "errMsg": "Error generated while processing Step Completion on Filter Enabled consumer",
              "eventTypeKv": "\"eventType\":\"RiskHumanTask_FINISHED\"",
              "excludeDecision": "APPROVED",
              "chunkSize": 16384,
              "headKeep": 600
            },
            "init_script": "state.frags = new ArrayList();",
            "map_script": """
              Map src = params['_source'];
              if (src == null) return;
              def msg = src.get('message');
              if (msg == null) return;
              Map dk = src.get('docker');
              if (dk == null || dk.get('container_id') == null) return;
              String m = msg.toString();
              int lp = m.indexOf('"logger":"');
              boolean head = m.startsWith('{') && lp >= 0 && lp < 512;
              // rank: 0 head, 1 full chunk (middle), 2 short chunk (last) -> tie-break for equal @timestamp
              int rank = head ? 0 : (m.length() >= params.chunkSize ? 1 : 2);
              // head of another logger: keep only the boundary, drop the body
              if (head && m.indexOf(params.logger) < 0 && m.length() > params.headKeep) m = m.substring(0, params.headKeep);
              def ts = src.get('@timestamp');
              Map f = new HashMap();
              f.put('c', dk.get('container_id').toString());
              f.put('t', ts == null ? '' : ts.toString());
              f.put('r', rank);
              f.put('m', m);
              state.frags.add(f);
            """,
            "combine_script": "return state.frags;",
            "reduce_script": """
              String ext(String r, String k) {
                int p = r.indexOf(k);
                if (p < 0) return null;
                int s = p + k.length();
                int e = r.indexOf('"', s);
                return e < 0 ? null : r.substring(s, e);
              }

              List all = new ArrayList();
              for (def s : states) { if (s != null) all.addAll(s); }
              all.sort((a, b) -> {
                int c = ((String) a.get('c')).compareTo((String) b.get('c'));
                if (c != 0) return c;
                c = ((String) a.get('t')).compareTo((String) b.get('t'));
                if (c != 0) return c;
                return Integer.compare((int) a.get('r'), (int) b.get('r'));
              });

              List recs = new ArrayList();
              List orphans = new ArrayList();
              Map cur = null;
              StringBuilder buf = null;
              int n = 0;
              for (def f : all) {
                String m = (String) f.get('m');
                boolean head = ((int) f.get('r')) == 0;
                boolean sameC = cur != null && cur.get('c').equals(f.get('c'));
                if ((head || !sameC) && cur != null) {
                  cur.put('r', buf.toString());
                  cur.put('n', n);
                  recs.add(cur);
                  cur = null;
                  buf = null;
                }
                if (head) {
                  cur = new HashMap();
                  cur.put('c', f.get('c'));
                  cur.put('t', f.get('t'));
                  buf = new StringBuilder(m);
                  n = 1;
                } else if (cur != null) {
                  buf.append(m);
                  n++;
                } else if (m.contains(params.eventTypeKv)) {
                  Map o = new LinkedHashMap();
                  o.put('applicationReferenceId', ext(m, '"applicationReferenceId":"'));
                  o.put('traceId', ext(m, '"traceId":"'));
                  o.put('ingestTs', f.get('t'));
                  o.put('containerId', f.get('c'));
                  orphans.add(o);
                }
              }
              if (cur != null) { cur.put('r', buf.toString()); cur.put('n', n); recs.add(cur); }

              Map apps = new TreeMap();
              List unresolved = new ArrayList();
              for (def rec : recs) {
                String r = (String) rec.get('r');
                if (!r.contains(params.logger) || !r.contains(params.errMsg) || !r.contains(params.eventTypeKv)) continue;
                String ref = ext(r, '"applicationReferenceId":"');
                boolean complete = r.trim().endsWith('}');
                if (ref == null || !complete) {
                  Map u = new LinkedHashMap();
                  u.put('applicationReferenceId', ref);
                  u.put('traceId', ext(r, '"traceId":"'));
                  u.put('timestamp', ext(r, '"timestamp":"'));
                  u.put('ingestTs', rec.get('t'));
                  u.put('containerId', rec.get('c'));
                  u.put('complete', complete);
                  u.put('chunks', rec.get('n'));
                  u.put('rawLength', r.length());
                  unresolved.add(u);
                  continue;
                }
                String dec = ext(r, '"decision":"');
                if (params.excludeDecision.equals(dec)) continue;
                Map e = new LinkedHashMap();
                e.put('applicationReferenceId', ref);
                e.put('decision', dec);
                e.put('traceId', ext(r, '"traceId":"'));
                e.put('timestamp', ext(r, '"timestamp":"'));
                e.put('ingestTs', rec.get('t'));
                e.put('containerId', rec.get('c'));
                e.put('chunks', rec.get('n'));
                e.put('rawLog', r);
                Map prev = apps.get(ref);
                if (prev == null || ((String) prev.get('ingestTs')).compareTo((String) rec.get('t')) < 0) apps.put(ref, e);
              }

              List orphansOut = new ArrayList();
              for (def o : orphans) {
                if (o.get('applicationReferenceId') == null || !apps.containsKey(o.get('applicationReferenceId'))) orphansOut.add(o);
              }

              Map out = new LinkedHashMap();
              out.put('count', apps.size());
              out.put('fragmentsScanned', all.size());
              out.put('apps', new ArrayList(apps.values()));
              out.put('orphanTails', orphansOut);
              out.put('unresolved', unresolved);
              return out;
            """
          }
        }
      }
    }
  }
}

GET _async_search/<id>

DELETE _async_search/<id>
