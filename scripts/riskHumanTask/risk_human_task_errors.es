# RiskHumanTask_FINISHED consumer errors (decision != APPROVED)
# v3: no scripted_metric (v2 hit "BytesStreamOutput cannot hold more than 2GB" → shard state = all fragments of the day).
#   Q1 parsed   : parsed log lines → by_app.latest.riskHumanTaskResponse           → save parsed.json
#   Q2 heads    : unparsed first chunks of consumer-error lines (container, ts)     → save heads.json
#   Q3 fragments: generated → python3 es_to_responses.py frag-query heads.json      → save fragments_1.json ...
#   convert     : python3 es_to_responses.py convert parsed.json fragments_*.json -o new_responses.json
# Time range: keep Q1 and Q2 identical.
# Async: POST → copy "id" → GET _async_search/<id> until "is_running": false → copy → DELETE _async_search/<id>

# ---------------------------------------------------------------- Q1 parsed
POST personal-loan-prod-*/_async_search?wait_for_completion_timeout=30s&keep_on_completion=true&keep_alive=1h
{
  "size": 0,
  "track_total_hits": true,
  "query": {
    "bool": {
      "filter": [
        { "range": { "@timestamp": { "gte": "now-24h", "lte": "now" } } },
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
}

# ---------------------------------------------------------------- Q2 heads (no scripts)
POST personal-loan-prod-*/_async_search?wait_for_completion_timeout=30s&keep_on_completion=true&keep_alive=1h
{
  "size": 10000,
  "track_total_hits": true,
  "_source": ["docker.container_id", "@timestamp"],
  "query": {
    "bool": {
      "filter": [
        { "range": { "@timestamp": { "gte": "now-24h", "lte": "now" } } },
        { "match_phrase": { "kubernetes.container_name": "personal-loan-orchestrator" } },
        { "exists": { "field": "docker.container_id" } },
        { "match_phrase": { "message": "com.axis.lending.commons.event.stepComplete.ApplicationStepCompleteFilterEnabledConsumer" } },
        { "match_phrase": { "message": "Error generated while processing Step Completion on Filter Enabled consumer" } }
      ],
      "must_not": [
        { "exists": { "field": "logger" } }
      ]
    }
  }
}

GET _async_search/<id>

DELETE _async_search/<id>
