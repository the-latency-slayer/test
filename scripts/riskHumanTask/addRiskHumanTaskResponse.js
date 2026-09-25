//############ Description:
//############ RCA is still pending
//########### Fix: ∀ app | applicationReferenceId ∈ keys(JSON) ∧ riskHumanTaskResponse.stepStatus = TRIGGERED:
//###########   1) push applicationStateLogs {_id:'RiskHumanTask', status:'FINISHED', input, output = JSON value}
//###########   2) set riskHumanTaskResponse = JSON value (stepStatus FINISHED) → journey moves ahead.
//########### Is this tested locally and Pre/Post count of records & result verified : YES (mongo 4.0.28: 10 eligible + 10 non-eligible, forced failures isolated)
//
//########### Expected Number of records to get updated : ≤ keys in JSON (only riskHumanTaskResponse.stepStatus = TRIGGERED)
//
//########### is permanent fixed planned : NA
//
//########### Status : OPEN
//
//########### Per-app try/catch: failure of 1 app is logged under FAILED; remaining apps continue.
//########### Legacy mongo shell 4.0.x compatible (ES5, cat(), no require/EJSON):
//###########   mongo "<uri>" --eval "var JSON_PATH='/path/on/server/rem_human_task_responses.json'" addRiskHumanTaskResponse.js
//###########   (JSON_PATH optional; default below)
//
// ################################################ Actual Script Start ####################################################################
var t1 = Date.now();

if (typeof JSON_PATH === 'undefined') var JSON_PATH = '/Users/vaibhav.bishnoi/maximus-scripts/Prod/2026/rem_human_task_responses.json';
var BACKUP_COLLECTION = 'riskHumanTaskResponse_backup_20260925_rem';

db = db.getSiblingDB('orchestration');
var personalCollection = db.getCollection('personal-applications');
var backupCollection = db.getCollection(BACKUP_COLLECTION);

// canonical EJSON wrappers → shell BSON types
function fromEjson(v) {
    if (v === null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(fromEjson);
    var k = Object.keys(v);
    if (k.length === 1) {
        if (k[0] === '$numberInt') return NumberInt(v.$numberInt);
        if (k[0] === '$numberLong') return NumberLong(v.$numberLong);
        if (k[0] === '$numberDouble') return parseFloat(v.$numberDouble);
        if (k[0] === '$numberDecimal') return NumberDecimal(v.$numberDecimal);
        if (k[0] === '$oid') return ObjectId(v.$oid);
        if (k[0] === '$date') return new Date(typeof v.$date === 'object' ? Number(v.$date.$numberLong) : v.$date);
    }
    var o = {};
    k.forEach(function (x) { o[x] = fromEjson(v[x]); });
    return o;
}

// raw EJSON kept; per-app conversion inside try → one bad value can't abort the run
var rawResponses = JSON.parse(cat(JSON_PATH));
var applicationReferenceIds = Object.keys(rawResponses);

function countByStatus(st) {
    return personalCollection.find({
        applicationReferenceId: { $in: applicationReferenceIds },
        'riskHumanTaskResponse.stepStatus': st
    }).count();
}

function buildRiskHumanTaskResponse(r, existing) {
    return {
        uniqueIdentifier: (existing && existing.uniqueIdentifier) ? existing.uniqueIdentifier : r.uniqueIdentifier,
        stepStatus: 'FINISHED',
        applicationId: r.applicationId,
        decision: r.decision,
        tasks: r.tasks,
        _class: 'HumanTaskResponse'
    };
}

function buildStateLog(r, now) {
    return {
        _id: 'RiskHumanTask',
        status: 'FINISHED',
        input: { applicationId: r.applicationId, decision: r.decision, tasks: r.tasks },
        output: { applicationId: r.applicationId, decision: r.decision, tasks: r.tasks, _class: 'HumanTaskResponse' },
        stepExecutionTimeInMillis: '11',
        timestamp: new Date(now).toISOString()
    };
}

function errMsg(e) {
    if (e == null) return String(e);
    return e.errmsg || e.message || tojsononeline(e);
}

var summary = { notFound: [], idMismatch: [], notTriggered: [], changedMeanwhile: [], failed: [], updated: [] };

print('Total responses in file: ' + applicationReferenceIds.length);
print('JSON_PATH: ' + JSON_PATH);
print('Pre count riskHumanTaskResponse TRIGGERED: ' + countByStatus('TRIGGERED') + ', FINISHED: ' + countByStatus('FINISHED'));

var apps = {};
personalCollection.find(
    { applicationReferenceId: { $in: applicationReferenceIds } },
    { _id: 1, applicationReferenceId: 1, riskHumanTaskResponse: 1 }
).forEach(function (a) { apps[a.applicationReferenceId] = a; });

function processOne(ref) {
    var app = apps[ref];
    if (app == null) { summary.notFound.push(ref); return; }

    var r = fromEjson(rawResponses[ref]);
    if (!r || !r.applicationId || !r.decision || !r.tasks) throw new Error('invalid JSON value (applicationId/decision/tasks missing)');
    if (app._id.str !== r.applicationId) { summary.idMismatch.push(ref); return; }

    var existing = app.riskHumanTaskResponse;
    if (!existing || existing.stepStatus !== 'TRIGGERED') {
        summary.notTriggered.push(ref + ':' + (existing ? existing.stepStatus : 'MISSING'));
        return;
    }

    var now = Date.now();
    var log = buildStateLog(r, now);
    var backupId = ObjectId();
    backupCollection.insertOne({
        _id: backupId,
        applicationReferenceId: ref,
        applicationId: app._id,
        oldRiskHumanTaskResponse: existing,
        pushedLogTimestamp: log.timestamp,
        backedUpAt: new Date(now)
    });
    try {
        var res = personalCollection.updateOne(
            { _id: app._id, applicationReferenceId: ref, 'riskHumanTaskResponse.stepStatus': 'TRIGGERED' },
            {
                $set: { riskHumanTaskResponse: buildRiskHumanTaskResponse(r, existing) },
                $push: { applicationStateLogs: log }
            }
        );
    } catch (e) {
        backupCollection.deleteOne({ _id: backupId });
        throw e;
    }
    if (res.matchedCount !== 1) {
        backupCollection.deleteOne({ _id: backupId });
        summary.changedMeanwhile.push(ref);
        return;
    }
    summary.updated.push(ref);
}

// ∀ ref: isolated try/catch → failure of one ≠ effect on others
applicationReferenceIds.forEach(function (ref) {
    try {
        processOne(ref);
    } catch (e) {
        summary.failed.push(ref + ': ' + errMsg(e));
    }
});

print('Not found (' + summary.notFound.length + '): ' + tojsononeline(summary.notFound));
print('applicationId mismatch (' + summary.idMismatch.length + '): ' + tojsononeline(summary.idMismatch));
print('Not TRIGGERED, skipped (' + summary.notTriggered.length + '): ' + tojsononeline(summary.notTriggered));
print('Status changed before write, skipped (' + summary.changedMeanwhile.length + '): ' + tojsononeline(summary.changedMeanwhile));
print('FAILED, not updated (' + summary.failed.length + '): ' + tojsononeline(summary.failed));
print('Updated (' + summary.updated.length + '): ' + tojsononeline(summary.updated));
print('Post count riskHumanTaskResponse TRIGGERED: ' + countByStatus('TRIGGERED') + ', FINISHED: ' + countByStatus('FINISHED'));

var t2 = Date.now();
print('Time took in milliseconds --> ' + (t2 - t1));

// Rollback per backup doc b:
//   $set riskHumanTaskResponse = b.oldRiskHumanTaskResponse (or $unset if null)
//   $pull applicationStateLogs { _id: 'RiskHumanTask', status: 'FINISHED', timestamp: b.pushedLogTimestamp }
// ################################################ Actual Script End  #####################################################################
