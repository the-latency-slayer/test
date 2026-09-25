//############ Description:
//############ RCA is still pending
//########### Fix: set riskHumanTaskResponse (from JSON) as FINISHED + push RiskHumanTask TRIGGERED & FINISHED state logs → journey moves ahead.
//########### Is this tested locally and Pre/Post count of records & result verified : YES (mongo 4.0.28 local dry run)
//
//########### Expected Number of records to get updated : 535 (max; apps with riskHumanTaskResponse already FINISHED are skipped)
//
//########### is permanent fixed planned : NA
//
//########### Status : OPEN
//
//########### Legacy mongo shell 4.0.x compatible (ES5, cat(), no require/EJSON):
//###########   mongo "<uri>" addRiskHumanTaskResponse.js
//###########   set DRY_RUN = true to only print the plan, no writes.
//
// ################################################ Actual Script Start ####################################################################
var t1 = Date.now();

var DRY_RUN = false;
var JSON_PATH = '/Users/vaibhav.bishnoi/maximus-scripts/Prod/2026/rem_human_task_responses.json';
var BACKUP_COLLECTION = 'riskHumanTaskResponse_backup_20260925_rem';
var BATCH_SIZE = 100;

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

function loadResponses(path) {
    return fromEjson(JSON.parse(cat(path)));
}

var humanTaskResponses = loadResponses(JSON_PATH);
var applicationReferenceIds = Object.keys(humanTaskResponses);

function countFinished() {
    return personalCollection.find({
        applicationReferenceId: { $in: applicationReferenceIds },
        'riskHumanTaskResponse.stepStatus': 'FINISHED'
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

function buildStateLogs(r, now) {
    return [
        {
            _id: 'RiskHumanTask',
            status: 'TRIGGERED',
            stepExecutionTimeInMillis: '2',
            timestamp: new Date(now).toISOString()
        },
        {
            _id: 'RiskHumanTask',
            status: 'FINISHED',
            input: { applicationId: r.applicationId, decision: r.decision, tasks: r.tasks },
            output: { applicationId: r.applicationId, decision: r.decision, tasks: r.tasks, _class: 'HumanTaskResponse' },
            stepExecutionTimeInMillis: '11',
            timestamp: new Date(now + 1).toISOString()
        }
    ];
}

var summary = { notFound: [], idMismatch: [], alreadyFinished: [], hadRiskHumanTaskLog: [], updated: [], matchedCount: 0 };
var ops = [];
var backups = [];
var sampleOp = null;

function flush() {
    if (ops.length === 0) return;
    if (!DRY_RUN) {
        backupCollection.insertMany(backups, { ordered: false });
        var res = personalCollection.bulkWrite(ops, { ordered: false });
        summary.matchedCount += res.matchedCount;
    }
    ops = [];
    backups = [];
}

print('DRY_RUN: ' + DRY_RUN);
print('Total responses in file: ' + applicationReferenceIds.length);
print('Pre count riskHumanTaskResponse FINISHED: ' + countFinished());

var apps = {};
personalCollection.find(
    { applicationReferenceId: { $in: applicationReferenceIds } },
    { _id: 1, applicationReferenceId: 1, riskHumanTaskResponse: 1, 'applicationStateLogs._id': 1 }
).forEach(function (a) { apps[a.applicationReferenceId] = a; });

applicationReferenceIds.forEach(function (ref) {
    var r = humanTaskResponses[ref];
    var app = apps[ref];

    if (app == null) { summary.notFound.push(ref); return; }
    if (app._id.str !== r.applicationId) { summary.idMismatch.push(ref); return; }
    var existing = app.riskHumanTaskResponse;
    if (existing && existing.stepStatus === 'FINISHED') { summary.alreadyFinished.push(ref); return; }
    if ((app.applicationStateLogs || []).some(function (l) { return l._id === 'RiskHumanTask'; })) summary.hadRiskHumanTaskLog.push(ref);

    var now = Date.now();
    var logs = buildStateLogs(r, now);
    backups.push({
        applicationReferenceId: ref,
        applicationId: app._id,
        oldRiskHumanTaskResponse: existing === undefined ? null : existing,
        pushedLogTimestamps: logs.map(function (l) { return l.timestamp; }),
        backedUpAt: new Date(now)
    });
    ops.push({
        updateOne: {
            filter: { _id: app._id, applicationReferenceId: ref, 'riskHumanTaskResponse.stepStatus': { $ne: 'FINISHED' } },
            update: {
                $set: { riskHumanTaskResponse: buildRiskHumanTaskResponse(r, existing) },
                $push: { applicationStateLogs: { $each: logs } }
            }
        }
    });
    if (sampleOp === null) sampleOp = ops[ops.length - 1];
    summary.updated.push(ref);

    if (ops.length >= BATCH_SIZE) flush();
});
flush();

if (DRY_RUN && sampleOp !== null) print('Sample update: ' + tojson(sampleOp));
print('Not found (' + summary.notFound.length + '): ' + tojsononeline(summary.notFound));
print('applicationId mismatch (' + summary.idMismatch.length + '): ' + tojsononeline(summary.idMismatch));
print('Already FINISHED, skipped (' + summary.alreadyFinished.length + '): ' + tojsononeline(summary.alreadyFinished));
print('Had RiskHumanTask log before (' + summary.hadRiskHumanTaskLog.length + '): ' + tojsononeline(summary.hadRiskHumanTaskLog));
print((DRY_RUN ? 'Would update (' : 'Updated (') + summary.updated.length + '): ' + tojsononeline(summary.updated));
print('Result: matchedCount ' + summary.matchedCount); // 4.0 shell bulkWrite has no modifiedCount
print('Post count riskHumanTaskResponse FINISHED: ' + countFinished());

var t2 = Date.now();
print('Time took in milliseconds --> ' + (t2 - t1));

// Rollback per backup doc b:
//   $set riskHumanTaskResponse = b.oldRiskHumanTaskResponse (or $unset if null)
//   $pull applicationStateLogs { _id: 'RiskHumanTask', timestamp: { $in: b.pushedLogTimestamps } }
// ################################################ Actual Script End  #####################################################################
