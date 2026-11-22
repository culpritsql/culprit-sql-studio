CREATE OR REPLACE VIEW $NPublished SQL SECURITY INVOKER AS
SELECT * FROM CulpritPublishedNamespaceV1(contract=$CONTRACT,store=$STORE,captured_from=$FROM,asof={asof:DateTime64(9, 'UTC')});

CREATE OR REPLACE VIEW $NHeaders SQL SECURITY INVOKER AS
SELECT *,JSONExtractRaw(SourceDescription,'observation','data') AS Header,
    JSONExtractString(Header,'display','connection_alias') AS Alias,JSONExtractString(Header,'display','database_name') AS DatabaseName,
    JSONExtractString(Header,'display','environment') AS Environment
FROM $NPublished(asof={asof:DateTime64(9, 'UTC')}) WHERE JSONExtractString(SourceDescription,'observation','kind')='derived-diagnosis';

CREATE OR REPLACE VIEW $NMatches SQL SECURITY INVOKER AS
SELECT DISTINCT ResourceId FROM $NHeaders(asof={asof:DateTime64(9, 'UTC')})
WHERE Alias={resource:String} OR (JSONHas(Header,'display','database_name') AND JSONExtractRaw(Header,'display','database_name')!='null' AND DatabaseName={resource:String});

CREATE OR REPLACE VIEW $NIncidentRows SQL SECURITY INVOKER AS
SELECT * FROM CulpritIncidentsV1NamespaceRows(contract=$CONTRACT,store=$STORE,captured_from=$FROM,asof={asof:DateTime64(9, 'UTC')});

CREATE OR REPLACE VIEW $NFindingRows SQL SECURITY INVOKER AS
SELECT * FROM CulpritFindingsV1NamespaceRows(contract=$CONTRACT,store=$STORE,captured_from=$FROM,asof={asof:DateTime64(9, 'UTC')});

CREATE OR REPLACE VIEW $NChangeRows SQL SECURITY INVOKER AS
SELECT * FROM CulpritChangesV1NamespaceRows(contract=$CONTRACT,store=$STORE,captured_from=$FROM,asof={asof:DateTime64(9, 'UTC')});

CREATE OR REPLACE VIEW $NCompleteness SQL SECURITY INVOKER AS
WITH OutputRows AS (
    SELECT RangeId,'incidents' AS Family FROM $NIncidentRows(asof={asof:DateTime64(9, 'UTC')})
    UNION ALL SELECT RangeId,'findings' AS Family FROM $NFindingRows(asof={asof:DateTime64(9, 'UTC')})
    UNION ALL SELECT RangeId,'changes' AS Family FROM $NChangeRows(asof={asof:DateTime64(9, 'UTC')})
), Counts AS (
    SELECT RangeId,countIf(Family='incidents') AS IncidentRows,countIf(Family='findings') AS FindingRows,countIf(Family='changes') AS ChangeRows
    FROM OutputRows GROUP BY RangeId
)
SELECT H.*,ifNull(C.IncidentRows,0)=JSONExtractUInt(H.Header,'counts','incidents') AS IncidentRowsComplete,
    ifNull(C.IncidentRows,0)=JSONExtractUInt(H.Header,'counts','incidents') AND ifNull(C.FindingRows,0)=JSONExtractUInt(H.Header,'counts','findings') AND ifNull(C.ChangeRows,0)=JSONExtractUInt(H.Header,'counts','changes') AS OutputRowsComplete
FROM $NHeaders(asof={asof:DateTime64(9, 'UTC')}) AS H
ALL LEFT JOIN Counts AS C ON H.RangeId=C.RangeId;

CREATE OR REPLACE VIEW $NResources SQL SECURITY INVOKER AS
SELECT SourceId,LogicalSourceId,Engine,Alias,DatabaseName,toBool(JSONHas(Header,'display','database_name') AND JSONExtractRaw(Header,'display','database_name')!='null') AS DatabaseNameKnown,Environment,toBool(JSONHas(Header,'display','environment') AND JSONExtractRaw(Header,'display','environment')!='null') AS EnvironmentKnown,CollectedFrom,CollectedUntil,
    JSONExtractRaw(SourceDescription,'partition') AS SourcePartition,JSONExtractString(Header,'observed_from') AS OriginalObservedFrom,JSONExtractString(Header,'observed_until') AS OriginalObservedUntil,
    JSONExtractString(Header,'rules_version') AS RulesVersion,JSONExtractRaw(Header,'analyzer') AS Analyzer,
    JSONExtractBool(Header,'input_read_complete') AS InputReadComplete,OutputRowsComplete,JSONExtractRaw(Header,'output_coverage') AS OutputCoverage,
    JSONExtractRaw(Header,'counts') AS Counts,JSONExtractRaw(Header,'context_issues') AS ContextIssues
FROM $NCompleteness(asof={asof:DateTime64(9, 'UTC')});

CREATE OR REPLACE VIEW $NConsistentOriginals SQL SECURITY INVOKER AS
WITH Originals AS (
    SELECT *,uniqExact((RevisionId,OriginalRecordedAt,Lifecycle,OriginalReference)) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,Revision) AS OriginalVersions,
        max(Revision) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS HeadRevision,
        uniqExactIf((RevisionId,OriginalRecordedAt,Lifecycle,OriginalReference),Revision=toDecimal128(1,0)) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS FirstOriginalVersions,
        countIf(Revision=toDecimal128(1,0) AND RevisionId!='' AND isNotNull(OriginalRecordedAt) AND Lifecycle IN ('active','resolved','uncertain') AND OriginalReference!='' AND OriginalReference!='null' AND OriginalReference!='{}') OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS FirstValidRows,
        anyIf(OriginalRecordedAt,Revision=toDecimal128(1,0)) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS FirstOriginalClock
    FROM $NIncidentRows(asof={asof:DateTime64(9, 'UTC')})
)
SELECT * EXCEPT (OriginalVersions,FirstOriginalVersions,FirstValidRows,FirstOriginalClock),
    if(FirstOriginalVersions=1 AND FirstValidRows>0,FirstOriginalClock,NULL) AS FirstRecordedObservation
FROM Originals WHERE OriginalVersions=1
AND RevisionId!='' AND isNotNull(OriginalRecordedAt) AND Lifecycle IN ('active','resolved','uncertain') AND OriginalReference!='' AND OriginalReference!='null' AND OriginalReference!='{}';

CREATE OR REPLACE VIEW $NHeadBasis SQL SECURITY INVOKER AS
WITH CompleteResources AS (SELECT ResourceId FROM $NCompleteness(asof={asof:DateTime64(9, 'UTC')}) GROUP BY ResourceId HAVING countIf(NOT IncidentRowsComplete)=0),
Originals AS (SELECT * FROM $NConsistentOriginals(asof={asof:DateTime64(9, 'UTC')}))
SELECT * EXCEPT (HeadRevision) FROM Originals WHERE Revision=HeadRevision AND ResourceId IN (SELECT ResourceId FROM CompleteResources);

CREATE OR REPLACE VIEW $NHeads SQL SECURITY INVOKER AS
WITH Pinned AS (SELECT *,max(RecordedAt) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS HeadRecordedAt
    FROM $NHeadBasis(asof={asof:DateTime64(9, 'UTC')}) WHERE ($ANALYZER_MATCH) AND RulesVersion=$RULES),
Latest AS (SELECT * FROM Pinned WHERE RecordedAt=HeadRecordedAt),
Bodies AS (SELECT *,uniqExact((AnalysisId,DetailSha256,InputReadComplete,OutputCoverage,OutputLimitations,FindingEvaluations,ChangeEvaluations,ContextIssues,OriginalObservedFrom,OriginalObservedUntil,InputReferences,AnalysisOrigin,Language)) OVER (PARTITION BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) AS BodyVersions FROM Latest)
SELECT * EXCEPT (HeadRecordedAt,BodyVersions) FROM Bodies WHERE BodyVersions=1
ORDER BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,RangeId,BatchId,RowOrdinal LIMIT 1 BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion;

CREATE OR REPLACE VIEW $NIncidents SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches
SELECT ConnectionAlias,DatabaseName,toBool(isNotNull(DatabaseName)) AS DatabaseNameKnown,Environment,toBool(isNotNull(Environment)) AS EnvironmentKnown,Title,Lifecycle,EnrichmentStatus,VictimSummary,AffectedQueries,CulpritQueries,CandidateQueries,AttributionStatus,Mechanism,toBool(isNotNull(Mechanism)) AS MechanismKnown,DeveloperAction,Verification,Caller,OriginalRecordedAt,OriginalObservedFrom,OriginalObservedUntil,OriginalIntervalFrom,OriginalIntervalUntil,OriginalScope,OriginalScopeId,OriginalSource,TargetId,EpisodeId,ControllerVersion,Revision,RevisionId,Details,RecordedAt AS InterpretationRecordedAt,RulesVersion,Analyzer,InputReadComplete,OutputCoverage,OutputLimitations,ContextIssues FROM $NHeads(asof={asof:DateTime64(9, 'UTC')})
WHERE has(Matches,ResourceId) AND length(Matches)=1 AND {from:DateTime64(9, 'UTC')}>=$FROM AND {from:DateTime64(9, 'UTC')}<{until:DateTime64(9, 'UTC')} AND {until:DateTime64(9, 'UTC')}<={asof:DateTime64(9, 'UTC')}
AND ((OriginalObservedFrom<{until:DateTime64(9, 'UTC')} AND OriginalObservedUntil>={from:DateTime64(9, 'UTC')})
 OR (OriginalIntervalFrom<{until:DateTime64(9, 'UTC')} AND OriginalIntervalUntil>={from:DateTime64(9, 'UTC')})
 OR (OriginalRecordedAt>={from:DateTime64(9, 'UTC')} AND OriginalRecordedAt<{until:DateTime64(9, 'UTC')}));

CREATE OR REPLACE VIEW $NIncidentHistory SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches
SELECT ConnectionAlias,DatabaseName,toBool(isNotNull(DatabaseName)) AS DatabaseNameKnown,Environment,toBool(isNotNull(Environment)) AS EnvironmentKnown,Title,Lifecycle,EnrichmentStatus,VictimSummary,AffectedQueries,CulpritQueries,CandidateQueries,AttributionStatus,Mechanism,toBool(isNotNull(Mechanism)) AS MechanismKnown,DeveloperAction,Verification,Caller,OriginalRecordedAt,OriginalObservedFrom,OriginalObservedUntil,OriginalIntervalFrom,OriginalIntervalUntil,OriginalScope,OriginalScopeId,OriginalSource,TargetId,EpisodeId,ControllerVersion,Revision,RevisionId,Details,RecordedAt AS InterpretationRecordedAt,RulesVersion,Analyzer,InputReadComplete,OutputCoverage,OutputLimitations,ContextIssues FROM $NIncidentRows(asof={asof:DateTime64(9, 'UTC')})
WHERE has(Matches,ResourceId) AND length(Matches)=1 AND {from:DateTime64(9, 'UTC')}>=$FROM AND {from:DateTime64(9, 'UTC')}<{until:DateTime64(9, 'UTC')} AND {until:DateTime64(9, 'UTC')}<={asof:DateTime64(9, 'UTC')} AND ($ANALYZER_MATCH) AND RulesVersion=$RULES
AND OriginalRecordedAt>={from:DateTime64(9, 'UTC')} AND OriginalRecordedAt<{until:DateTime64(9, 'UTC')};

CREATE OR REPLACE VIEW $NFindings SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches
SELECT ConnectionAlias,DatabaseName,toBool(isNotNull(DatabaseName)) AS DatabaseNameKnown,Environment,toBool(isNotNull(Environment)) AS EnvironmentKnown,Title,Summary,FindingId,CheckId,Severity,Actionability,Confidence,Subject,Impact,Metrics,AffectedQueries,OriginalQuery,EstablishedCause,DeveloperAction,Verification,Caveats,Inactive,Guidance,OriginalObservedFrom,OriginalObservedUntil,SourceId,LogicalSourceId,Details,RecordedAt AS InterpretationRecordedAt,RulesVersion,Analyzer,FindingEvaluations,InputReadComplete,OutputCoverage,OutputLimitations,ContextIssues FROM $NFindingRows(asof={asof:DateTime64(9, 'UTC')})
WHERE has(Matches,ResourceId) AND length(Matches)=1 AND {from:DateTime64(9, 'UTC')}>=$FROM AND {from:DateTime64(9, 'UTC')}<{until:DateTime64(9, 'UTC')} AND {until:DateTime64(9, 'UTC')}<={asof:DateTime64(9, 'UTC')} AND ($ANALYZER_MATCH) AND RulesVersion=$RULES
AND OriginalObservedFrom<{until:DateTime64(9, 'UTC')} AND OriginalObservedUntil>={from:DateTime64(9, 'UTC')};

CREATE OR REPLACE VIEW $NChanges SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches
SELECT ConnectionAlias,DatabaseName,toBool(isNotNull(DatabaseName)) AS DatabaseNameKnown,Environment,toBool(isNotNull(Environment)) AS EnvironmentKnown,Title,Detail,toBool(isNotNull(Detail)) AS DetailKnown,ChangeId,ChangeKind,OriginalSource,Action,toBool(isNotNull(Action)) AS ActionKnown,ChangedAt,ChangedAfter,toBool(isNotNull(ChangedAfter)) AS ChangedAfterKnown,ChangedUntil,toBool(isNotNull(ChangedUntil)) AS ChangedUntilKnown,TableName,toBool(isNotNull(TableName)) AS TableNameKnown,ObjectName,toBool(isNotNull(ObjectName)) AS ObjectNameKnown,OriginalObservedFrom,OriginalObservedUntil,SourceId,LogicalSourceId,Details,RecordedAt AS InterpretationRecordedAt,RulesVersion,Analyzer,ChangeEvaluations,InputReadComplete,OutputCoverage,OutputLimitations,ContextIssues FROM $NChangeRows(asof={asof:DateTime64(9, 'UTC')})
WHERE has(Matches,ResourceId) AND length(Matches)=1 AND {from:DateTime64(9, 'UTC')}>=$FROM AND {from:DateTime64(9, 'UTC')}<{until:DateTime64(9, 'UTC')} AND {until:DateTime64(9, 'UTC')}<={asof:DateTime64(9, 'UTC')} AND ($ANALYZER_MATCH) AND RulesVersion=$RULES
AND OriginalObservedFrom<{until:DateTime64(9, 'UTC')} AND OriginalObservedUntil>={from:DateTime64(9, 'UTC')};

CREATE OR REPLACE VIEW $NNewIncidents SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches,
Incomplete AS (SELECT count() AS Count FROM $NCompleteness(asof={asof:DateTime64(9, 'UTC')}) WHERE has(Matches,ResourceId) AND NOT OutputRowsComplete),
Heads AS (SELECT * FROM $NHeads(asof={asof:DateTime64(9, 'UTC')}) WHERE has(Matches,ResourceId) AND length(Matches)=1 AND {from:DateTime64(9, 'UTC')}>=$FROM AND {from:DateTime64(9, 'UTC')}<{until:DateTime64(9, 'UTC')} AND {until:DateTime64(9, 'UTC')}<={asof:DateTime64(9, 'UTC')}
    AND (SELECT Count FROM Incomplete)=0 AND Lifecycle='active' AND InputReadComplete AND JSONExtractString(OutputCoverage,'incidents','state')='evaluated'
    AND FirstRecordedObservation>={from:DateTime64(9, 'UTC')} AND FirstRecordedObservation<{until:DateTime64(9, 'UTC')})
SELECT H.EpisodeId,H.ControllerVersion,H.OriginalScopeId,H.OriginalSource,H.TargetId,H.FirstRecordedObservation,
    H.ConnectionAlias,H.DatabaseName,H.Title,H.VictimSummary,H.AffectedQueries,H.CulpritQueries,H.CandidateQueries,H.AttributionStatus,H.Mechanism,toBool(isNotNull(H.Mechanism)) AS MechanismKnown,H.DeveloperAction,H.Verification,H.Caller,
    H.OriginalRecordedAt,H.OriginalObservedFrom,H.OriginalObservedUntil,H.Revision,H.RevisionId,H.RecordedAt AS InterpretationRecordedAt,H.OutputLimitations,H.ContextIssues
FROM Heads AS H;

CREATE OR REPLACE VIEW $NStatus SQL SECURITY INVOKER AS
WITH (SELECT groupArray(ResourceId) FROM $NMatches(resource={resource:String},asof={asof:DateTime64(9, 'UTC')})) AS Matches,
N AS (SELECT length(Matches) AS Count),
Headers AS (SELECT *,max(CollectedUntil) OVER (PARTITION BY ResourceId,SourceId,LogicalSourceId) AS LastAcquired
    FROM $NCompleteness(asof={asof:DateTime64(9, 'UTC')}) WHERE has(Matches,ResourceId) AND length(Matches)=1),
OriginalHeads AS (SELECT ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion FROM $NIncidentRows(asof={asof:DateTime64(9, 'UTC')}) WHERE has(Matches,ResourceId) GROUP BY ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion),
(SELECT (count(),countIf(isNull(FirstRecordedObservation))) FROM $NHeads(asof={asof:DateTime64(9, 'UTC')}) WHERE has(Matches,ResourceId)) AS HeadCounts,
MissingHeads AS (SELECT (SELECT count() FROM OriginalHeads)-tupleElement(HeadCounts,1) AS Count)
SELECT multiIf({max_age_seconds:UInt32}=0,'invalid-max-age',isNull(parseDateTime64BestEffortOrNull(JSONExtractString(Header,'observed_until'),9,'UTC')),'unknown-clock',parseDateTime64BestEffortOrNull(JSONExtractString(Header,'observed_until'),9,'UTC')>{asof:DateTime64(9, 'UTC')},'clock-boundary',parseDateTime64BestEffortOrNull(JSONExtractString(Header,'observed_until'),9,'UTC')<{asof:DateTime64(9, 'UTC')}-toIntervalSecond({max_age_seconds:UInt32}),'stale',
    NOT ($HEADER_ANALYZER_MATCH) OR JSONExtractString(Header,'rules_version')!=$RULES,'rules-unavailable',NOT OutputRowsComplete,'incomplete-output',NOT JSONExtractBool(Header,'input_read_complete') OR length(JSONExtractArrayRaw(Header,'context_issues'))>0 OR JSONExtractString(Header,'output_coverage','incidents','state')!='evaluated' OR JSONExtractString(Header,'output_coverage','findings','state')!='evaluated' OR JSONExtractString(Header,'output_coverage','changes','state')!='evaluated','partial',(SELECT Count FROM MissingHeads)>0,'head-interpretation-unavailable',tupleElement(HeadCounts,2)>0,'first-observation-unavailable','observed') AS State,
    Alias,DatabaseName,SourceId,CollectedFrom,CollectedUntil,JSONExtractString(Header,'observed_from') AS OriginalObservedFrom,JSONExtractString(Header,'observed_until') AS OriginalObservedUntil,
    JSONExtractBool(Header,'input_read_complete') AS InputReadComplete,OutputRowsComplete,JSONExtractRaw(Header,'output_coverage') AS OutputCoverage,
    JSONExtractRaw(Header,'counts') AS Counts,JSONExtractRaw(Header,'context_issues') AS ContextIssues,ifNull((SELECT Count FROM MissingHeads),toUInt64(0)) AS UnselectedIncidentHeads,ifNull(tupleElement(HeadCounts,2),toUInt64(0)) AS UnavailableFirstObservations,'unknown' AS NativeHistoryCoverage
FROM Headers WHERE CollectedUntil=LastAcquired
UNION ALL SELECT multiIf((SELECT Count FROM N)=0,'unknown-label',(SELECT Count FROM N)!=1,'ambiguous-label','no-published-analysis') AS State,
    {resource:String} AS Alias,'' AS DatabaseName,'' AS SourceId,NULL AS CollectedFrom,NULL AS CollectedUntil,'' AS OriginalObservedFrom,'' AS OriginalObservedUntil,
    false AS InputReadComplete,false AS OutputRowsComplete,'null' AS OutputCoverage,'null' AS Counts,'[]' AS ContextIssues,0 AS UnselectedIncidentHeads,0 AS UnavailableFirstObservations,'unknown' AS NativeHistoryCoverage
WHERE (SELECT Count FROM N)!=1 OR (SELECT count() FROM Headers)=0;
