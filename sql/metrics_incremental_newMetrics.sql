--================================================================================================================================================================
--NEW METRICS (Incremental Load) - DELSERT (Delete latest Batch, Reinsert from latest Batch)
--1. Identify the new data to be loaded to stage
--2. Delete out the records tied to the last Batch that was loaded
--3. Load from Stage to Target
--================================================================================================================================================================

--1. Stage Loads only for the latest Batch ID found in the existing set of Sessions identified
TRUNCATE TABLE metrics.Views_STG_new;
VACUUM metrics.Views_STG_new;
INSERT INTO metrics.Views_STG_new 
SELECT
    'newMetrics_Live'::text AS src,
    fact_views_live.batch_id,
    fact_views_live.row_id,
    fact_views_live.tinserted,
    fact_views_live.created,
    UPPER((fact_views_live.bundle_id)::text)      AS bundleid,
    UPPER((fact_views_live.application_id)::text) AS applicationid,
    UPPER((fact_views_live.global_user_id)::text) AS globaluserid,
    CASE
        WHEN (fact_views_live.anonymous_id IS NOT NULL)
        THEN true
        ELSE false
    END                                      AS isanonymous,
    UPPER((fact_views_live.device_id)::text) AS deviceid,
    CASE
        WHEN ((fact_views_live.device_type)::text = 'ios'::text)
        THEN 1
        WHEN ((fact_views_live.device_type)::text = 'android'::text)
        THEN 3
        ELSE NULL::INTEGER
    END                                                AS apptypeid,
    fact_views_live.device_os_version                  AS deviceosversion,
    fact_views_live.binary_version                     AS binaryversion,
    (fact_views_live.mmm_info)::CHARACTER VARYING(256) AS mmm_info,
    fact_views_live.identifier,
    fact_views_live.metadata
FROM
    PUBLIC.Fact_Views_Live fact_views_live
--Incremental Logic
WHERE Batch_Id >= (SELECT COALESCE(MAX(Batch_Id),0) FROM metrics.Views WHERE Src = 'newMetrics_Live') --Identify the last batch that was loaded)
;

--================================================================================================================================================================
--2. Delete records tied to the latest Batch
DELETE FROM metrics.Views WHERE SRC = 'newMetrics_Live' AND Batch_Id IN (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'newMetrics_Live');

--================================================================================================================================================================
--3. Insert from Stage to Target
INSERT INTO metrics.Views SELECT * FROM metrics.Views_STG_new;

--================================================================================================================================================================
