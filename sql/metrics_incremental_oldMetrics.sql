--================================================================================================================================================================
--OLD METRICS (Incremental Load) - DELSERT (Delete latest Batch, Reinsert from latest Batch)
--1. Identify the new data to be loaded to stage
--2. Delete out the records tied to the last Batch that was loaded
--3. Load from Stage to Target
--================================================================================================================================================================

--1. Stage Loads only for the latest Batch ID found in the existing set of Sessions identified
TRUNCATE TABLE metrics.Views_STG_old;
VACUUM metrics.Views_STG_old;
INSERT INTO metrics.Views_STG_old
SELECT
    'oldMetrics_Live'::text AS src,
    fact_views_new.batch_id,
    fact_views_new.row_id,
    fact_views_new.tinserted,
    fact_views_new.created,
    UPPER((fact_views_new.bundle_id)::text)      AS bundleid,
    UPPER((fact_views_new.application_id)::text) AS applicationid,
    UPPER((fact_views_new.global_user_id)::text) AS globaluserid,
    fact_views_new.is_anonymous                  AS isanonymous,
    UPPER((fact_views_new.device_id)::text)      AS deviceid,
    fact_views_new.app_type_id                   AS apptypeid,
    fact_views_new.device_os_version             AS deviceosversion,
    fact_views_new.binary_version                AS binaryversion,
    fact_views_new.mmm_info,
    fact_views_new.identifier,
    fact_views_new.metadata
FROM
    PUBLIC.Fact_Views_New fact_views_new
--Incremental Logic
WHERE Batch_Id >= (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'oldMetrics_Live') --Identify the last batch that was loaded
;

--================================================================================================================================================================
--2. Delete records tied to the latest Batch
DELETE FROM metrics.Views WHERE SRC = 'oldMetrics_Live' AND Batch_Id IN (SELECT COALESCE(MAX(Batch_Id),0) FROM metrics.Views WHERE Src = 'oldMetrics_Live');

--================================================================================================================================================================
--3. Insert from Stage to Target
INSERT INTO metrics.Views SELECT * FROM metrics.Views_STG_old;

--================================================================================================================================================================
