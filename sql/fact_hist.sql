-- ======================
-- VIEWS 
-- ======================

--New Table
DROP TABLE IF EXISTS metrics.Views;
CREATE TABLE metrics.Views
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        created TIMESTAMP(6) WITHOUT TIME ZONE,
        bundleid TEXT,
        applicationid TEXT,
        globaluserid TEXT,
        isanonymous BOOLEAN,
        deviceid TEXT,
        apptypeid INTEGER,
        deviceosversion CHARACTER VARYING(256),
        binaryversion CHARACTER VARYING(32),
        mmm_info CHARACTER VARYING(256),
        identifier CHARACTER VARYING(256),
        metadata JSONB
    );    
    
--New STG Table (for newMetrics)
DROP TABLE IF EXISTS metrics.Views_STG_new;
CREATE TABLE
    metrics.Views_STG_new
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        created TIMESTAMP(6) WITHOUT TIME ZONE,
        bundleid TEXT,
        applicationid TEXT,
        globaluserid TEXT,
        isanonymous BOOLEAN,
        deviceid TEXT,
        apptypeid INTEGER,
        deviceosversion CHARACTER VARYING(256),
        binaryversion CHARACTER VARYING(32),
        mmm_info CHARACTER VARYING(256),
        identifier CHARACTER VARYING(256),
        metadata JSONB
    ) TABLESPACE FastStorage;      
    
--New STG Table (for oldMetrics)
DROP TABLE IF EXISTS metrics.Views_STG_old;
CREATE TABLE
    metrics.Views_STG_old
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        created TIMESTAMP(6) WITHOUT TIME ZONE,
        bundleid TEXT,
        applicationid TEXT,
        globaluserid TEXT,
        isanonymous BOOLEAN,
        deviceid TEXT,
        apptypeid INTEGER,
        deviceosversion CHARACTER VARYING(256),
        binaryversion CHARACTER VARYING(32),
        mmm_info CHARACTER VARYING(256),
        identifier CHARACTER VARYING(256),
        metadata JSONB
    ) TABLESPACE FastStorage;    

--Full refresh requires a full truncate + vacuum
TRUNCATE TABLE metrics.Views;
VACUUM metrics.Views;

--Originate from the base set of Sessions from Alfred.
INSERT INTO metrics.Views
SELECT
    'Hist_Alfred'::text AS src,
    fact_views.batch_id,
    fact_views.row_id,
    fact_views.tinserted,
    fact_views.created,
    UPPER((fact_views.bundle_id)::text)      AS bundleid,
    UPPER((fact_views.application_id)::text) AS applicationid,
    UPPER((fact_views.global_user_id)::text) AS globaluserid,
    fact_views.is_anonymous                  AS isanonymous,
    UPPER((fact_views.device_id)::text)      AS deviceid,
    fact_views.app_type_id                   AS apptypeid,
    fact_views.device_os_version             AS deviceosversion,
    fact_views.binary_version                AS binaryversion,
    fact_views.mmm_info,
    fact_views.identifier,
    fact_views.metadata
FROM
    fact_views
WHERE
    (
        fact_views.created >= '2015-04-24 00:00:00'::TIMESTAMP without TIME zone)
;        

--Load the Historical Dataset from Robin
INSERT INTO metrics.Views
SELECT
    'Hist_Robin'::text AS src,
    fact_views_old.batch_id,
    fact_views_old.row_id,
    fact_views_old.tinserted,
    fact_views_old.created,
    fact_views_old.bundle_id         AS bundleid,
    fact_views_old.application_id    AS applicationid,
    fact_views_old.global_user_id    AS globaluserid,
    fact_views_old.is_anonymous      AS isanonymous,
    fact_views_old.device_id         AS deviceid,
    fact_views_old.app_type_id       AS apptypeid,
    fact_views_old.device_os_version AS deviceosversion,
    fact_views_old.binary_version    AS binaryversion,
    fact_views_old.mmm_info,
    fact_views_old.identifier,
    CASE
        WHEN (fact_views_old.metadata = '{}'::jsonb)
        THEN NULL::jsonb
        ELSE fact_views_old.metadata
    END AS metadata
FROM
    fact_views_old
WHERE
    (
        fact_views_old.created < '2015-04-24 00:00:00'::TIMESTAMP without TIME zone)
;