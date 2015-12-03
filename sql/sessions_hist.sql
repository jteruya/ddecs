--New Table
DROP TABLE IF EXISTS EventCube.Sessions;
CREATE TABLE
    EventCube.Sessions
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        applicationid CHARACTER VARYING(64),
        userid INTEGER,
        deviceid TEXT,
        apptypeid INTEGER,
        binaryversion CHARACTER VARYING(32),
        startdate TIMESTAMP(6) WITHOUT TIME ZONE,
        enddate TIMESTAMP(6) WITHOUT TIME ZONE
    );
    
--New STG Table (for newMetrics)
DROP TABLE IF EXISTS EventCube.Sessions_STG_new;
CREATE TABLE
    EventCube.Sessions_STG_new
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        applicationid CHARACTER VARYING(64),
        userid INTEGER,
        deviceid TEXT,
        apptypeid INTEGER,
        binaryversion CHARACTER VARYING(32),
        startdate TIMESTAMP(6) WITHOUT TIME ZONE,
        enddate TIMESTAMP(6) WITHOUT TIME ZONE
    ) TABLESPACE FastStorage;    
    
--New STG Table (for oldMetrics)
DROP TABLE IF EXISTS EventCube.Sessions_STG_old;
CREATE TABLE
    EventCube.Sessions_STG_old
    (
        src TEXT,
        batch_id INTEGER,
        row_id INTEGER,
        tinserted TIMESTAMP(6) WITHOUT TIME ZONE,
        applicationid CHARACTER VARYING(64),
        userid INTEGER,
        deviceid TEXT,
        apptypeid INTEGER,
        binaryversion CHARACTER VARYING(32),
        startdate TIMESTAMP(6) WITHOUT TIME ZONE,
        enddate TIMESTAMP(6) WITHOUT TIME ZONE
    ) TABLESPACE FastStorage;      

--Full refresh requires a full truncate + vacuum
TRUNCATE TABLE EventCube.Sessions;
VACUUM EventCube.Sessions;

--Originate from the base set of Sessions from AnalyticsDB.
INSERT INTO EventCube.Sessions
SELECT
    'AnalyticsDB'::text AS src,
    fact_sessions_old.batch_id,
    fact_sessions_old.row_id,
    fact_sessions_old.tinserted,
    fact_sessions_old.application_id           AS applicationid,
    fact_sessions_old.user_id                  AS userid,
    --NULL::text                                 AS globaluserid,
    --NULL::INTEGER                              AS metrictypeid,
    UPPER((fact_sessions_old.device_id)::text) AS deviceid,
    fact_sessions_old.app_type_id              AS apptypeid,
    fact_sessions_old.binary_version           AS binaryversion,
    fact_sessions_old.start_date               AS startdate,
    fact_sessions_old.end_date                 AS enddate
FROM
    fact_sessions_old
WHERE
    (
        fact_sessions_old.end_date < '2015-04-24 00:00:00'::TIMESTAMP without TIME zone)
;

--Load the Historical Dataset from Robin
--Identify the linked Session Start/End metric records per User and then merge the two records to one that indicates a session
INSERT INTO EventCube.Sessions
SELECT 
  'Hist' AS SRC,
  t.Batch_Id,
  t.Row_Id,
  t.TInserted,
  t.ApplicationId, 
  t.UserId, 
  t.DeviceId,
  t.AppTypeId,
  t.BinaryVersion,
  t.DT AS StartDate,
  CASE WHEN t.MetricTypeId = 1 AND t.NEXT_MetricTypeId = 2 THEN t.NEXT_DT END AS EndDate
  --CASE WHEN t.MetricTypeId = 1 AND t.NEXT_MetricTypeId = 2 THEN t.NEXT_DT - t.DT  END AS Duration,
  --CASE WHEN t.MetricTypeId = 1 AND t.NEXT_MetricTypeId = 2 THEN CAST(EXTRACT(EPOCH FROM t.NEXT_DT - t.DT) AS NUMERIC) END AS Duration_Seconds
FROM (
        SELECT 
          Batch_Id,
          Row_Id,
          TInserted,
          ApplicationId, 
          UserId, 
          DT, 
          MetricTypeId,
          DeviceId,
          AppTypeId,
          BinaryVersion,
          MAX(DT) OVER (PARTITION BY ApplicationId, UserId ORDER BY ApplicationId, UserId, DT ROWS BETWEEN 1 FOLLOWING and 1 FOLLOWING) AS NEXT_DT, 
          MAX(MetricTypeId) OVER (PARTITION BY ApplicationId, UserId ORDER BY ApplicationId, UserId, DT ROWS BETWEEN 1 FOLLOWING and 1 FOLLOWING) AS NEXT_MetricTypeId
        FROM (

                SELECT 
                  Batch_Id, 
                  Row_Id, 
                  TInserted, 
                  UPPER(Application_Id) AS ApplicationId, 
                  User_Id AS UserId, 
                  Metrics_Type_Id AS MetricTypeId, 
                  UPPER(Device_Id) AS DeviceId, 
                  App_Type_Id AS AppTypeId, 
                  Binary_Version AS BinaryVersion, 
                  CASE WHEN Metrics_Type_Id = 1 THEN Start_Date WHEN Metrics_Type_Id = 2 THEN End_Date END AS DT 
                FROM Fact_Sessions 
                WHERE Metrics_Type_Id IN (1,2) 
                AND CASE WHEN Metrics_Type_Id = 1 THEN Start_Date WHEN Metrics_Type_Id = 2 THEN End_Date END >= '2015-04-24 00:00:00'
                
        ) t
) t
WHERE t.MetricTypeId = 1 
AND CASE WHEN t.MetricTypeId = 1 AND t.NEXT_MetricTypeId = 2 THEN t.NEXT_DT END IS NOT NULL;