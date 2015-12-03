--================================================================================================================================================================
--OLD METRICS (Incremental Load) - DELSERT (Delete latest Batch, Reinsert from latest Batch)
--1. Identify the new data to be loaded to stage
--2. Delete out the records tied to the last Batch that was loaded
--3. Load from Stage to Target
--================================================================================================================================================================

--1. Stage Loads only for the latest Batch ID found in the existing set of Sessions identified
TRUNCATE TABLE EventCube.Sessions_STG_old;
VACUUM EventCube.Sessions_STG_old;
INSERT INTO EventCube.Sessions_STG_old
SELECT 
  'oldMetrics_Live' AS SRC,
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
                FROM PUBLIC.Fact_Sessions_New
                WHERE Metrics_Type_Id IN (1,2) 
                --Incremental Logic
                AND Batch_Id >= (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'oldMetrics_Live') --Identify the last batch that was loaded
                
        ) t
) t
WHERE t.MetricTypeId = 1 
AND CASE WHEN t.MetricTypeId = 1 AND t.NEXT_MetricTypeId = 2 THEN t.NEXT_DT END IS NOT NULL;

--================================================================================================================================================================
--2. Delete records tied to the latest Batch
DELETE FROM EventCube.Sessions WHERE SRC = 'oldMetrics_Live' AND Batch_Id IN (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'oldMetrics_Live');

--================================================================================================================================================================
--3. Insert from Stage to Target
INSERT INTO EventCube.Sessions SELECT * FROM EventCube.Sessions_STG_old;

--================================================================================================================================================================
