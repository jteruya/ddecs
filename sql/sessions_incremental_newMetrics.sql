--================================================================================================================================================================
--NEW METRICS (Incremental Load) - DELSERT (Delete latest Batch, Reinsert from latest Batch)
--1. Identify the new data to be loaded to stage
--2. Delete out the records tied to the last Batch that was loaded
--3. Load from Stage to Target
--================================================================================================================================================================

--1. Stage Loads only for the latest Batch ID found in the existing set of Sessions identified
TRUNCATE TABLE EventCube.Sessions_STG_new;
VACUUM EventCube.Sessions_STG_new;
INSERT INTO EventCube.Sessions_STG_new 
SELECT 
  'newMetrics_Live' AS SRC,
  base.Batch_Id, 
  base.Row_Id, 
  base.TInserted, 
  UPPER(base.Application_Id) AS ApplicationId, 
  iu.UserId AS UserId, 
  UPPER(base.Global_User_Id) AS GlobalUserId,
  UPPER(base.Device_Id) AS DeviceId, 
  CASE WHEN base.Device_Type = 'ios' THEN 1 WHEN base.Device_Type = 'android' THEN 3 END AS AppTypeId, 
  base.Binary_Version AS BinaryVersion, 
  ('1970-01-01 00:00:00'::TIMESTAMP without TIME zone + (((((base.Metadata ->> 'Start'::text))::NUMERIC / (1000)::NUMERIC))::DOUBLE PRECISION * '00:00:01'::interval)) AS StartDate,
  base.Created AS EndDate
FROM PUBLIC.Fact_Sessions_Live base
LEFT JOIN (
        --Identify the latest/live UserId tied to this GlobalUserId/ApplicationId
        SELECT * FROM (
          SELECT ApplicationId, UserId, GlobalUserId, Created, RANK() OVER (PARTITION BY ApplicationId, GlobalUserId ORDER BY IsDisabled ASC, Created DESC) AS RNK
          FROM AuthDB_IS_Users 
        ) t WHERE t.RNK = 1
) iu ON UPPER(base.Application_Id) = iu.ApplicationId AND UPPER(base.Global_User_Id) = iu.GlobalUserId
WHERE base.Identifier = 'end'
--Incremental Logic
AND base.Batch_Id >= (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'newMetrics_Live') --Identify the last batch that was loaded)
;

--================================================================================================================================================================
--2. Delete records tied to the latest Batch
DELETE FROM EventCube.Sessions WHERE SRC = 'newMetrics_Live' AND Batch_Id IN (SELECT COALESCE(MAX(Batch_Id),0) FROM EventCube.Sessions WHERE Src = 'newMetrics_Live');

--================================================================================================================================================================
--3. Insert from Stage to Target
INSERT INTO EventCube.Sessions SELECT * FROM EventCube.Sessions_STG_new;

--================================================================================================================================================================
