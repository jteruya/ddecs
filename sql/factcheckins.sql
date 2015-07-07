DROP TABLE IF EXISTS EventCube.FactCheckIns;

--======================================================
-- Base set of Check-Ins from Ratings. 
-- * Upstream dependency on DimUsers
--
-- Minor transformations:
-- 1. Translation of numeric codes to string values.
-- 2. Additional joins for identifying flag indicators. 
--======================================================

CREATE TABLE EventCube.FactCheckIns AS 
SELECT 
S.ShowUpId, 
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
CAST(S.IsTransient AS INT) IsHeadcount
FROM PUBLIC.Ratings_ShowUps S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;

