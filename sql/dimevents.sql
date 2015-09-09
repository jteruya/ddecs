--===============================================================================================
-- Base data on the Application source records. 
-- * Upstream dependency on DimUsers. 
--
-- The following conditions are applied:
-- 1. Application must be related to a identified User (in the Dimension table)
--
-- The main transformations are flag indicator fields from the following:
-- 1. Application configuration grid items
-- 2. Application configuration settings
-- The above flagging transformations are handled differently depending on source table logic. 
--===============================================================================================

TRUNCATE TABLE EventCube.DimEvents;
VACUUM EventCube.DimEvents;
INSERT INTO EventCube.DimEvents
SELECT DISTINCT A.ApplicationId, TRIM(A.Name) AS Name,

CAST(StartDate AS DATE) AS StartDate,
CAST(EndDate AS DATE) AS EndDate,

CAST(A.CanRegister AS INT) AS OpenEvent,

COALESCE(LeadScanning,0) AS LeadScanning,
COALESCE(SurveysOn,0) AS SurveysOn,
COALESCE(InteractiveMap,0) AS InteractiveMap,
COALESCE(Leaderboard,0) AS Leaderboard,
COALESCE(Bookmarking,0) AS Bookmarking,
COALESCE(Photofeed,0) AS Photofeed,
COALESCE(AttendeesList,0) AS AttendeesList,
COALESCE(QRCode,0) AS QRCode,

COALESCE(ExhibitorReqInfo,0) AS ExhibitorReqInfo,
COALESCE(ExhibitorMsg,0) AS ExhibitorMsg,
COALESCE(PrivateMsging,0) AS PrivateMsging,
COALESCE(PeopleMatching,0) AS PeopleMatching,
COALESCE(SocialNetworks,0) AS SocialNetworks,
COALESCE(RatingsOn,0) AS RatingsOn

FROM PUBLIC.AuthDB_Applications A

JOIN (SELECT DISTINCT ApplicationId FROM EventCube.DimUsers) U ON A.ApplicationId = U.Applicationid

LEFT OUTER JOIN
( SELECT ApplicationId,
  MAX(CASE WHEN TypeId = 14 THEN 1 ELSE 0 END) LeadScanning,
  MAX(CASE WHEN TypeId = 12 THEN 1 ELSE 0 END) SurveysOn,
  MAX(CASE WHEN TypeId = 10 THEN 1 ELSE 0 END) InteractiveMap,
  MAX(CASE WHEN TypeId = 6 THEN 1 ELSE 0 END) Leaderboard,
  MAX(CASE WHEN TypeId = 7 THEN 1 ELSE 0 END) Bookmarking,
  MAX(CASE WHEN TypeId = 11 THEN 1 ELSE 0 END) Photofeed,
  MAX(CASE WHEN TypeId = 8 THEN 1 ELSE 0 END) AttendeesList,
  MAX(CASE WHEN TypeId = 15 THEN 1 ELSE 0 END) QRCode
  FROM PUBLIC.Ratings_ApplicationConfigGridItems
  GROUP BY ApplicationId
) G
ON U.ApplicationId = G.ApplicationId

LEFT OUTER JOIN
( SELECT ApplicationId,
  MAX(CASE WHEN Name = 'ExhibitorRequestInformationEnabled' AND SettingValue = 'True' THEN 1 ELSE 0 END) ExhibitorReqInfo,
  MAX(CASE WHEN Name = 'ExhibitorMessagingEnabled' AND SettingValue = 'True' THEN 1 ELSE 0 END) ExhibitorMsg,
  MAX(CASE WHEN Name = 'MessagingEnabled' AND SettingValue = 'True' THEN 1 ELSE 0 END) PrivateMsging,
  MAX(CASE WHEN Name = 'EnablePeopleMatching' AND SettingValue = 'True' THEN 1 ELSE 0 END) PeopleMatching,
  MAX(CASE WHEN Name = 'SocialNetworks' AND SettingValue IS NOT NULL THEN 1 ELSE 0 END) SocialNetworks,
  MAX(CASE WHEN Name = 'EnableRatings' AND SettingValue = 'True' THEN 1 ELSE 0 END) RatingsOn
  FROM PUBLIC.Ratings_ApplicationConfigSettings
  GROUP BY ApplicationId
) S
ON U.ApplicationId = S.ApplicationId;

--CREATE INDEX ndx_ecs_dimevents_applicationid ON EventCube.DimEvents (ApplicationId);