IF OBJECT_ID('ReportingDB.dbo.DimEvents','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimEvents

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

SELECT DISTINCT A.ApplicationId, dbo.STRIP_STRING(A.Name) Name,

CAST(StartDate AS DATE) StartDate,
CAST(EndDate AS DATE) EndDate,

CAST(A.CanRegister AS INT) OpenEvent,

ISNULL(LeadScanning,0) LeadScanning,
ISNULL(SurveysOn,0) SurveysOn,
ISNULL(InteractiveMap,0) InteractiveMap,
ISNULL(Leaderboard,0) Leaderboard,
ISNULL(Bookmarking,0) Bookmarking,
ISNULL(Photofeed,0) Photofeed,
ISNULL(AttendeesList,0) AttendeesList,
ISNULL(QRCode,0) QRCode,

ISNULL(ExhibitorReqInfo,0) ExhibitorReqInfo,
ISNULL(ExhibitorMsg,0) ExhibitorMsg,
ISNULL(PrivateMsging,0) PrivateMsging,
ISNULL(PeopleMatching,0) PeopleMatching,
ISNULL(SocialNetworks,0) SocialNetworks,
ISNULL(RatingsOn,0) RatingsOn

INTO ReportingDB.dbo.DimEvents

FROM AuthDB.dbo.Applications A

JOIN (SELECT DISTINCT ApplicationId FROM ReportingDB.dbo.DimUsers) U ON A.ApplicationId = U.Applicationid

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
  FROM Ratings.dbo.ApplicationConfigGridItems
  GROUP BY ApplicationId
) G
ON U.ApplicationId = G.ApplicationId

LEFT OUTER JOIN
( SELECT ApplicationId,
  MAX(CASE WHEN Name = 'ExhibitorRequestInformationEnabled' AND Value = 'True' THEN 1 ELSE 0 END) ExhibitorReqInfo,
  MAX(CASE WHEN Name = 'ExhibitorMessagingEnabled' AND Value = 'True' THEN 1 ELSE 0 END) ExhibitorMsg,
  MAX(CASE WHEN Name = 'MessagingEnabled' AND Value = 'True' THEN 1 ELSE 0 END) PrivateMsging,
  MAX(CASE WHEN Name = 'EnablePeopleMatching' AND Value = 'True' THEN 1 ELSE 0 END) PeopleMatching,
  MAX(CASE WHEN Name = 'SocialNetworks' AND Value IS NOT NULL THEN 1 ELSE 0 END) SocialNetworks,
  MAX(CASE WHEN Name = 'EnableRatings' AND Value = 'True' THEN 1 ELSE 0 END) RatingsOn
  FROM Ratings.dbo.ApplicationConfigSettings
  GROUP BY ApplicationId
) S
ON U.ApplicationId = S.ApplicationId

