--==========================================================
-- Increase PG Timeout Window to 180 Minutes
--==========================================================

SET statement_timeout = '180 min';
COMMIT;

--===================================================================================================
-- * Upstream dependent on creation of all Dimension and Fact tables.
-- Creates an aggregate at the User level with Application-level fields for slicing.
--===================================================================================================

TRUNCATE TABLE EventCube.STG_UserCubeSummary;
VACUUM EventCube.STG_UserCubeSummary;
INSERT INTO EventCube.STG_UserCubeSummary
SELECT 
  --== Application Metadata
  u.ApplicationId, 
  COALESCE(e.Name,'???') AS Name, 
  e.StartDate AS StartDate, 
  e.EndDate AS EndDate,
  -- CASE WHEN SF.SF_EventStartDate IS NOT NULL AND SF.SF_EventStartDate <> '' THEN CAST(SF.SF_EventStartDate AS DATE) ELSE e.StartDate END AS StartDate, 
  -- CASE WHEN SF.SF_EventEndDate IS NOT NULL AND SF.SF_EventEndDate <> '' THEN CAST(SF.SF_EventEndDate AS DATE) ELSE e.EndDate END AS EndDate,
  
  --== User Metadata
  u.GlobalUserId, 
  u.UserId,
  u.FirstTimestamp,
  u.LastTimestamp,
  
  COALESCE(social.Facebook,0) AS Facebook, 
  COALESCE(social.Twitter,0) AS Twitter, 
  COALESCE(social.LinkedIn,0) AS LinkedIn, 

  --== User Inferred Properties
  COALESCE(device.Device,'???') AS Device, 
  COALESCE(device.DeviceType,'???') AS DeviceType,   
  COALESCE(binaryversion.BinaryVersion,'v???') AS BinaryVersion, 
  CASE WHEN sessions.Sessions >= 1 THEN 1 ELSE 0 END AS Active, 
  CASE WHEN sessions.Sessions >= 10 THEN 1 ELSE 0 END AS Engaged,

  --== Fact Data
  COALESCE(Sessions,0) AS Sessions,
  COALESCE(sessions.EventSessions, 0) AS EventSessions, 
  COALESCE(Posts,0) AS Posts, 
  COALESCE(PostsImage,0) AS PostsImage, 
  COALESCE(PostsItem,0) AS PostsItem, 
  COALESCE(Likes,0) AS Likes, 
  COALESCE(Comments,0) AS Comments, 
  COALESCE(TotalBookmarks,0) AS TotalBookmarks, 
  COALESCE(ImportedBookmarks,0) AS ImportedBookmarks, 
  COALESCE(Follows,0) AS Follows, 
  COALESCE(CheckIns,0) AS CheckIns, 
  COALESCE(CheckInsHeadcount,0) AS CheckInsHeadcount, 
  COALESCE(Ratings,0) AS Ratings, 
  COALESCE(Reviews,0) AS Reviews, 
  COALESCE(Surveys,0) AS Surveys,

  --== Feature Indicators
  COALESCE(e.OpenEvent,-1) AS OpenEvent, 
  COALESCE(e.LeadScanning,-1) AS LeadScanning, 
  COALESCE(e.SurveysOn,-1) AS SurveysOn, 
  COALESCE(e.InteractiveMap,-1) AS InteractiveMap, 
  COALESCE(e.Leaderboard,-1) AS Leaderboard, 
  COALESCE(e.Bookmarking,-1) AS Bookmarking, 
  COALESCE(e.Photofeed,-1) AS Photofeed, 
  COALESCE(e.AttendeesList,-1) AS AttendeesList, 
  COALESCE(e.QRCode,-1) AS QRCode, 
  COALESCE(e.DirectMessaging,-1) AS DirectMessaging,
  COALESCE(e.TopicChannel,-1) AS TopicChannel,
  COALESCE(e.ExhibitorReqInfo,-1) AS ExhibitorReqInfo, 
  COALESCE(e.ExhibitorMsg,-1) AS ExhibitorMsg, 
  COALESCE(e.PrivateMsging,-1) AS PrivateMsging, 
  COALESCE(e.PeopleMatching,-1) AS PeopleMatching, 
  COALESCE(e.SocialNetworks,-1) AS SocialNetworks, 
  COALESCE(e.RatingsOn,-1) AS RatingsOn,
  COALESCE(e.NativeSessionNotes,-1) AS NativeSessionNotes,
  COALESCE(e.SessionChannel, -1) AS SessionChannel,
  COALESCE(e.SessionRecommendations, -1) AS SessionRecommendations,
  COALESCE(e.PeopleRecommendations, -1) AS PeopleRecommendations,
  COALESCE(e.AttendeeSessionScans, -1) AS AttendeeSessionScans,
  COALESCE(e.OrganizerOnlyFeed, -1) AS OrganizerOnlyFeed,
  COALESCE(e.NestedAgenda, -1) AS NestedAgenda,

  --== SalesForce Metadata
  NULL AS EventType,
  NULL AS EventSize,
  NULL AS AccountCustomerDomain,
  NULL AS ServiceTierName,
  NULL AS App365Indicator,
  NULL AS OwnerName
  -- COALESCE(SF.EventType,'_Unknown') AS EventType, 
  -- COALESCE(SF.EventSize,'_Unknown') AS EventSize, 
  -- COALESCE(SF.AccountCustomerDomain,'_Unknown') AS AccountCustomerDomain, 
  -- COALESCE(SF.ServiceTierName,'_Unknown') AS ServiceTierName, 
  -- COALESCE(SF.App365Indicator,'_Unknown') AS App365Indicator, 
  -- COALESCE(SF.SF_OwnerName,'_Unknown') AS OwnerName
  
FROM EventCube.DimUsers u

--== Application Metadata
LEFT OUTER JOIN EventCube.DimEvents e ON u.ApplicationId = e.ApplicationId
-- LEFT OUTER JOIN EventCube.V_DimEventsSFDC SF ON u.ApplicationId = SF.ApplicationId

--== User Metadata
LEFT OUTER JOIN EventCube.V_DimUserDeviceType device ON u.UserId = device.UserId
LEFT OUTER JOIN EventCube.V_DimUserSocialNetworks social ON u.UserId = social.UserId
LEFT OUTER JOIN EventCube.V_DimUserBinaryVersion binaryversion ON u.UserId = binaryversion.UserId

--== User Facts
LEFT OUTER JOIN (SELECT UserId, Sessions, EventSessions FROM EventCube.Agg_Session_per_AppUser) sessions ON u.UserId = sessions.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Posts, SUM(HasImage) PostsImage, SUM(CASE WHEN ListType != 'Regular' THEN 1 ELSE 0 END) PostsItem FROM EventCube.V_FactPosts GROUP BY UserId) P ON U.UserId = P.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Likes FROM EventCube.V_FactLikes GROUP BY UserId) L ON U.UserId = L.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Comments FROM EventCube.V_FactComments GROUP BY UserId) C ON U.UserId = C.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS TotalBookmarks, SUM(CASE WHEN IsImported IS NULL THEN 0 WHEN IsImported IS false THEN 0 ELSE 1 END) AS ImportedBookmarks FROM EventCube.V_FactBookmarks GROUP BY UserId) B ON U.UserId = B.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Follows FROM EventCube.V_FactFollows GROUP BY UserId) F ON U.UserId = F.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS CheckIns, SUM(IsHeadcount) CheckInsHeadcount FROM EventCube.V_FactCheckIns GROUP BY UserId) K ON U.UserId = K.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Ratings, SUM(HasReview) Reviews FROM EventCube.V_FactRatings GROUP BY UserId) R ON U.UserId = R.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Surveys FROM EventCube.V_FactSurveys GROUP BY UserId) V ON U.UserId = V.UserId
;

--======================================================================================================================================================--

--=========--
-- UPSERT --
--=========--
--Identify the Users that are not yet in UserCubeSummary (so we can INSERT them)
TRUNCATE TABLE EventCube.STG_UserCubeSummary_INSERT;
VACUUM EventCube.STG_UserCubeSummary_INSERT;
INSERT INTO EventCube.STG_UserCubeSummary_INSERT
SELECT ApplicationId, Name, StartDate, EndDate, GlobalUserId, UserId, FirstTimestamp, LastTimestamp, Facebook, Twitter, LinkedIn, Device, DeviceType, BinaryVersion, Active, Engaged, Sessions, EventSessions, Posts, PostsImage, PostsItem, Likes, Comments, TotalBookmarks, ImportedBookmarks, Follows, CheckIns, CheckInsHeadCount, Ratings, Reviews, Surveys, OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, PhotoFeed, AttendeesList, QRCode, DirectMessaging, TopicChannel, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn, NativeSessionNotes, SessionChannel, SessionRecommendations, PeopleRecommendations, AttendeeSessionScans, OrganizerOnlyFeed, NestedAgenda, EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName FROM (
SELECT a.*, b.UserId AS bUserId FROM EventCube.STG_UserCubeSummary a
--Forced to use the Left Join instead of NOT IN logic (due to performance)
LEFT JOIN (SELECT DISTINCT UserId FROM EventCube.UserCubeSummary) b ON a.UserId = b.UserId
) t WHERE bUserId IS NULL;

--Identify the Users that are in UserCubeSummary (so we can UPDATE them)
TRUNCATE TABLE EventCube.STG_UserCubeSummary_UPDATE;
VACUUM EventCube.STG_UserCubeSummary_UPDATE;
INSERT INTO EventCube.STG_UserCubeSummary_UPDATE
SELECT * FROM EventCube.STG_UserCubeSummary WHERE UserId IN (SELECT UserId FROM EventCube.UserCubeSummary);

INSERT INTO EventCube.UserCubeSummary SELECT *, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM EventCube.STG_UserCubeSummary_INSERT;

UPDATE EventCube.UserCubeSummary ucs 
SET 
  Name = EventCube.STG_UserCubeSummary_UPDATE.Name,
  StartDate = EventCube.STG_UserCubeSummary_UPDATE.StartDate,
  EndDate = EventCube.STG_UserCubeSummary_UPDATE.EndDate,
  OpenEvent = EventCube.STG_UserCubeSummary_UPDATE.OpenEvent,
  LeadScanning = EventCube.STG_UserCubeSummary_UPDATE.LeadScanning,
  SurveysOn = EventCube.STG_UserCubeSummary_UPDATE.SurveysOn,
  InteractiveMap = EventCube.STG_UserCubeSummary_UPDATE.InteractiveMap,
  Leaderboard = EventCube.STG_UserCubeSummary_UPDATE.Leaderboard,
  Bookmarking = EventCube.STG_UserCubeSummary_UPDATE.Bookmarking,
  Photofeed = EventCube.STG_UserCubeSummary_UPDATE.Photofeed,
  AttendeesList = EventCube.STG_UserCubeSummary_UPDATE.AttendeesList,
  QRCode = EventCube.STG_UserCubeSummary_UPDATE.QRCode,
  DirectMessaging = EventCube.STG_UserCubeSummary_UPDATE.DirectMessaging,
  TopicChannel = EventCube.STG_UserCubeSummary_UPDATE.TopicChannel,
  ExhibitorReqInfo = EventCube.STG_UserCubeSummary_UPDATE.ExhibitorReqInfo,
  ExhibitorMsg = EventCube.STG_UserCubeSummary_UPDATE.ExhibitorMsg,
  PrivateMsging = EventCube.STG_UserCubeSummary_UPDATE.PrivateMsging,
  PeopleMatching = EventCube.STG_UserCubeSummary_UPDATE.PeopleMatching,
  SocialNetworks = EventCube.STG_UserCubeSummary_UPDATE.SocialNetworks,
  RatingsOn = EventCube.STG_UserCubeSummary_UPDATE.RatingsOn,
  NativeSessionNotes = EventCube.STG_UserCubeSummary_UPDATE.NativeSessionNotes,
  SessionChannel = EventCube.STG_UserCubeSummary_UPDATE.SessionChannel,
  SessionRecommendations = EventCube.STG_UserCubeSummary_UPDATE.SessionRecommendations,
  PeopleRecommendations = EventCube.STG_UserCubeSummary_UPDATE.PeopleRecommendations,
  AttendeeSessionScans = EventCube.STG_UserCubeSummary_UPDATE.AttendeeSessionScans,
  OrganizerOnlyFeed = EventCube.STG_UserCubeSummary_UPDATE.OrganizerOnlyFeed,
  NestedAgenda = EventCube.STG_UserCubeSummary_UPDATE.NestedAgenda,  
  EventType = EventCube.STG_UserCubeSummary_UPDATE.EventType,
  EventSize = EventCube.STG_UserCubeSummary_UPDATE.EventSize,
  AccountCustomerDomain = EventCube.STG_UserCubeSummary_UPDATE.AccountCustomerDomain,
  ServiceTierName = EventCube.STG_UserCubeSummary_UPDATE.ServiceTierName,
  App365Indicator = EventCube.STG_UserCubeSummary_UPDATE.App365Indicator,
  OwnerName = EventCube.STG_UserCubeSummary_UPDATE.OwnerName,
  BinaryVersion = EventCube.STG_UserCubeSummary_UPDATE.BinaryVersion,
  DeviceType = EventCube.STG_UserCubeSummary_UPDATE.DeviceType,
  Device = EventCube.STG_UserCubeSummary_UPDATE.Device,
  Facebook = EventCube.STG_UserCubeSummary_UPDATE.Facebook,
  Twitter = EventCube.STG_UserCubeSummary_UPDATE.Twitter,
  LinkedIn = EventCube.STG_UserCubeSummary_UPDATE.LinkedIn,
  FirstTimestamp = EventCube.STG_UserCubeSummary_UPDATE.FirstTimestamp,
  LastTimestamp = EventCube.STG_UserCubeSummary_UPDATE.LastTimestamp,
  Active = EventCube.STG_UserCubeSummary_UPDATE.Active,
  Engaged = EventCube.STG_UserCubeSummary_UPDATE.Engaged,
  Sessions = EventCube.STG_UserCubeSummary_UPDATE.Sessions,
  EventSessions = EventCube.STG_UserCubeSummary_UPDATE.EventSessions,
  Posts = EventCube.STG_UserCubeSummary_UPDATE.Posts,
  PostsImage = EventCube.STG_UserCubeSummary_UPDATE.PostsImage,
  PostsItem = EventCube.STG_UserCubeSummary_UPDATE.PostsItem,
  Likes = EventCube.STG_UserCubeSummary_UPDATE.Likes,
  Comments = EventCube.STG_UserCubeSummary_UPDATE.Comments,
  TotalBookmarks = EventCube.STG_UserCubeSummary_UPDATE.TotalBookmarks,
  ImportedBookmarks = EventCube.STG_UserCubeSummary_UPDATE.ImportedBookmarks,
  Follows = EventCube.STG_UserCubeSummary_UPDATE.Follows,
  Checkins = EventCube.STG_UserCubeSummary_UPDATE.Checkins,
  CheckInsHeadCount = EventCube.STG_UserCubeSummary_UPDATE.CheckInsHeadCount,
  Ratings = EventCube.STG_UserCubeSummary_UPDATE.Ratings,
  Reviews = EventCube.STG_UserCubeSummary_UPDATE.Reviews,
  Surveys = EventCube.STG_UserCubeSummary_UPDATE.Surveys,
  Updated = CURRENT_TIMESTAMP 
FROM EventCube.STG_UserCubeSummary_UPDATE
WHERE EventCube.STG_UserCubeSummary_UPDATE.UserId = ucs.UserId;

--======================================================================================================================================================--

--CREATE INDEX ndx_ecs_usercubesummary ON EventCube.UserCubeSummary (UserId);
--CREATE INDEX ndx_ecs_usercubesummary_applicationid ON EventCube.UserCubeSummary (ApplicationId);
--CREATE INDEX ndx_ecs_usercubesummary_applicationid_binaryversion ON EventCube.UserCubeSummary (ApplicationId, BinaryVersion);