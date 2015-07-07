DROP TABLE IF EXISTS EventCube.UserCubeSummary;  

--===================================================================================================
-- * Upstream dependent on creation of all Dimension and Fact tables in ReportingDB. 
-- Creates an aggregate at the User level with Application-level fields for slicing.
--===================================================================================================

<<<<<<< HEAD
CREATE TABLE EventCube.UserCubeSummary AS
SELECT 
U.ApplicationId, 
COALESCE(Name,'???') AS Name, 
CASE WHEN SF.SF_EventStartDate IS NOT NULL THEN CAST(SF.SF_EventStartDate AS DATE) ELSE E.StartDate END AS StartDate, 
CASE WHEN SF.SF_EventEndDate IS NOT NULL THEN CAST(SF.SF_EventEndDate AS DATE) ELSE E.EndDate END AS EndDate,
COALESCE(OpenEvent,-1) AS OpenEvent, 
COALESCE(LeadScanning,-1) AS LeadScanning, 
COALESCE(SurveysOn,-1) AS SurveysOn, 
COALESCE(InteractiveMap,-1) AS InteractiveMap, 
COALESCE(Leaderboard,-1) AS Leaderboard, 
COALESCE(Bookmarking,-1) AS Bookmarking, 
COALESCE(Photofeed,-1) AS Photofeed, 
COALESCE(AttendeesList,-1) AS AttendeesList, 
COALESCE(QRCode,-1) AS QRCode, 
COALESCE(ExhibitorReqInfo,-1) AS ExhibitorReqInfo, 
COALESCE(ExhibitorMsg,-1) AS ExhibitorMsg, 
COALESCE(PrivateMsging,-1) AS PrivateMsging, 
COALESCE(PeopleMatching,-1) AS PeopleMatching, 
COALESCE(SocialNetworks,-1) AS SocialNetworks, 
COALESCE(RatingsOn,-1) AS RatingsOn,
COALESCE(SF.EventType,'_Unknown') AS EventType, 
COALESCE(SF.EventSize,'_Unknown') AS EventSize, 
COALESCE(SF.AccountCustomerDomain,'_Unknown') AS AccountCustomerDomain, 
COALESCE(SF.ServiceTierName,'_Unknown') AS ServiceTierName, 
COALESCE(SF.App365Indicator,'_Unknown') AS App365Indicator, 
COALESCE(SF.SF_OwnerName,'_Unknown') AS OwnerName,
COALESCE(BinaryVersion,'v???') AS BinaryVersion, 
COALESCE(DeviceType,'???') AS DeviceType, 
COALESCE(Device,'???') AS Device, 
COALESCE(Facebook,0) AS Facebook, 
COALESCE(Twitter,0) AS Twitter, 
COALESCE(LinkedIn,0) AS LinkedIn, 
U.GlobalUserId, 
U.UserId,
CASE WHEN Sessions >= 2 THEN 1 ELSE 0 END AS Active, 
CASE WHEN Sessions >= 10 THEN 1 ELSE 0 END AS Engaged,
COALESCE(Sessions,0) AS Sessions, 
COALESCE(Posts,0) AS Posts, 
COALESCE(PostsImage,0) AS PostsImage, 
COALESCE(PostsItem,0) AS PostsItem, 
COALESCE(Likes,0) AS Likes, 
COALESCE(Comments,0) AS Comments, 
COALESCE(Bookmarks,0) AS Bookmarks, 
COALESCE(Follows,0) AS Follows, 
COALESCE(CheckIns,0) AS CheckIns, 
COALESCE(CheckInsHeadcount,0) AS CheckInsHeadcount, 
COALESCE(Ratings,0) AS Ratings, 
COALESCE(Reviews,0) AS Reviews, 
COALESCE(Surveys,0) AS Surveys
FROM EventCube.DimUsers U
LEFT OUTER JOIN (SELECT User_Id AS UserId, COUNT(*) AS Sessions FROM EventCube.FactSessions GROUP BY User_Id) S ON U.UserId = S.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Posts, SUM(HasImage) PostsImage, SUM(CASE WHEN ListType != 'Regular' THEN 1 ELSE 0 END) PostsItem FROM EventCube.FactPosts GROUP BY UserId) P ON U.UserId = P.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Likes FROM EventCube.FactLikes GROUP BY UserId) L ON U.UserId = L.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Comments FROM EventCube.FactComments GROUP BY UserId) C ON U.UserId = C.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Bookmarks FROM EventCube.FactBookmarks GROUP BY UserId) B ON U.UserId = B.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Follows FROM EventCube.FactFollows GROUP BY UserId) F ON U.UserId = F.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS CheckIns, SUM(IsHeadcount) CheckInsHeadcount FROM EventCube.FactCheckIns GROUP BY UserId) K ON U.UserId = K.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Ratings, SUM(HasReview) Reviews FROM EventCube.FactRatings GROUP BY UserId) R ON U.UserId = R.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) AS Surveys FROM EventCube.FactSurveys GROUP BY UserId) V ON U.UserId = V.UserId
LEFT OUTER JOIN EventCube.DimUserBinaryVersion N ON U.UserId = N.UserId
LEFT OUTER JOIN EventCube.DimUserDeviceType D ON U.UserId = D.UserId
LEFT OUTER JOIN EventCube.DimUserSocialNetworks O ON U.UserId = O.UserId
LEFT OUTER JOIN EventCube.DimEvents E ON U.ApplicationId = E.ApplicationId
LEFT OUTER JOIN EventCube.DimEventsSFDC SF ON U.ApplicationId = CAST(SF.ApplicationId AS VARCHAR);
=======
SELECT U.ApplicationId, ISNULL(Name,'???') Name, 
CASE WHEN SF.SF_EventStartDate IS NOT NULL THEN SF.SF_EventStartDate ELSE E.StartDate END AS StartDate, 
CASE WHEN SF.SF_EventEndDate IS NOT NULL THEN SF.SF_EventEndDate ELSE E.EndDate END AS EndDate,
ISNULL(OpenEvent,-1) OpenEvent, 
ISNULL(LeadScanning,-1) LeadScanning, 
ISNULL(SurveysOn,-1) SurveysOn, 
ISNULL(InteractiveMap,-1) InteractiveMap, 
ISNULL(Leaderboard,-1) Leaderboard, 
ISNULL(Bookmarking,-1) Bookmarking, 
ISNULL(Photofeed,-1) Photofeed, 
ISNULL(AttendeesList,-1) AttendeesList, 
ISNULL(QRCode,-1) QRCode, 
ISNULL(ExhibitorReqInfo,-1) ExhibitorReqInfo, 
ISNULL(ExhibitorMsg,-1) ExhibitorMsg, 
ISNULL(PrivateMsging,-1) PrivateMsging, 
ISNULL(PeopleMatching,-1) PeopleMatching, 
ISNULL(SocialNetworks,-1) SocialNetworks, 
ISNULL(RatingsOn,-1) RatingsOn,
ISNULL(SF.EventType,'_Unknown') EventType, 
ISNULL(SF.EventSize,'_Unknown') EventSize, 
ISNULL(SF.AccountCustomerDomain,'_Unknown') AccountCustomerDomain, 
ISNULL(SF.ServiceTierName,'_Unknown') ServiceTierName, 
ISNULL(SF.App365Indicator,'_Unknown') App365Indicator, 
ISNULL(SF.SF_OwnerName,'_Unknown') AS OwnerName,
ISNULL(BinaryVersion,'v???') BinaryVersion, 
ISNULL(DeviceType,'???') DeviceType, 
ISNULL(Device,'???') Device, 
ISNULL(Facebook,0) Facebook, 
ISNULL(Twitter,0) Twitter, 
ISNULL(LinkedIn,0) LinkedIn, 
GlobalUserId, 
U.UserId,
U.FirstTimestamp,
U.LastTimestamp,
CASE WHEN Sessions >= 2 THEN 1 ELSE 0 END Active, 
CASE WHEN Sessions >= 10 THEN 1 ELSE 0 END Engaged,
ISNULL(Sessions,0) Sessions, 
ISNULL(Posts,0) Posts, 
ISNULL(PostsImage,0) PostsImage, 
ISNULL(PostsItem,0) PostsItem, 
ISNULL(Likes,0) Likes, 
ISNULL(Comments,0) Comments, 
ISNULL(Bookmarks,0) Bookmarks, 
ISNULL(Follows,0) Follows, 
ISNULL(CheckIns,0) CheckIns, 
ISNULL(CheckInsHeadcount,0) CheckInsHeadcount, 
ISNULL(Ratings,0) Ratings, 
ISNULL(Reviews,0) Reviews, 
ISNULL(Surveys,0) Surveys
INTO ReportingDB.dbo.UserCubeSummary
FROM ReportingDB.dbo.DimUsers U
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Sessions FROM ReportingDB.dbo.FactSessions GROUP BY UserId) S ON U.UserId = S.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Posts, SUM(HasImage) PostsImage, SUM(CASE WHEN ListType != 'Regular' THEN 1 ELSE 0 END) PostsItem FROM ReportingDB.dbo.FactPosts GROUP BY UserId) P ON U.UserId = P.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Likes FROM ReportingDB.dbo.FactLikes GROUP BY UserId) L ON U.UserId = L.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Comments FROM ReportingDB.dbo.FactComments GROUP BY UserId) C ON U.UserId = C.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Bookmarks FROM ReportingDB.dbo.FactBookmarks GROUP BY UserId) B ON U.UserId = B.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Follows FROM ReportingDB.dbo.FactFollows GROUP BY UserId) F ON U.UserId = F.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) CheckIns, SUM(IsHeadcount) CheckInsHeadcount FROM ReportingDB.dbo.FactCheckIns GROUP BY UserId) K ON U.UserId = K.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Ratings, SUM(HasReview) Reviews FROM ReportingDB.dbo.FactRatings GROUP BY UserId) R ON U.UserId = R.UserId
LEFT OUTER JOIN (SELECT UserId, COUNT(*) Surveys FROM ReportingDB.dbo.FactSurveys GROUP BY UserId) V ON U.UserId = V.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUserBinaryVersion N ON U.UserId = N.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUserDeviceType D ON U.UserId = D.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUserSocialNetworks O ON U.UserId = O.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimEvents E ON U.ApplicationId = E.ApplicationId
LEFT OUTER JOIN ReportingDB.dbo.DimEventsSFDC SF ON U.ApplicationId = SF.ApplicationId
>>>>>>> upstream/master

CREATE INDEX ndx_ecs_usercubesummary ON EventCube.UserCubeSummary (UserId);
CREATE INDEX ndx_ecs_usercubesummary_applicationid ON EventCube.UserCubeSummary (ApplicationId);
CREATE INDEX ndx_ecs_usercubesummary_applicationid_binaryversion ON EventCube.UserCubeSummary (ApplicationId, BinaryVersion);