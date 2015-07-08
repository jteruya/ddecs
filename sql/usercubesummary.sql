DROP TABLE IF EXISTS EventCube.UserCubeSummary;  

--===================================================================================================
-- * Upstream dependent on creation of all Dimension and Fact tables in ReportingDB. 
-- Creates an aggregate at the User level with Application-level fields for slicing.
--===================================================================================================

CREATE TABLE EventCube.UserCubeSummary AS
SELECT 
U.ApplicationId, 
COALESCE(Name,'???') AS Name, 
CASE WHEN SF.SF_EventStartDate IS NOT NULL AND SF.SF_EventStartDate <> '' THEN CAST(SF.SF_EventStartDate AS DATE) ELSE E.StartDate END AS StartDate, 
CASE WHEN SF.SF_EventEndDate IS NOT NULL AND SF.SF_EventEndDate <> '' THEN CAST(SF.SF_EventEndDate AS DATE) ELSE E.EndDate END AS EndDate,
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
U.FirstTimestamp,
U.LastTimestamp,
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
LEFT OUTER JOIN EventCube.DimEventsSFDC SF ON LOWER(U.ApplicationId) = LOWER(CAST(SF.ApplicationId AS VARCHAR));

CREATE INDEX ndx_ecs_usercubesummary ON EventCube.UserCubeSummary (UserId);
CREATE INDEX ndx_ecs_usercubesummary_applicationid ON EventCube.UserCubeSummary (ApplicationId);
CREATE INDEX ndx_ecs_usercubesummary_applicationid_binaryversion ON EventCube.UserCubeSummary (ApplicationId, BinaryVersion);