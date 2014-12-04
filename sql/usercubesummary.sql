IF OBJECT_ID('ReportingDB.dbo.UserCubeSummary','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.UserCubeSummary

--===================================================================================================
-- * Upstream dependent on creation of all Dimension and Fact tables in ReportingDB. 
-- Creates an aggregate at the User level with Application-level fields for slicing.
--===================================================================================================

SELECT U.ApplicationId, ISNULL(Name,'???') Name, StartDate, EndDate,
ISNULL(OpenEvent,-1) OpenEvent, ISNULL(LeadScanning,-1) LeadScanning, ISNULL(SurveysOn,-1) SurveysOn, ISNULL(InteractiveMap,-1) InteractiveMap, ISNULL(Leaderboard,-1) Leaderboard, ISNULL(Bookmarking,-1) Bookmarking, ISNULL(Photofeed,-1) Photofeed, ISNULL(AttendeesList,-1) AttendeesList, ISNULL(QRCode,-1) QRCode, ISNULL(ExhibitorReqInfo,-1) ExhibitorReqInfo, ISNULL(ExhibitorMsg,-1) ExhibitorMsg, ISNULL(PrivateMsging,-1) PrivateMsging, ISNULL(PeopleMatching,-1) PeopleMatching, ISNULL(SocialNetworks,-1) SocialNetworks, ISNULL(RatingsOn,-1) RatingsOn,
ISNULL(BinaryVersion,'v???') BinaryVersion, ISNULL(DeviceType,'???') DeviceType, ISNULL(Device,'???') Device, ISNULL(Facebook,0) Facebook, ISNULL(Twitter,0) Twitter, ISNULL(LinkedIn,0) LinkedIn, GlobalUserId, U.UserId,
CASE WHEN Sessions >= 10 THEN 1 ELSE 0 END Active,
ISNULL(Sessions,0) Sessions, ISNULL(Posts,0) Posts, ISNULL(PostsImage,0) PostsImage, ISNULL(PostsItem,0) PostsItem, ISNULL(Likes,0) Likes, ISNULL(Comments,0) Comments, ISNULL(Bookmarks,0) Bookmarks, ISNULL(Follows,0) Follows, ISNULL(CheckIns,0) CheckIns, ISNULL(CheckInsHeadcount,0) CheckInsHeadcount, ISNULL(Ratings,0) Ratings, ISNULL(Reviews,0) Reviews, ISNULL(Surveys,0) Surveys
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
