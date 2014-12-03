IF OBJECT_ID('ReportingDB.dbo.UserDateSpine','U') IS NOT NULL
  DROP TABLE  ReportingDB.dbo.UserDateSpine

SELECT DISTINCT ApplicationId, GlobalUserId, UserId, Date
INTO ReportingDB.dbo.UserDateSpine
FROM
( SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactSessions UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactPosts UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactLikes UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactComments UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactBookmarks UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactFollows UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactCheckIns UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactRatings UNION
  SELECT DISTINCT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) Date FROM ReportingDB.dbo.FactSurveys
) U

IF OBJECT_ID('ReportingDB.dbo.UserCubeDaily','U') IS NOT NULL
  DROP TABLE  ReportingDB.dbo.UserCubeDaily

SELECT U.ApplicationId, ISNULL(Name,'???') Name, StartDate, EndDate,
ISNULL(OpenEvent,-1) OpenEvent, ISNULL(LeadScanning,-1) LeadScanning, ISNULL(SurveysOn,-1) SurveysOn, ISNULL(InteractiveMap,-1) InteractiveMap, ISNULL(Leaderboard,-1) Leaderboard, ISNULL(Bookmarking,-1) Bookmarking, ISNULL(Photofeed,-1) Photofeed, ISNULL(AttendeesList,-1) AttendeesList, ISNULL(QRCode,-1) QRCode, ISNULL(ExhibitorReqInfo,-1) ExhibitorReqInfo, ISNULL(ExhibitorMsg,-1) ExhibitorMsg, ISNULL(PrivateMsging,-1) PrivateMsging, ISNULL(PeopleMatching,-1) PeopleMatching, ISNULL(SocialNetworks,-1) SocialNetworks, ISNULL(RatingsOn,-1) RatingsOn,
ISNULL(BinaryVersion,'v???') BinaryVersion, ISNULL(DeviceType,'???') DeviceType, ISNULL(Device,'???') Device, ISNULL(Facebook,0) Facebook, ISNULL(Twitter,0) Twitter, ISNULL(LinkedIn,0) LinkedIn, GlobalUserId, U.UserId, U.Date,
CASE WHEN Sessions >= 10 THEN 1 ELSE 0 END Active,
ISNULL(Sessions,0) Sessions, ISNULL(Posts,0) Posts, ISNULL(PostsImage,0) PostsImage, ISNULL(PostsItem,0) PostsItem, ISNULL(Likes,0) Likes, ISNULL(Comments,0) Comments, ISNULL(Bookmarks,0) Bookmarks, ISNULL(Follows,0) Follows, ISNULL(CheckIns,0) CheckIns, ISNULL(CheckInsHeadcount,0) CheckInsHeadcount, ISNULL(Ratings,0) Ratings, ISNULL(Reviews,0) Reviews, ISNULL(Surveys,0) Surveys
INTO ReportingDB.dbo.UserCubeDaily
FROM ReportingDB.dbo.UserDateSpine U
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Sessions FROM ReportingDB.dbo.FactSessions GROUP BY UserId, CAST(Timestamp AS DATE)) S ON U.UserId = S.UserId AND U.Date = S.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Posts, SUM(HasImage) PostsImage, SUM(CASE WHEN ListType != 'Regular' THEN 1 ELSE 0 END) PostsItem FROM ReportingDB.dbo.FactPosts GROUP BY UserId, CAST(Timestamp AS DATE)) P ON U.UserId = P.UserId AND U.Date = P.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Likes FROM ReportingDB.dbo.FactLikes GROUP BY UserId, CAST(Timestamp AS DATE)) L ON U.UserId = L.UserId AND U.Date = L.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Comments FROM ReportingDB.dbo.FactComments GROUP BY UserId, CAST(Timestamp AS DATE)) C ON U.UserId = C.UserId AND U.Date = C.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Bookmarks FROM ReportingDB.dbo.FactBookmarks GROUP BY UserId, CAST(Timestamp AS DATE)) B ON U.UserId = B.UserId AND U.Date = B.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Follows FROM ReportingDB.dbo.FactFollows GROUP BY UserId, CAST(Timestamp AS DATE)) F ON U.UserId = F.UserId AND U.Date = F.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) CheckIns, SUM(IsHeadcount) CheckInsHeadcount FROM ReportingDB.dbo.FactCheckIns GROUP BY UserId, CAST(Timestamp AS DATE)) K ON U.UserId = K.UserId AND U.Date = K.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Ratings, SUM(HasReview) Reviews FROM ReportingDB.dbo.FactRatings GROUP BY UserId, CAST(Timestamp AS DATE)) R ON U.UserId = R.UserId AND U.Date = R.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Surveys FROM ReportingDB.dbo.FactSurveys GROUP BY UserId, CAST(Timestamp AS DATE)) V ON U.UserId = V.UserId AND U.Date = V.Date
LEFT OUTER JOIN ReportingDB.dbo.DimUserBinaryVersion N ON U.UserId = N.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUserDeviceType D ON U.UserId = D.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUserSocialNetworks O ON U.UserId = O.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimEvents E ON U.ApplicationId = E.ApplicationId
