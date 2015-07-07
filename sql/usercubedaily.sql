DROP TABLE IF EXISTS EventCube.UserDateSpine;  

CREATE TABLE EventCube.UserDateSpine AS
SELECT ApplicationId, GlobalUserId, UserId, Date
FROM
( SELECT ApplicationId, GlobalUserId, User_Id AS UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactSessions UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactPosts UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactLikes UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactComments UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactBookmarks UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactFollows UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactCheckIns UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactRatings UNION ALL
  SELECT ApplicationId, GlobalUserId, UserId, CAST(Timestamp AS DATE) AS Date FROM EventCube.FactSurveys
) U
GROUP BY 1,2,3,4;

DROP TABLE IF EXISTS EventCube.UserCubeDaily;  

CREATE TABLE EventCube.UserCubeDaily AS
EXPLAIN
SELECT U.ApplicationId, ISNULL(Name,'???') Name, StartDate, EndDate,
ISNULL(OpenEvent,-1) OpenEvent, ISNULL(LeadScanning,-1) LeadScanning, ISNULL(SurveysOn,-1) SurveysOn, ISNULL(InteractiveMap,-1) InteractiveMap, ISNULL(Leaderboard,-1) Leaderboard, ISNULL(Bookmarking,-1) Bookmarking, ISNULL(Photofeed,-1) Photofeed, ISNULL(AttendeesList,-1) AttendeesList, ISNULL(QRCode,-1) QRCode, ISNULL(ExhibitorReqInfo,-1) ExhibitorReqInfo, ISNULL(ExhibitorMsg,-1) ExhibitorMsg, ISNULL(PrivateMsging,-1) PrivateMsging, ISNULL(PeopleMatching,-1) PeopleMatching, ISNULL(SocialNetworks,-1) SocialNetworks, ISNULL(RatingsOn,-1) RatingsOn,
ISNULL(BinaryVersion,'v???') BinaryVersion, ISNULL(DeviceType,'???') DeviceType, ISNULL(Device,'???') Device, ISNULL(Facebook,0) Facebook, ISNULL(Twitter,0) Twitter, ISNULL(LinkedIn,0) LinkedIn, GlobalUserId, U.UserId, U.Date,
CASE WHEN Sessions >= 10 THEN 1 ELSE 0 END Active,
ISNULL(Sessions,0) Sessions, ISNULL(Posts,0) Posts, ISNULL(PostsImage,0) PostsImage, ISNULL(PostsItem,0) PostsItem, ISNULL(Likes,0) Likes, ISNULL(Comments,0) Comments, ISNULL(Bookmarks,0) Bookmarks, ISNULL(Follows,0) Follows, ISNULL(CheckIns,0) CheckIns, ISNULL(CheckInsHeadcount,0) CheckInsHeadcount, ISNULL(Ratings,0) Ratings, ISNULL(Reviews,0) Reviews, ISNULL(Surveys,0) Surveys
FROM EventCube.UserDateSpine U
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Sessions FROM EventCube.FactSessions GROUP BY UserId, CAST(Timestamp AS DATE)) S ON U.UserId = S.UserId AND U.Date = S.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Posts, SUM(HasImage) PostsImage, SUM(CASE WHEN ListType != 'Regular' THEN 1 ELSE 0 END) PostsItem FROM EventCube.FactPosts GROUP BY UserId, CAST(Timestamp AS DATE)) P ON U.UserId = P.UserId AND U.Date = P.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Likes FROM EventCube.FactLikes GROUP BY UserId, CAST(Timestamp AS DATE)) L ON U.UserId = L.UserId AND U.Date = L.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Comments FROM EventCube.FactComments GROUP BY UserId, CAST(Timestamp AS DATE)) C ON U.UserId = C.UserId AND U.Date = C.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Bookmarks FROM EventCube.FactBookmarks GROUP BY UserId, CAST(Timestamp AS DATE)) B ON U.UserId = B.UserId AND U.Date = B.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Follows FROM EventCube.FactFollows GROUP BY UserId, CAST(Timestamp AS DATE)) F ON U.UserId = F.UserId AND U.Date = F.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) CheckIns, SUM(IsHeadcount) CheckInsHeadcount FROM EventCube.FactCheckIns GROUP BY UserId, CAST(Timestamp AS DATE)) K ON U.UserId = K.UserId AND U.Date = K.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Ratings, SUM(HasReview) Reviews FROM EventCube.FactRatings GROUP BY UserId, CAST(Timestamp AS DATE)) R ON U.UserId = R.UserId AND U.Date = R.Date
LEFT OUTER JOIN (SELECT UserId, CAST(Timestamp AS DATE) Date, COUNT(*) Surveys FROM EventCube.FactSurveys GROUP BY UserId, CAST(Timestamp AS DATE)) V ON U.UserId = V.UserId AND U.Date = V.Date
LEFT OUTER JOIN EventCube.DimUserBinaryVersion N ON U.UserId = N.UserId
LEFT OUTER JOIN EventCube.DimUserDeviceType D ON U.UserId = D.UserId
LEFT OUTER JOIN EventCube.DimUserSocialNetworks O ON U.UserId = O.UserId
LEFT OUTER JOIN EventCube.DimEvents E ON U.ApplicationId = E.ApplicationId;
