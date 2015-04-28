IF OBJECT_ID('ReportingDB.dbo.EventCubeSummary','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.EventCubeSummary

--==========================================================
-- Aggregation on the User Cube Summary at the Event level
-- * Upstream dependency on User Cube Summary
--==========================================================

SELECT S.ApplicationId, Name, StartDate, EndDate,
OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn,
EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName,
BinaryVersion,
ISNULL(Registrants,0) Registrants, ISNULL(Downloads,0) Downloads, Users, UsersActive, UsersEngaged, UsersFacebook, UsersTwitter, UsersLinkedIn, Sessions, Posts, PostsImage, PostsItem, Likes, Comments, Bookmarks, Follows, CheckIns, CheckInsHeadcount, Ratings, Reviews, Surveys,
ISNULL(PromotedPosts,0) PromotedPosts, ISNULL(GlobalPushNotifications,0) GlobalPushNotifications, A.Adoption, E.Exhibitors
INTO ReportingDB.dbo.EventCubeSummary
FROM
( SELECT S.ApplicationId, Name, StartDate, EndDate,
  OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn,
  EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName,
  B.BinaryVersion,
  COUNT(*) Users, SUM(Active) UsersActive, SUM(Engaged) UsersEngaged, SUM(Facebook) UsersFacebook, SUM(Twitter) UsersTwitter, SUM(LinkedIn) UsersLinkedIn,
  SUM(Sessions) Sessions, SUM(Posts) Posts, SUM(PostsImage) PostsImage, SUM(PostsItem) PostsItem, SUM(Likes) Likes, SUM(Comments) Comments, SUM(Bookmarks) Bookmarks, SUM(Follows) Follows, SUM(CheckIns) CheckIns, SUM(CheckInsHeadcount) CheckInsHeadcount, SUM(Ratings) Ratings, SUM(Reviews) Reviews, SUM(Surveys) Surveys
  FROM ReportingDB.dbo.UserCubeSummary S
  JOIN ReportingDB.dbo.DimEventBinaryVersion B ON S.ApplicationId = B.Applicationid
  GROUP BY S.ApplicationId, Name, StartDate, EndDate,
  OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn,
  EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName, B.BinaryVersion
) S
LEFT OUTER JOIN
( SELECT ApplicationId, COUNT(DISTINCT UserId) Registrants
  FROM AuthDB.dbo.IS_Users
  WHERE IsDisabled = 0
  GROUP BY ApplicationId
) R
ON S.ApplicationId = R.ApplicationId
LEFT OUTER JOIN
( SELECT ApplicationId, COUNT(DISTINCT DeviceId) Downloads
  FROM AnalyticsDB.dbo.Sessions
  GROUP BY ApplicationId
) D
ON S.ApplicationId = D.ApplicationId
LEFT OUTER JOIN
( SELECT ApplicationId, COUNT(*) PromotedPosts
  FROM Ratings.dbo.PromotedPosts
  GROUP BY ApplicationId
) P
ON S.ApplicationId = P.ApplicationId
LEFT OUTER JOIN
( SELECT ApplicationId, COUNT(*) GlobalPushNotifications
  FROM Ratings.dbo.GlobalMessages
  GROUP BY ApplicationId
) G
ON S.ApplicationId = G.ApplicationId
LEFT OUTER JOIN
( SELECT ApplicationId, round(1.0 * SUM(User_Ind)/COUNT(*),2) * 100 AS Adoption, 
  CASE WHEN COUNT(*) < SUM(User_Ind) THEN 1 ELSE 0 END AS Error_Ind
  FROM (
  SELECT 
  base.UserId,
  ecs.ApplicationId,
  CASE WHEN s.UserId IS NOT NULL THEN 1 ELSE 0 END AS User_Ind
  FROM AuthDB.dbo.IS_Users base
  JOIN ReportingDB.dbo.EventCubeSummary ecs ON base.ApplicationId = ecs.ApplicationId
  LEFT JOIN (SELECT DISTINCT ApplicationId, UserId FROM AnalyticsDB.dbo.Sessions) s ON CAST(base.UserId AS INT) = s.UserId AND base.ApplicationId = s.ApplicationId
  WHERE base.IsDisabled = 0
  ) t 
  GROUP BY ApplicationId
) A  
ON S.ApplicationId = A.ApplicationId
LEFT OUTER JOIN
( SELECT i.applicationid ApplicationId, COUNT(DISTINCT itemid) Exhibitors
  FROM ratings.dbo.item i
  JOIN ratings.dbo.topic t ON i.parenttopicid = t.topicid
  WHERE listtypeid = 3 AND i.isdisabled = 0 AND IsArchived = 'false'
  GROUP BY i.applicationid
) E
ON E.ApplicationId = S.ApplicationId
 
 
 