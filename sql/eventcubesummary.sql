IF OBJECT_ID('ReportingDB.dbo.EventCubeSummary','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.EventCubeSummary

--==========================================================
-- Aggregation on the User Cube Summary at the Event level
-- * Upstream dependency on User Cube Summary
--==========================================================

SELECT S.ApplicationId, Name, StartDate, EndDate,
OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn,
EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName,
BinaryVersion, Registrants, ISNULL(Downloads,0) Downloads, Users, UsersActive, UsersEngaged, UsersFacebook, UsersTwitter, UsersLinkedIn, Sessions, Posts, PostsImage, PostsItem, Likes, Comments, Bookmarks, Follows, CheckIns, CheckInsHeadcount, Ratings, Reviews, Surveys,
ISNULL(PromotedPosts,0) PromotedPosts, ISNULL(GlobalPushNotifications,0) GlobalPushNotifications, ADOPTION_FOOL.Adoption, ISNULL(E.Exhibitors,0) Exhibitors, ISNULL(PC.Polls,0) Polls, ISNULL(PR.PollResponses,0) PollResponses
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
( SELECT U.ApplicationId, COUNT(DISTINCT U.UserId) Registrants
  FROM AuthDB.dbo.IS_Users U
  JOIN AuthDB.dbo.Applications A ON U.ApplicationId = A.ApplicationId
  WHERE U.IsDisabled = 0 AND A.CanRegister = 0
  GROUP BY U.ApplicationId
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
( SELECT U.ApplicationId,
  1.0*SUM(CASE WHEN S.ApplicationId IS NOT NULL AND S.UserId IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*) Adoption
  FROM
  ( SELECT DISTINCT U.ApplicationId, UserId
    FROM AuthDB.dbo.IS_Users U
    JOIN (SELECT DISTINCT ApplicationId FROM AuthDB.dbo.Applications WHERE CanRegister = 0) E
    ON U.ApplicationId = E.ApplicationId
    WHERE IsDisabled = 0
  ) U
  LEFT OUTER JOIN
  ( SELECT DISTINCT ApplicationId, UserId
    FROM AnalyticsDB.dbo.Sessions
  ) S
  ON U.ApplicationId = S.ApplicationId AND U.UserId = S.UserId
  GROUP BY U.ApplicationId
) ADOPTION_FOOL
ON S.ApplicationId = ADOPTION_FOOL.ApplicationId
LEFT OUTER JOIN
( SELECT i.applicationid ApplicationId, COUNT(DISTINCT itemid) Exhibitors
  FROM ratings.dbo.item i
  JOIN ratings.dbo.topic t ON i.parenttopicid = t.topicid
  WHERE listtypeid = 3 AND i.isdisabled = 0 AND IsArchived = 'false'
  GROUP BY i.applicationid
) E
ON E.ApplicationId = S.ApplicationId
LEFT OUTER JOIN
( SELECT S.ApplicationId, 1.0 * count(S.ApplicationId) Polls 
  FROM Ratings.dbo.Surveys S 
  WHERE S.IsPoll = 'true'
  GROUP BY S.ApplicationId
) PC
ON PC.ApplicationId = S.ApplicationId
LEFT OUTER JOIN
( SELECT s.ApplicationId, 1.0 * COUNT(sr.SurveyResponseId) PollResponses
  FROM Ratings.dbo.SurveyResponses sr
  JOIN Ratings.dbo.SurveyQuestions sq ON sr.SurveyQuestionId = sq.SurveyQuestionId
  JOIN Ratings.dbo.Surveys s ON sq.SurveyId = s.SurveyId
  WHERE s.IsPoll = 'true'
  GROUP BY s.ApplicationId
) PR
ON PR.ApplicationId = S.ApplicationId
