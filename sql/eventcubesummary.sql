DROP TABLE IF EXISTS EventCube.EventCubeSummary;  

--==========================================================
-- Aggregation on the User Cube Summary at the Event level
-- * Upstream dependency on User Cube Summary
--==========================================================

<<<<<<< HEAD
CREATE TABLE EventCube.EventCubeSummary AS
SELECT 
        S.ApplicationId, 
        Name, 
        StartDate, 
        EndDate,
        OpenEvent, 
        LeadScanning, 
        SurveysOn, 
        InteractiveMap, 
        Leaderboard, 
        Bookmarking, 
        Photofeed, 
        AttendeesList, 
        QRCode, 
        ExhibitorReqInfo, 
        ExhibitorMsg, 
        PrivateMsging, 
        PeopleMatching, 
        SocialNetworks, 
        RatingsOn,
        EventType, 
        EventSize, 
        AccountCustomerDomain, 
        ServiceTierName, 
        App365Indicator, 
        OwnerName,
        BinaryVersion,
        COALESCE(Registrants,0) AS Registrants, 
        --COALESCE(Downloads,0) AS Downloads, 
        Users, 
        UsersActive, 
        UsersFacebook, 
        UsersTwitter, 
        UsersLinkedIn, 
        Sessions, 
        Posts, 
        PostsImage, 
        PostsItem, 
        Likes, 
        Comments, 
        Bookmarks, 
        Follows, 
        CheckIns, 
        CheckInsHeadcount, 
        Ratings, 
        Reviews, 
        Surveys,
        COALESCE(PromotedPosts,0) AS PromotedPosts, 
        COALESCE(GlobalPushNotifications,0) AS GlobalPushNotifications
FROM
(       SELECT 
                S.ApplicationId, 
                Name, 
                StartDate, 
                EndDate,
                OpenEvent, 
                LeadScanning, 
                SurveysOn, 
                InteractiveMap, 
                Leaderboard, 
                Bookmarking, 
                Photofeed, 
                AttendeesList, 
                QRCode, 
                ExhibitorReqInfo, 
                ExhibitorMsg, 
                PrivateMsging, 
                PeopleMatching, 
                SocialNetworks, 
                RatingsOn,
                EventType, 
                EventSize, 
                AccountCustomerDomain, 
                ServiceTierName, 
                App365Indicator, 
                OwnerName,
                B.BinaryVersion,
                COUNT(*) AS Users, 
                SUM(Active) AS UsersActive, 
                SUM(Facebook) AS UsersFacebook, 
                SUM(Twitter) AS UsersTwitter, 
                SUM(LinkedIn) AS UsersLinkedIn,
                SUM(Sessions) AS Sessions, 
                SUM(Posts) AS Posts, 
                SUM(PostsImage) AS PostsImage, 
                SUM(PostsItem) AS PostsItem, 
                SUM(Likes) AS Likes, 
                SUM(Comments) AS Comments, 
                SUM(Bookmarks) AS Bookmarks, 
                SUM(Follows) AS Follows, 
                SUM(CheckIns) AS CheckIns, 
                SUM(CheckInsHeadcount) AS CheckInsHeadcount, 
                SUM(Ratings) AS Ratings, 
                SUM(Reviews) AS Reviews, 
                SUM(Surveys) AS Surveys
        FROM EventCube.UserCubeSummary S
        JOIN EventCube.DimEventBinaryVersion B ON S.ApplicationId = B.Applicationid
        GROUP BY S.ApplicationId, Name, StartDate, EndDate, OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn, EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName, B.BinaryVersion
) S
LEFT OUTER JOIN
( 
        SELECT 
                ApplicationId, 
                COUNT(DISTINCT UserId) AS Registrants
        FROM PUBLIC.AuthDB_IS_Users
        WHERE IsDisabled = 0
        GROUP BY ApplicationId
) R ON S.ApplicationId = R.ApplicationId
=======
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
>>>>>>> upstream/master
LEFT OUTER JOIN
( 
        SELECT Application_Id AS ApplicationId, COUNT(*) AS Downloads 
        FROM (
                SELECT DISTINCT Device_Id, Application_Id FROM PUBLIC.Fact_Sessions_Old
        ) t GROUP BY Application_Id
) D ON S.ApplicationId = D.ApplicationId
LEFT OUTER JOIN
( 
        SELECT 
                ApplicationId, 
                COUNT(*) PromotedPosts
        FROM PUBLIC.Ratings_PromotedPosts
        GROUP BY ApplicationId
) P ON S.ApplicationId = P.ApplicationId
LEFT OUTER JOIN
<<<<<<< HEAD
( 
        SELECT 
                ApplicationId, 
                COUNT(*) GlobalPushNotifications
        FROM PUBLIC.Ratings_GlobalMessages
        GROUP BY ApplicationId
) G ON S.ApplicationId = G.ApplicationId;
=======
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
>>>>>>> upstream/master
