DROP TABLE IF EXISTS EventCube.EventCubeSummary;  

--==========================================================
-- Aggregation on the User Cube Summary at the Event level
-- * Upstream dependency on User Cube Summary
--==========================================================

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
( 
        SELECT 
                ApplicationId, 
                COUNT(*) GlobalPushNotifications
        FROM PUBLIC.Ratings_GlobalMessages
        GROUP BY ApplicationId
) G ON S.ApplicationId = G.ApplicationId;