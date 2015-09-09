--==========================================================
-- Aggregation on the User Cube Summary at the Event level
-- * Upstream dependency on User Cube Summary
--==========================================================

TRUNCATE TABLE EventCube.EventCubeSummary;
VACUUM EventCube.EventCubeSummary;
INSERT INTO EventCube.EventCubeSummary
SELECT 
        --== Application Metadata
        S.ApplicationId, 
        Name, 
        StartDate, 
        EndDate,
        BinaryVersion,
        OpenEvent, 
        
        --== Feature Indicators
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
        
        --== SalesForce Metadata
        EventType, 
        EventSize, 
        AccountCustomerDomain, 
        ServiceTierName, 
        App365Indicator, 
        OwnerName,
        
        --== Device Fact Aggregates
        COALESCE(Registrants,0) AS Registrants, 
        COALESCE(UniqueDevices,0) AS UniqueDevices, 
        
        --== User Fact Aggregates
        Users, 
        UsersActive, 
        UsersEngaged,
        UsersFacebook, 
        UsersTwitter, 
        UsersLinkedIn, 
        
        --== Fact Data
        Sessions, 
        Posts, 
        PostsImage, 
        PostsItem, 
        Likes, 
        Comments, 
        TotalBookmarks, 
        ImportedBookmarks,
        Follows, 
        CheckIns, 
        CheckInsHeadcount, 
        Ratings, 
        Reviews, 
        Surveys,
        COALESCE(PromotedPosts,0) AS PromotedPosts, 
        COALESCE(GlobalPushNotifications,0) AS GlobalPushNotifications,
        COALESCE(E.Exhibitors,0) AS Exhibitors, 
        COALESCE(PC.Polls,0) AS Polls, 
        COALESCE(PR.PollResponses,0) AS PollResponses,
        
        --== Calculated Rates
        ADOPTION_FOOL.Adoption
FROM
--== Basic Aggregate from UserCubeSummary
(       
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
                B.BinaryVersion,
                COUNT(*) AS Users, 
                SUM(Active) AS UsersActive, 
                SUM(Engaged) UsersEngaged,
                SUM(Facebook) AS UsersFacebook, 
                SUM(Twitter) AS UsersTwitter, 
                SUM(LinkedIn) AS UsersLinkedIn,
                SUM(Sessions) AS Sessions, 
                SUM(Posts) AS Posts, 
                SUM(PostsImage) AS PostsImage, 
                SUM(PostsItem) AS PostsItem, 
                SUM(Likes) AS Likes, 
                SUM(Comments) AS Comments, 
                SUM(TotalBookmarks) AS TotalBookmarks, 
                SUM(ImportedBookmarks) AS ImportedBookmarks, 
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
--== Get the Registrant Count (Users listed in the App, for Closed Events)
LEFT OUTER JOIN
(
        SELECT 
                u.ApplicationId, 
                COUNT(DISTINCT u.UserId) AS Registrants
        FROM PUBLIC.AuthDB_IS_Users u
        JOIN PUBLIC.AuthDB_Applications a ON u.ApplicationId = a.ApplicationId
        WHERE u.IsDisabled = 0 AND a.CanRegister = 'false'
        GROUP BY u.ApplicationId
) R ON S.ApplicationId = R.ApplicationId
--== Get the count of Unique Devices that have accessed the Event App
LEFT OUTER JOIN
( 
        SELECT Application_Id AS ApplicationId, COUNT(*) AS UniqueDevices 
        FROM (
                SELECT DISTINCT Device_Id, Application_Id FROM PUBLIC.Fact_Sessions_Old
        ) t GROUP BY Application_Id
) D ON S.ApplicationId = D.ApplicationId
--========================================================================================================================
--
--== Additional Event-Level Fact Data
--
--========================================================================================================================
--== PROMOTED POSTS
LEFT OUTER JOIN
( 
        SELECT 
                ApplicationId, 
                COUNT(*) PromotedPosts
        FROM PUBLIC.Ratings_PromotedPosts
        GROUP BY ApplicationId
) P ON S.ApplicationId = P.ApplicationId
--== GLOBAL MESSAGES
LEFT OUTER JOIN
( 
        SELECT 
                ApplicationId, 
                COUNT(*) GlobalPushNotifications
        FROM PUBLIC.Ratings_GlobalMessages
        GROUP BY ApplicationId
) G ON S.ApplicationId = G.ApplicationId
--== Adoption Percentage
LEFT OUTER JOIN
( 
        SELECT 
                U.ApplicationId,
                1.0*SUM(CASE WHEN S.ApplicationId IS NOT NULL AND S.UserId IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*) Adoption
        FROM
        ( 
                SELECT 
                        U.ApplicationId, 
                        U.UserId
                FROM PUBLIC.AuthDB_IS_Users U
                JOIN (SELECT ApplicationId FROM PUBLIC.AuthDB_Applications WHERE CanRegister = false) E ON U.ApplicationId = E.ApplicationId
                WHERE IsDisabled = 0
        ) U
        LEFT OUTER JOIN
        (SELECT ApplicationId, UserId FROM EventCube.Agg_Session_Per_AppUser) S ON U.ApplicationId = S.ApplicationId AND U.UserId = S.UserId
        GROUP BY U.ApplicationId
) ADOPTION_FOOL ON S.ApplicationId = ADOPTION_FOOL.ApplicationId
--== Exhibitors
LEFT OUTER JOIN
( 
        SELECT 
                i.ApplicationId, 
                COUNT(DISTINCT ItemId) AS Exhibitors
        FROM PUBLIC.Ratings_Item i
        JOIN PUBLIC.Ratings_Topic t ON i.ParentTopicId = t.TopicId
        WHERE ListTypeId = 3 AND i.IsDisabled = 0 AND i.IsArchived = 'false'
        GROUP BY i.ApplicationId
) E ON E.ApplicationId = S.ApplicationId
--== Polls Set Up
LEFT OUTER JOIN
( 
        SELECT 
                S.ApplicationId, 
                1.0 * count(S.ApplicationId) Polls 
        FROM PUBLIC.Ratings_Surveys S 
        WHERE S.IsPoll = 'true'
        GROUP BY S.ApplicationId
) PC ON PC.ApplicationId = S.ApplicationId
--== Polls Responses
LEFT OUTER JOIN
( 
        SELECT 
                s.ApplicationId, 
                1.0 * COUNT(sr.SurveyResponseId) PollResponses
        FROM PUBLIC.Ratings_SurveyResponses sr
        JOIN PUBLIC.Ratings_SurveyQuestions sq ON sr.SurveyQuestionId = sq.SurveyQuestionId
        JOIN PUBLIC.Ratings_Surveys s ON sq.SurveyId = s.SurveyId
        WHERE s.IsPoll = 'true'
        GROUP BY s.ApplicationId
) PR ON PR.ApplicationId = S.ApplicationId
;

-- Create the View for Reporter user 
CREATE OR REPLACE VIEW report.v_eventcubesummary AS
SELECT * FROM EventCube.EventCubeSummary;

-- Grant usage
GRANT USAGE ON SCHEMA eventcube TO integrations;
GRANT SELECT ON ALL TABLES IN SCHEMA eventcube TO integrations;

