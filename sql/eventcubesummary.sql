--==========================================================
-- Increase PG Timeout Window to 180 Minutes
--==========================================================

SET statement_timeout = '180 min';
COMMIT;

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
        BV.BinaryVersion,
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
        DirectMessaging,
        TopicChannel,
        ExhibitorReqInfo, 
        ExhibitorMsg, 
        PrivateMsging, 
        PeopleMatching, 
        SocialNetworks, 
        RatingsOn,
        NativeSessionNotes,
        SessionChannel,
        SessionRecommendations,
        PeopleRecommendations,
        
        --== SalesForce Metadata
        EventType, 
        EventSize, 
        AccountCustomerDomain, 
        ServiceTierName, 
        App365Indicator, 
        OwnerName,
        
        --== Device Fact Aggregates
        COALESCE(Registrants,0) AS Registrants, 
        COALESCE(D.Devices,0) AS UniqueDevices, 
        
        --== User Fact Aggregates
        Users, 
        UsersActive, 
        UsersEngaged,
        UsersFacebook, 
        UsersTwitter, 
        UsersLinkedIn, 
        
        --== Fact Data
        Sessions, 
        EventSessions,
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
        COALESCE(AGENDA_SESSIONS.TotalAgendaSessions,0) AS TotalAgendaSessions,
        COALESCE(PC.Polls,0) AS Polls, 
        COALESCE(PR.PollResponses,0) AS PollResponses,
        COALESCE(TC.TopicChannelCnt,0) AS TopicChannelCnt,
        NULL AS DirectMessagingSentCnt,
        NULL AS TopicChannelMsgSentCnt,
        NULL AS SessionChannelMsgSentCnt,
        
        --== Calculated Rates
        ADOPTION_FOOL.Adoption,
        CASE WHEN UsersActive > 0 THEN EventSessions/(UsersActive * (EndDate - StartDate + 1)) ELSE NULL END AS EventSessionsPerUsersPerDay,

        --== Leads Scanned
        COALESCE(LEADS_SCANNED.ScanningExhibitors,0) AS ScanningExhibitors,
        COALESCE(LEADS_SCANNED.LeadsScannedTotal,0) AS LeadsScannedTotal,
        COALESCE(LEADS_SCANNED.LeadsScannedUnique,0) AS LeadsScannedUnique

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
                DirectMessaging,
                TopicChannel,                
                ExhibitorReqInfo, 
                ExhibitorMsg, 
                PrivateMsging, 
                PeopleMatching, 
                SocialNetworks, 
                RatingsOn,
                NativeSessionNotes,
                SessionChannel,
                SessionRecommendations,
                PeopleRecommendations,
                EventType, 
                EventSize, 
                AccountCustomerDomain, 
                ServiceTierName, 
                App365Indicator, 
                OwnerName,
                -- COUNT(CASE WHEN ISU.UserId IS NOT NULL THEN 1 ELSE NULL END) AS Users, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Active ELSE 0 END) AS Users,
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Active ELSE 0 END) AS UsersActive, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Engaged ELSE 0 END) UsersEngaged,
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Facebook ELSE 0 END) AS UsersFacebook, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Twitter ELSE 0 END) AS UsersTwitter, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN LinkedIn ELSE 0 END) AS UsersLinkedIn,
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Sessions ELSE 0 END) AS Sessions, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN EventSessions ELSE 0 END) AS EventSessions, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Posts ELSE 0 END) AS Posts, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN PostsImage ELSE 0 END) AS PostsImage, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN PostsItem ELSE 0 END) AS PostsItem, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Likes ELSE 0 END) AS Likes, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Comments ELSE 0 END) AS Comments, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN TotalBookmarks ELSE 0 END) AS TotalBookmarks, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN ImportedBookmarks ELSE 0 END) AS ImportedBookmarks, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Follows ELSE 0 END) AS Follows, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN CheckIns ELSE 0 END) AS CheckIns, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN CheckInsHeadcount ELSE 0 END) AS CheckInsHeadcount, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Ratings ELSE 0 END) AS Ratings, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Reviews ELSE 0 END) AS Reviews, 
                SUM(CASE WHEN ISU.UserId IS NOT NULL THEN Surveys ELSE 0 END) AS Surveys
        FROM EventCube.UserCubeSummary S
        LEFT JOIN (SELECT DISTINCT ApplicationId,
                              UserId
              FROM Public.AuthDB_IS_Users
              WHERE IsDisabled = 0) ISU
        ON S.ApplicationId = ISU.ApplicationID AND S.UserId = ISU.UserId
        GROUP BY S.ApplicationId, Name, StartDate, EndDate, OpenEvent, LeadScanning, SurveysOn, InteractiveMap, Leaderboard, Bookmarking, Photofeed, AttendeesList, QRCode, DirectMessaging, TopicChannel, ExhibitorReqInfo, ExhibitorMsg, PrivateMsging, PeopleMatching, SocialNetworks, RatingsOn, NativeSessionNotes, SessionChannel, SessionRecommendations, PeopleRecommendations, EventType, EventSize, AccountCustomerDomain, ServiceTierName, App365Indicator, OwnerName
        
) S
--== Get the Binary Version that was the majority
JOIN EventCube.V_DimEventBinaryVersion BV ON S.ApplicationId = BV.Applicationid
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
LEFT OUTER JOIN EventCube.Agg_Devices_per_App D ON S.ApplicationId = D.ApplicationId
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
--== Channels Count
LEFT OUTER JOIN
(
        SELECT 
                UPPER(ApplicationId) ApplicationId
              , COUNT(*) TopicChannelCnt
        FROM Channels.Rooms
        WHERE Type = 'TOPIC'
        GROUP BY ApplicationId
) TC ON S.ApplicationId = TC.ApplicationId
--== Leads Scanned
LEFT OUTER JOIN
( SELECT
    I.ApplicationId,
    COUNT(DISTINCT L.ItemId) ScanningExhibitors,
    COUNT(*) LeadsScannedTotal,
    COUNT(DISTINCT L.UserId) LeadsScannedUnique
  FROM PUBLIC.Ratings_Leads L
  JOIN PUBLIC.Ratings_Item I
  ON L.ItemId = I.ItemId
  WHERE L.Source = 1
  GROUP BY 1
) LEADS_SCANNED ON E.ApplicationId = LEADS_SCANNED.ApplicationId
--== Agenda Sessions
LEFT OUTER JOIN 
(  SELECT 
     i.ApplicationId, 
     COUNT(DISTINCT ItemId) AS TotalAgendaSessions
   FROM PUBLIC.Ratings_Item i
   JOIN PUBLIC.Ratings_Topic t ON i.ParentTopicId = t.TopicId
   WHERE ListTypeId = 2 AND i.IsDisabled = 0 AND i.IsArchived = 'false'
   group by applicationid
) AGENDA_SESSIONS E.ApplicationId = AGENDA_SESSIONS.ApplicationId
;

-- Create the View for Reporter user 
CREATE OR REPLACE VIEW report.v_eventcubesummary AS
SELECT * FROM EventCube.EventCubeSummary;

-- Grant usage
GRANT USAGE ON SCHEMA eventcube TO integrations;
GRANT SELECT ON ALL TABLES IN SCHEMA eventcube TO integrations;

