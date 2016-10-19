
TRUNCATE TABLE RTERRY .CMSUserCube;

VACUUM  RTERRY.CMSUserCube;
INSERT INTO RTERRY.CMSUserCube

SELECT  UPPER(PAGES.globaluserid) AS GlobalUserId,
        UPPER(PAGES.applicationid) AS ApplicationID,
        RGD.emailaddress AS emailadress,
        RGD.firstname,
        RGD.lastname,
        MinEventLogin,
        MaxEventLogin,
        NumberOfDaysActive,
        TotalPageviews,
        EventPerformancePageViews,
        all_users,
        Activeusers_PerDay,
        Visitsperday_Summary,
        visitsperday_platform,
        visitsperuser,
        AttendeesScanned,
        Scanners,
        AppSectionVisits,
        bookmarks,
        ContentEngagementSummary,
        Content_Average_RatingsSummary,
        Content_Ratings_Per_Day_Summary,
        Content_Reviews_Per_User,
        ContentList,
        exhibitormeetingrequests,
        All_time_lead_report,
        beacons_messages,
        promotedpost_clickthroughs,
        surveyresults_byuser,
        SurveyResults_Combined,
        SurveyResults_MultipleChoice,
        SurveyResults_FreeForm,
        SurveyIDs,
        pollresultbyuser,
        UserActivites_Summary,
        AllCommentsUpdatesLikes_PerUser,
        Comments_PerUser,
        Follows_PerUser,
        Follows_PerDay,
        hashtags,
        likes_PerUser,
        likes_PerDay,
        Mentions_PerDay,
        PhotosUploadedSmall,
        PhotosUploadedMediumSize,
        StatusUpdatesPerDay_Summary,
        StatusUpdates_PerUser,
        statusupdatesandcheckins_PerUser,
        StatusUpdateonContentItems_Summary,
        StatusUpdateSentiment,
        AchievementsAwarded_Summary,
        Achievements_PerDaySummary,
        Achievements_PerUser,
        LeaderboardRankings,
        PointsAwarded_PerDay

--===Select data on page views

FROM (
        SELECT  global_user_id AS GlobalUserID,
                application_id AS ApplicationID,
                MIN(date) AS MinEventLogin,
                MAX(date) AS MaxEventLogin,
                COUNT(date) AS NumberOfDaysActive,
                SUM(total_pageviews) TotalPageviews,
                SUM(CASE WHEN page_path like '/ep/%' THEN total_pageviews * 1 ELSE 0 END) AS EventPerformancePageViews
        FROM GOOGLE.ep_app_pageview_counts
        GROUP BY global_user_id, applicationid
      ) AS PAGES
--==Select data on event performance downloads
LEFT JOIN (
           SELECT global_user_id AS globaluserid,
                  application_id AS ApplicationID,
--"App Usage" reports
                SUM(CASE WHEN event_label='newusers' THEN 1* total_events ELSE 0 END) all_users,
                SUM(CASE WHEN event_label='activeusersperday' THEN 1* total_events ELSE 0 END) Activeusers_PerDay,
                SUM(CASE WHEN event_label='visitsperday' THEN 1* total_events ELSE 0 END) Visitsperday_Summary,
                SUM(CASE WHEN event_label='visitsperdaybyapptype' THEN 1* total_events ELSE 0 END) visitsperday_platform,
                SUM(CASE WHEN event_label='visitsperuser' THEN 1* total_events ELSE 0 END) visitsperuser,
--"Attendee Session Tracking" reports
                SUM(CASE WHEN event_label='attendeesscanned' THEN 1* total_events ELSE 0 END) AttendeesScanned,
                SUM(CASE WHEN event_label='attendeesscanners' THEN 1* total_events ELSE 0 END) Scanners,
--"Content Engagement" reports
                SUM(CASE WHEN event_label='gridstats' THEN 1* total_events ELSE 0 END) AppSectionVisits,
                SUM(CASE WHEN event_label='bookmarks' THEN 1* total_events ELSE 0 END) bookmarks,
                SUM(CASE WHEN event_label='itemstats' THEN 1* total_events ELSE 0 END) ContentEngagementSummary,
                SUM(CASE WHEN event_label='itemwithratings' THEN 1* total_events ELSE 0 END) Content_Average_RatingsSummary,
                SUM(CASE WHEN event_label='ratingsperday' THEN 1* total_events ELSE 0 END) Content_Ratings_Per_Day_Summary,
                SUM(CASE WHEN event_label='userreviews' THEN 1* total_events ELSE 0 END) Content_Reviews_Per_User,
                SUM(CASE WHEN event_label='itemlist' THEN 1* total_events ELSE 0 END) ContentList,
--Leads REPORTS
                SUM(CASE WHEN event_label='exhibitormeetingrequests' THEN 1* total_events ELSE 0 END) ExhibitorMeetingRequests,
                SUM(CASE WHEN event_label='leads' THEN 1* total_events ELSE 0 END) All_time_lead_report,
--Broadcast Messages REPORTS
                SUM(CASE WHEN event_label='beacons' THEN 1* total_events ELSE 0 END) beacons_messages,
                SUM(CASE WHEN event_label='promotedpostclickthroughs' THEN 1* total_events ELSE 0 END) promotedpost_clickthroughs,
--Surveys & Polls REPORTS
                SUM(CASE WHEN event_label='surveyresultsbyuser' THEN 1* total_events ELSE 0 END) surveyresults_byuser,
                SUM(CASE WHEN event_label='combinedsurveyresults' THEN 1* total_events ELSE 0 END) SurveyResults_Combined,
                SUM(CASE WHEN event_label='multiplechoicesurveyresults' THEN 1* total_events ELSE 0 END) SurveyResults_MultipleChoice,
                SUM(CASE WHEN event_label='freeformsurveyresults' THEN 1* total_events ELSE 0 END) SurveyResults_FreeForm,
                SUM(CASE WHEN event_label='surveys' THEN 1* total_events ELSE 0 END) SurveyIDs,
                SUM(CASE WHEN event_label='pollresultbyuser' THEN 1* total_events ELSE 0 END) pollresultbyuser,
--Social Engagement REPORTS

                SUM(CASE WHEN event_label='useractivitiescount' THEN 1* total_events ELSE 0 END) UserActivites_Summary,
                SUM(CASE WHEN event_label='statusupdateswithassociatedcommentslikes' THEN 1* total_events ELSE 0 END) AllCommentsUpdatesLikes_PerUser,
                SUM(CASE WHEN event_label='newcomments' THEN 1* total_events ELSE 0 END) Comments_PerUser,
                SUM(CASE WHEN event_label='followsperuser' THEN 1* total_events ELSE 0 END) Follows_PerUser,
                SUM(CASE WHEN event_label='followsperday' THEN 1* total_events ELSE 0 END) Follows_PerDay,
                SUM(CASE WHEN event_label='hashtags' THEN 1* total_events ELSE 0 END) hashtags,
                SUM(CASE WHEN event_label='likes' THEN 1* total_events ELSE 0 END) likes_PerUser,
                SUM(CASE WHEN event_label='likesperday' THEN 1* total_events ELSE 0 END) likes_PerDay,
                SUM(CASE WHEN event_label='mentionsperuser' THEN 1* total_events ELSE 0 END) Mentions_PerDay,
                SUM(CASE WHEN event_label='statusupdateimages' THEN 1* total_events ELSE 0 END) PhotosUploadedSmall,
                SUM(CASE WHEN event_label='statusupdateimages-full' THEN 1* total_events ELSE 0 END) PhotosUploadedMediumSize,
                SUM(CASE WHEN event_label='statusupdatesbreakdowns' THEN 1* total_events ELSE 0 END) StatusUpdatesPerDay_Summary,
                SUM(CASE WHEN event_label='noofstatusupdatesbyusers' THEN 1* total_events ELSE 0 END) StatusUpdates_PerUser,
                SUM(CASE WHEN event_label='statusupdatesandcheckins' THEN 1* total_events ELSE 0 END) statusupdatesandcheckins_PerUser,
                SUM(CASE WHEN event_label='noofstatusupdatesinitem' THEN 1* total_events ELSE 0 END) StatusUpdateonContentItems_Summary,
                SUM(CASE WHEN event_label='statusupdatesentiment' THEN 1* total_events ELSE 0 END) StatusUpdateSentiment,

--Gamification
                SUM(CASE WHEN event_label='badgebreakdowns' THEN 1* total_events ELSE 0 END) AchievementsAwarded_Summary,
                SUM(CASE WHEN event_label='badgesperdaybreakdowns' THEN 1* total_events ELSE 0 END) Achievements_PerDaySummary,
                SUM(CASE WHEN event_label='userbadges' THEN 1* total_events ELSE 0 END) Achievements_PerUser,
                SUM(CASE WHEN event_label='leaderboard' THEN 1* total_events ELSE 0 END) LeaderboardRankings,
                SUM(CASE WHEN event_label='pointsbreakdowns' THEN 1* total_events ELSE 0 END) PointsAwarded_PerDay


          FROM google.ep_app_event_counts
              GROUP BY globaluserid, applicationid
              ) AS REPORTS
ON REPORTS.ApplicationID= PAGES.ApplicationID AND REPORTS.GlobalUserId = PAGES.GlobalUserId
--==Join data on global user detail information.
JOIN ( SELECT firstname,
              lastname,
              emailaddress,
              lower(GlobalUserId) AS GlobalUserId,
              lower(ApplicationID) AS ApplicationID
        FROM KEVIN.Ratings_GlobalUserDetails
      )RGD
ON PAGES.GlobalUserId = RGD.GlobalUserId AND PAGES.ApplicationID = RGD.ApplicationID
;
