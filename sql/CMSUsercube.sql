
TRUNCATE TABLE RUDO.CMSUserCube;

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
        EventPerformancePageViews
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
                SUM(CASE WHEN event_label='newusers' THEN 1* total_events ELSE 0 END) newuser
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
