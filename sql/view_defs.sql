--===============================================================================================
--
--== FACT DATASETS
--
--===============================================================================================

--== POSTS 
CREATE OR REPLACE VIEW EventCube.V_FactPosts AS
SELECT 
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
FROM (SELECT CheckInId, Created, ApplicationId, ItemId, UserId FROM PUBLIC.Ratings_UserCheckIns) S
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON S.CheckInId = I.CheckInId
LEFT OUTER JOIN (SELECT I.ItemId, T.ListTypeId FROM PUBLIC.Ratings_Item I JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) T ON S.ItemId = T.ItemId;

--==LIKES
CREATE OR REPLACE VIEW EventCube.V_FactLikes AS 
SELECT 
S.Created AS Timestamp, 
S.ApplicationId, 
S.GlobalUserId, 
S.UserId,
CASE
  WHEN P.ListTypeId = 0 THEN 'Unspecified'
  WHEN P.ListTypeId = 1 THEN 'Regular'
  WHEN P.ListTypeId = 2 THEN 'Agenda'
  WHEN P.ListTypeId = 3 THEN 'Exhibitors'
  WHEN P.ListTypeId = 4 THEN 'Speakers'
  WHEN P.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
P.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
FROM (SELECT S.ApplicationId, S.UserId, S.Created, S.CheckInId, U.GlobalUserId FROM PUBLIC.Ratings_UserCheckInLikes S JOIN EventCube.DimUsers U ON S.UserId = U.UserId) S
LEFT OUTER JOIN (SELECT P.ApplicationId, P.CheckInId, T.ListTypeId, P.ItemId FROM PUBLIC.Ratings_UserCheckIns P JOIN PUBLIC.Ratings_Item I ON P.ItemId = I.ItemId JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) P ON S.CheckInId = P.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON S.CheckInId = I.CheckInId;

--==COMMENTS
CREATE OR REPLACE VIEW EventCube.V_FactComments AS 
SELECT 
S.Created AS Timestamp, 
P.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN P.ListTypeId = 0 THEN 'Unspecified'
  WHEN P.ListTypeId = 1 THEN 'Regular'
  WHEN P.ListTypeId = 2 THEN 'Agenda'
  WHEN P.ListTypeId = 3 THEN 'Exhibitors'
  WHEN P.ListTypeId = 4 THEN 'Speakers'
  WHEN P.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
P.ItemId,
CASE WHEN I.CheckInId IS NOT NULL THEN 1 ELSE 0 END HasImage
FROM PUBLIC.Ratings_UserCheckInComments S
LEFT OUTER JOIN (SELECT P.ApplicationId, P.CheckInId, T.ListTypeId, P.ItemId FROM PUBLIC.Ratings_UserCheckIns P JOIN PUBLIC.Ratings_Item I ON P.ItemId = I.ItemId JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId) P ON S.CheckInId = P.CheckInId
LEFT OUTER JOIN (SELECT DISTINCT CheckInId FROM PUBLIC.Ratings_UserCheckInImages) I ON P.CheckInId = I.CheckInId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;

--==BOOKMARKS
CREATE OR REPLACE VIEW EventCube.V_FactBookmarks AS 
SELECT  
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
S.IsImported
FROM PUBLIC.Ratings_UserFavorites S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;

--==FOLLOWS
CREATE OR REPLACE VIEW EventCube.V_FactFollows AS 
SELECT 
S.Created AS Timestamp, 
U.ApplicationId, 
U.GlobalUserId, 
S.UserId, 
T.GlobalUserId AS TargetGlobalUserId, 
T.UserId AS TargetUserId
FROM PUBLIC.Ratings_UserTrust S
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN EventCube.DimUsers T ON S.TrustsThisUserId = T.UserId;

--==CHECKINS
CREATE OR REPLACE VIEW EventCube.V_FactCheckins AS 
SELECT 
S.ShowUpId, 
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
CAST(S.IsTransient AS INT) IsHeadcount
FROM PUBLIC.Ratings_ShowUps S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId;

--==RATINGS
CREATE OR REPLACE VIEW EventCube.V_FactRatings AS 
SELECT * FROM (
SELECT 
S.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
S.UserId,
CASE
  WHEN T.ListTypeId = 0 THEN 'Unspecified'
  WHEN T.ListTypeId = 1 THEN 'Regular'
  WHEN T.ListTypeId = 2 THEN 'Agenda'
  WHEN T.ListTypeId = 3 THEN 'Exhibitors'
  WHEN T.ListTypeId = 4 THEN 'Speakers'
  WHEN T.ListTypeId = 5 THEN 'File'
  ELSE '???'
END ListType, 
S.ItemId,
S.Rating, 
CASE WHEN S.Comments != '' AND S.Comments IS NOT NULL THEN 1 ELSE 0 END HasReview
FROM PUBLIC.Ratings_ItemRatings S
LEFT OUTER JOIN PUBLIC.Ratings_Item I ON S.ItemId = I.ItemId
LEFT OUTER JOIN PUBLIC.Ratings_Topic T ON I.ParentTopicId = T.TopicId
JOIN EventCube.DimUsers U ON S.UserId = U.UserId
) t WHERE HasReview = 1 ORDER BY Timestamp;

--==SURVEY RESPONSES
CREATE OR REPLACE VIEW EventCube.V_FactSurveys AS 
SELECT DISTINCT
R.Created AS Timestamp, 
S.ApplicationId, 
U.GlobalUserId, 
R.UserId, 
Q.SurveyId, 
N.Questions
FROM PUBLIC.Ratings_SurveyResponses R
JOIN PUBLIC.Ratings_SurveyQuestions Q ON R.SurveyQuestionId = Q.SurveyQuestionId
JOIN PUBLIC.Ratings_Surveys S ON Q.SurveyId = S.SurveyId
LEFT OUTER JOIN
( SELECT SurveyId, COUNT(*) AS Questions
  FROM PUBLIC.Ratings_SurveyQuestions
  GROUP BY SurveyId
) N
ON Q.SurveyId = N.SurveyId
JOIN EventCube.DimUsers U ON R.UserId = U.UserId
WHERE S.IsPoll IS FALSE;

--===============================================================================================
--
--== Dimension Datasets
--
--===============================================================================================

--==DIM: Items
CREATE OR REPLACE VIEW EventCube.V_DimItems AS 
SELECT 
A.ItemId, 
A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.ShortName)) AS ShortName,
A.Address1,
A.Address2,
A.Suite,
A.City,
A.State,
A.ZipCode,
A.Country,
A.Phone,
A.FAX,
A.Email,
A.Lattitude,
A.Longitude
FROM PUBLIC.Ratings_Item A;

--==DIM: Surveys
CREATE OR REPLACE VIEW EventCube.V_DimSurveys AS 
SELECT
A.SurveyId, 
A.ApplicationId, 
RTRIM(LTRIM(A.Name)) AS Name,
RTRIM(LTRIM(A.Description)) AS Description,
A.ItemId,
A.PostCheckInPrompt,
A.PostCheckInDelay,
A.IsDisabled,
A.IsPoll
FROM PUBLIC.Ratings_Surveys A;

--===============================================================================================
--
--== User Metadata
--
--===============================================================================================

--==USER DIM: Social Networks
CREATE OR REPLACE VIEW EventCube.V_DimUserSocialNetworks AS 
SELECT UserId,
MAX(CASE WHEN FacebookUserId IS NOT NULL AND FacebookUserId != 0 THEN 1 ELSE 0 END) Facebook,
MAX(CASE WHEN TwitterUserName IS NOT NULL THEN 1 ELSE 0 END) Twitter,
MAX(CASE WHEN LinkedInId IS NOT NULL THEN 1 ELSE 0 END) LinkedIn
FROM PUBLIC.Ratings_UserDetails
GROUP BY UserId;

--==USER DIM: First Binary Version
CREATE OR REPLACE VIEW EventCube.V_DimUserBinaryVersion AS
SELECT ApplicationId, UserId, FirstBinaryVersion AS BinaryVersion FROM EventCube.Agg_Session_per_AppUser;

--==USER DIM: Prefered Device Type
CREATE OR REPLACE VIEW EventCube.V_DimUserDeviceType AS 
SELECT UserId, 
CASE 
  WHEN DevicePreference IN ('iPad','iPhone','iPhone/iPad') THEN 'iOS'
  WHEN DevicePreference IN ('Android') THEN 'Android'
  WHEN DevicePreference IN ('HTML5') THEN 'HTML5'
  WHEN DevicePreference IN ('iPhone/Android','iPad/Android') THEN 'iOS/Android'
  WHEN DevicePreference IN ('iPhone/HTML5','iPad/HTML5') THEN 'iOS/HTML5'
  WHEN DevicePreference IN ('Android/HTML5') THEN 'Android/HTML5'
  WHEN DevicePreference IN ('Multiple Devices') THEN 'Multiple'
END AS DeviceType,
DevicePreference AS Device
FROM EventCube.Agg_Session_per_AppUserAppTypeId;

--===============================================================================================
--
--== Event Metadata
--
--===============================================================================================

--==DIM: Event SalesForce Metadata
CREATE OR REPLACE VIEW EventCube.V_DimEventsSFDC AS 
SELECT DISTINCT UPPER(CAST(sf_Event_Id_CMS__c AS TEXT)) ApplicationId,
LAST_VALUE(sf_Event_Type__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) EventType,
LAST_VALUE(sf_Event_Size__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) EventSize,
LAST_VALUE(sf_Account_Customer_Domain__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AccountCustomerDomain,
LAST_VALUE(sf_Service_Tier_Name__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ServiceTierName,
LAST_VALUE(sf_X365__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) App365Indicator,
LAST_VALUE(sf_Event_Date__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) SF_EventStartDate,
LAST_VALUE(sf_Event_End_Date__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) SF_EventEndDate,
LAST_VALUE(sf_Owner_Name__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(SF_LastModifiedDate AS TIMESTAMP) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) SF_OwnerName
FROM Integrations.Implementation__c
WHERE LENGTH(sf_Event_Id_CMS__c) = 36
AND LENGTH(sf_Event_Id_CMS__c) - LENGTH(REPLACE(sf_Event_Id_CMS__c,'-','')) = 4;

--==EVENT DIM: Majority First Binary Version
CREATE OR REPLACE VIEW EventCube.V_DimEventBinaryVersion AS
SELECT DISTINCT ApplicationId,
LAST_VALUE(BinaryVersion) OVER (PARTITION BY ApplicationId ORDER BY PctUsers ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) BinaryVersion
FROM
( 
  SELECT DISTINCT ApplicationId, FirstBinaryVersion AS BinaryVersion,
  1.0*COUNT(*) OVER (PARTITION BY ApplicationId, FirstBinaryVersion)/COUNT(*) OVER (PARTITION BY ApplicationId) PctUsers
  FROM EventCube.Agg_Session_per_AppUser
) B;