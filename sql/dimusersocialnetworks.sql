DROP TABLE IF EXISTS EventCube.DimUserSocialNetworks;

--=========================================================================================================================
-- Per User, identifies the authorizations performed per separate 3rd party social networks and flags per each identified. 
--=========================================================================================================================

-- SELECT U.UserId,
-- MAX(CASE WHEN OAuthPartnerId = 1 THEN 1 ELSE 0 END) Facebook,
-- MAX(CASE WHEN OAuthPartnerId = 2 THEN 1 ELSE 0 END) Twitter,
-- MAX(CASE WHEN OAuthPartnerId = 6 THEN 1 ELSE 0 END) LinkedIn
-- INTO ReportingDB.dbo.DimUserSocialNetworks
-- FROM Ratings.dbo.UserOAuthTokens U
-- JOIN Ratings.dbo.ApplicationOAuthKeys A ON AppOAuthMappingId = MappingId
-- JOIN ReportingDB.dbo.DimUsers S ON U.UserId = S.UserId
-- WHERE OAuthPartnerId IN (1,2,6)
-- GROUP BY U.UserId

CREATE TABLE EventCube.DimUserSocialNetworks AS 
SELECT UserId,
MAX(CASE WHEN FacebookUserId IS NOT NULL AND FacebookUserId != 0 THEN 1 ELSE 0 END) Facebook,
MAX(CASE WHEN TwitterUserName IS NOT NULL THEN 1 ELSE 0 END) Twitter,
MAX(CASE WHEN LinkedInId IS NOT NULL THEN 1 ELSE 0 END) LinkedIn
FROM PUBLIC.Ratings_UserDetails
GROUP BY UserId;