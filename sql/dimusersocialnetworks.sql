IF OBJECT_ID('ReportingDB.dbo.DimUserSocialNetworks','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimUserSocialNetworks

SELECT U.UserId,
MAX(CASE WHEN OAuthPartnerId = 1 THEN 1 ELSE 0 END) Facebook,
MAX(CASE WHEN OAuthPartnerId = 2 THEN 1 ELSE 0 END) Twitter,
MAX(CASE WHEN OAuthPartnerId = 6 THEN 1 ELSE 0 END) LinkedIn
INTO ReportingDB.dbo.DimUserSocialNetworks
FROM Ratings.dbo.UserOAuthTokens U
JOIN Ratings.dbo.ApplicationOAuthKeys A ON AppOAuthMappingId = MappingId
JOIN ReportingDB.dbo.NewEventCube_DimUsers S ON U.UserId = S.UserId
WHERE OAuthPartnerId IN (1,2,6)
GROUP BY U.UserId
