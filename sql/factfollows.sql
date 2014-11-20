IF OBJECT_ID('ReportingDB.dbo.FactFollows','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactFollows

--===================================================================================================
-- Source on User Trust Fact table and enforces that each Follow is tied to an identified user. 
-- * Upstream dependency on DimUsers.
--===================================================================================================

SELECT DISTINCT S.CreatedOn Timestamp, U.ApplicationId, U.GlobalUserId, S.UserId, T.GlobalUserId TargetGlobalUserId, T.UserId TargetUserId
INTO ReportingDB.dbo.FactFollows
FROM Ratings.dbo.UserTrust S
JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
LEFT OUTER JOIN ReportingDB.dbo.DimUsers T ON S.TrustsThisUserId = T.UserId

