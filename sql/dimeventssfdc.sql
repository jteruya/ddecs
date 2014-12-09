IF OBJECT_ID('ReportingDB.dbo.DimEventsSFDC','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimEventsSFDC

-- ====================================================
-- Creates a dim table from SFDC implementation records
-- Attributes one dim per app id based on last modified
-- ====================================================

SELECT DISTINCT CAST(UPPER(sf_Event_Id_CMS__c) AS UNIQUEIDENTIFIER) ApplicationId,
LAST_VALUE(sf_Event_Type__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(sf_LastModifiedDate AS DATETIME) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) EventType,
LAST_VALUE(sf_Event_Size__c) OVER (PARTITION BY sf_Event_Id_CMS__c ORDER BY CAST(sf_LastModifiedDate AS DATETIME) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) EventSize
INTO ReportingDB.dbo.DimEventsSFDC
FROM ReportingDB.dbo.Implementation__c
WHERE LEN(sf_Event_Id_CMS__c) = 36

