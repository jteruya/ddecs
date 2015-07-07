IF OBJECT_ID('ReportingDB.dbo.FactSessionsRetention','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.FactSessionsRetention

SELECT DISTINCT RANK() OVER (PARTITION BY UserId ORDER BY Timestamp) SessionNo, Timestamp, Date, UserId, BinaryVersion
INTO ReportingDB.dbo.FactSessionsRetention
FROM 
( SELECT Timestamp, MIN(CAST(Timestamp AS DATE)) OVER (PARTITION BY S.UserId) Date, S.UserId, B.BinaryVersion
  FROM ReportingDB.dbo.FactSessions S
  JOIN ReportingDB.dbo.DimUserBinaryVersion B ON S.UserId = B.UserId
  WHERE CASE WHEN B.BinaryVersion = 'v???' THEN -1.0 ELSE CAST(RIGHT(B.BinaryVersion,3) AS FLOAT) END >= 4.9
) S

