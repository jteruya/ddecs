-- Experimental, for DOMO

IF OBJECT_ID('ReportingDB.dbo.YieldCube','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.YieldCube

SELECT S.*,
RANK() OVER (PARTITION BY BinaryVersion ORDER BY Date) Day
INTO ReportingDB.dbo.YieldCube
FROM
( SELECT DISTINCT S.BinaryVersion, S.Date,
  SUM(Sessions) OVER (PARTITION BY S.BinaryVersion ORDER BY S.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)*1.0/SUM(Users) OVER (PARTITION BY S.BinaryVersion) cSessionsPerUser,
  SUM(Sessions) OVER (PARTITION BY S.BinaryVersion ORDER BY S.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cSessions,
  SUM(Users) OVER (PARTITION BY S.BinaryVersion ORDER BY S.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cUsers,
  SUM(Events) OVER (PARTITION BY S.BinaryVersion ORDER BY S.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cEvents,
  SUM(Sessions) OVER (PARTITION BY S.BinaryVersion, S.Date) dSessions,
  SUM(Users) OVER (PARTITION BY S.BinaryVersion, S.Date) dUsers,
  SUM(Events) OVER (PARTITION BY S.BinaryVersion, S.Date) dEvents,
  SUM(Sessions) OVER (PARTITION BY S.BinaryVersion) tSessions,
  SUM(Users) OVER (PARTITION BY S.BinaryVersion) tUsers,
  SUM(Events) OVER (PARTITION BY S.BinaryVersion) tEvents
  FROM
  ( SELECT BinaryVersion, Date, COUNT(*) Sessions, COUNT(DISTINCT UserId) Users
    FROM
    ( SELECT MIN(CAST(Timestamp AS DATE)) OVER (PARTITION BY S.UserId) Date, S.UserId, B.BinaryVersion
      FROM ReportingDB.dbo.FactSessions S
      JOIN ReportingDB.dbo.DimUserBinaryVersion B ON S.UserId = B.UserId
      WHERE CASE WHEN B.BinaryVersion = 'v???' THEN -1.0 ELSE CAST(RIGHT(B.BinaryVersion,3) AS FLOAT) END >= 4.9
    ) S
    GROUP BY BinaryVersion, Date
  ) S
  JOIN
  ( SELECT BinaryVersion, Date, COUNT(DISTINCT ApplicationId) Events
    FROM
    ( SELECT MIN(CAST(Timestamp AS DATE)) Date, S.ApplicationId, B.BinaryVersion
      FROM ReportingDB.dbo.FactSessions S
      JOIN ReportingDB.dbo.DimUserBinaryVersion B ON S.UserId = B.UserId
      WHERE CASE WHEN B.BinaryVersion = 'v???' THEN -1.0 ELSE CAST(RIGHT(B.BinaryVersion,3) AS FLOAT) END >= 4.9
      GROUP BY S.ApplicationId, B.BinaryVersion
    ) S
    GROUP BY BinaryVersion, Date
  ) E
  ON S.BinaryVersion = E.BinaryVersion AND S.Date = E.Date
) S
WHERE cSessionsPerUser >= 1.0
ORDER BY BinaryVersion, Day

