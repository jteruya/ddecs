IF OBJECT_ID('ReportingDB.dbo.DimUserDeviceType','U') IS NOT NULL
  DROP TABLE ReportingDB.dbo.DimUserDeviceType

SELECT UserId, CASE WHEN Device IN ('iPhone','iPad') THEN 'iOS' WHEN Device = 'Android' THEN 'Android' ELSE 'Other' END DeviceType, Device
INTO ReportingDB.dbo.DimUserDeviceType
FROM
( SELECT DISTINCT UserId, LAST_VALUE(CASE
    WHEN AppTypeId = 1 THEN 'iPhone'
    WHEN AppTypeId = 2 THEN 'iPad'
    WHEN AppTypeId = 3 THEN 'Android'
    WHEN AppTypeId = 4 THEN 'HTML5'
    WHEN AppTypeId = 5 THEN 'WindowsPhone7'
    WHEN AppTypeId = 6 THEN 'Blackberry'
    ELSE '???' END
  )
  OVER (PARTITION BY UserId ORDER BY PctSessions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) Device
  FROM
  ( SELECT DISTINCT S.UserId, AppTypeId, 1.0*COUNT(*) OVER (PARTITION BY S.UserId, AppTypeId)/COUNT(*) OVER (PARTITION BY S.UserId) PctSessions
    FROM AnalyticsDB.dbo.Sessions S
    JOIN ReportingDB.dbo.DimUsers U ON S.UserId = U.UserId
  ) S
) S

