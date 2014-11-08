-- DOESN'T WORK WITH PYMSSQL, HERE FOR REFERENCE ONLY

-- Remove non-alphanumeric characters
-- SELECT dbo.STRIP_STRING('abc1234def5678ghi90jkl')
-- http://stackoverflow.com/questions/1007697/how-to-strip-all-non-alphabetic-characters-from-string-in-sql-server

IF OBJECT_ID('STRIP_STRING','FN') IS NOT NULL
  DROP FUNCTION STRIP_STRING
GO

CREATE FUNCTION dbo.STRIP_STRING(@Temp VARCHAR(1000))
RETURNS VARCHAR(1000)
AS
BEGIN
  DECLARE @KeepValues AS VARCHAR(50)
  SET @KeepValues = '%[^A-Za-z0-9 ]%'
  WHILE PatIndex(@KeepValues, @Temp) > 0
    SET @Temp = STUFF(@Temp, PATINDEX(@KeepValues, @Temp), 1, '')
  RETURN @Temp
END
GO

