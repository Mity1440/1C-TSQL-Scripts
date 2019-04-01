SET NOCOUNT ON;

DECLARE @query NVARCHAR(MAX)
DECLARE @tablename NVARCHAR(MAX)
DECLARE @DBName NVARCHAR(200)

DECLARE @SchedulList TABLE (
[name]           NVARCHAR(max),
[object_id]      INT,
[processed]      BIT)

SET @DBName = 'base_name'

SET @query =N'SELECT t.name, t.object_id, 0x0 From %BASENAME%.[sys].[tables] t
WHERE t.name like ''_ScheduledJobs%'''

SET @query = REPLACE(@query, N'%BASENAME%', QUOTENAME(@DBName))

INSERT INTO @SchedulList EXEC(@query)

WHILE EXISTS (SELECT TOP 1 0x0 FROM @SchedulList WHERE [processed] = 0x0)

BEGIN

	SELECT TOP 1 @tablename = [name] FROM @SchedulList	WHERE [processed] = 0x0

	SET @query = N'UPDATE %BASENAME%.[dbo].%TABLENAME% SET _Use = 0x00'
	SET @query = REPLACE(@query, N'%BASENAME%', QUOTENAME(@DBName))
	SET @query = REPLACE(@query, N'%TABLENAME%', QUOTENAME(@tablename))

	BEGIN TRY
		EXEC(@query)
	END TRY

	BEGIN CATCH
		BREAK;
	END CATCH

	UPDATE @SchedulList	SET [processed] = 0x1 WHERE [name] =  @tablename

 END