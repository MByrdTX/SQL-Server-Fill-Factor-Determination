DECLARE @SchemaName			SYSName
DECLARE @TableName			SYSNAME
DECLARE @IndexName			SYSNAME
DECLARE @SQL				NVARCHAR(512);

DECLARE Index_Cursor CURSOR FOR 
SELECT DISTINCT SchemaName,TableName,IndexName
  FROM [ROICore].[Admin].[AgentIndexRebuilds]
  ORDER BY 2,3;

OPEN Index_Cursor;

FETCH NEXT FROM Index_Cursor INTO @SchemaName,@TableName,@IndexName

WHILE @@Fetch_Status = 0
	BEGIN SET @SQL = N'SET QUOTED_IDENTIFIER ON     
                    ALTER INDEX ' + @indexname +' ON [' + @schemaname + 
                    N'].[' + @tablename + N'] REBUILD WITH (ONLINE = ON,DATA_COMPRESSION = ROW,MAXDOP = 1,FILLFACTOR = 100)'
		PRINT @SQL;
        EXEC sys.sp_executesql @sql     
		FETCH NEXT FROM Index_Cursor INTO @SchemaName,@TableName,@IndexName
	END

CLOSE Index_Cursor;
DEALLOCATE Index_Cursor;
GO

TRUNCATE TABLE [Admin].[AgentIndexRebuilds]
GO
