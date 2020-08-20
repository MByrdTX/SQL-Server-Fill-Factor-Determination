--© 2020 | ByrdNest Consulting

-- ensure a USE <databasename> statement has been executed first. 
--USE <Database>    
GO
/****************************************************************************************

    Index Rebuild (defrag) script with logic from Jeff Moden and
        additional logic to change fillfactor as needed.  Updated
		code and documentation can be found at GitHub under
		MByrdTX as author.  

    Designed to work with SS2012 and later, Enterprise Edition and 
        Developer Edition.  If you are using Standard Edition, the
        script will remove ONLINE = ON option from the dynamic SQL

    This script was created to rebuild clustered and non-clustered 
        indexes with average fragmentation > 1.2%.  It picks the top 
        20 (configurable) worse average fragmented indexes for an index 
        rebuild and it also varies each index fill factor (not heaps or 
        partitioned tables) to determine a "near optimum" value for 
        existing conditions.  Once a fill factor value is determined, it 
        is fixed for each succeeding execution of this script.  If the 
        fill factor value has not changed in last 90 days (configurable),
        it is again put in the queue for finding the best fill factor 
        (rationale for this logic is that data skew and calling patterns 
        from applications may change over time).

    If a table and its indexes are partitioned, this script rebuilds the 
        appropriate index partition with no adjustment to the fill factor.  

	After several months of analysis I determined that fill factor pertubation
	    should not be accomplished on weekends (at least for this database).
		Code was added to bypass index pertubation on weekends.  If this is
		not an issue for you, please adjust @WorkDay definition in the
		DECLARE segment of this code.

	This started out to be a proof of concept trying to determine if we could
	perturb fill factor from a history table to find an "optimum" fill factor
	for each index.  After 90 days, I saw a 30% drop in overall wait times for
	a very active online transaction database.  I've continued to "tweak" this
	script as I collect data.  This originally started out as a defragmentation
	script, then evolved into a fill factor determination script, and finally
	has run full circle to both a fill factor and defragmentation script where
	the major defragmentation occurs on the weekend (Saturday and Sunday).  
	This script will not tweak fill factor for heaps and partitioned indexes, but
	does defragment partitioned indexes.  

	All data modifications are stored in the [Admin].AgentIndexRebuilds table.  This
	table can be subsequently be queried for reports.

    This script should be executed from a SQL Agent job that runs daily 
        -- recommend time when server is least active.  It also depends on 
           a table (created by this script first time run) to store index 
           parametrics for the fill factor determination.  This table, the
		   Admin schema, and the extended event code definition can be
		   found in the FillFactorIndexSetup.sql script.  

    You may alter this code for your own *non-commercial* purposes. You may
    republish altered code as long as you include this copyright and give 
     due credit. 


    THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
    ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
    TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
    PARTICULAR PURPOSE. 

    Created         By                Comments
    20190424        Mike Byrd        Created
    20190513        Mike Byrd        Added additional data columns to 
                                         AgentIndexRebuilds table
    20190604        Mike Byrd        Added additional logic for setting FixFillFactor
    20190616        Mike Byrd        Revised FillFactor logic
    20190718        Mike Byrd        Added logic to get bad page splits (thanks to Jonathan Kehayias)
                                         https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/
    20190826        William Meitzen  Added logic to check SS version; some debug flags
    20190826        Mike Byrd        Revised logic to identify best fill factor to use and fix it.
	20200712	    Mike Byrd	     Revised Admin.AgentIndexRebuilds table for better daily reporting, removed un-necessary columns
                                     Updated script (minimal logic change)

****************************************************************************************/

--Set Configuration parameters
DECLARE @RedoPeriod          INT     = 90;    --Days
DECLARE @TopWorkCount        INT     = 20;    --Specify how large result 

DECLARE @ShowDynamicSQLCommands bit = 1; -- show dynamic SQL commands before they run
DECLARE @ShowProcessSteps bit = 1; -- show where we are in the code
                                             
--  --get current database name
DECLARE @Database            SYSNAME = (SELECT DB_NAME());
DECLARE @DatabaseID          SMALLINT = DB_ID(@Database);

DECLARE @Retry				 INT;

--get email profile
DECLARE @SvrName			 SYSNAME = (SELECT TOP 1 [srvname] FROM [master].[sys].[sysservers] WHERE [srvproduct]  = 'SQL Server');
DECLARE @ProfileName		 SYSNAME = CASE WHEN @SvrName = N'RO-SQLDEV' THEN N'DevDB'
                                            WHEN @SvrName = N'RO-SQL1'   THEN N'DBA_Admin'
											ELSE NULL END;


/********************************************************************
code setup for Always On Primary Node; comment out next 3 statements
      if not an Always On Node
**********************************************************************/
DECLARE @preferredReplica INT
SET @preferredReplica = (SELECT [master].sys.fn_hadr_backup_is_preferred_replica(@Database))
IF (@preferredReplica = 0) 
  BEGIN
	DECLARE @ID                                  INT;
    DECLARE @Date                                DATE = CONVERT(DATE,GETDATE());    
    DECLARE @RowCount                            INT = 0;    
    DECLARE @objectid                            INT;     
    DECLARE @indexid                             INT;    
    DECLARE @partitioncount                      BIGINT;    
    DECLARE @schemaname                          SYSNAME;     
    DECLARE @objectname                          SYSNAME;     
    DECLARE @indexname                           SYSNAME;     
    DECLARE @partitionnum                        BIGINT;     
    DECLARE @partitions                          BIGINT;     
    DECLARE @frag                                FLOAT;     
    DECLARE @FillFactor                          INT;    
    DECLARE @OldFillFactor                       INT;    
    DECLARE @FixFillFactor                       INT;    
    DECLARE @LagDate                             INT;    
    DECLARE @NewFrag                             FLOAT;    
    DECLARE @NewPageSplitForIndex                BIGINT;    
    DECLARE @NewPageAllocationCausedByPageSplit  BIGINT;    
    DECLARE @PageCount                           BIGINT;    
    DECLARE @RecordCount                         BIGINT;    
    DECLARE @ForwardRecordCount                  BIGINT;    
    DECLARE @NewForwardRecordCount               BIGINT;    
    DECLARE @Command                             NVARCHAR(4000);    
    DECLARE @Msg                                 VARCHAR(256);    
    DECLARE @PartitionFlag                       BIT = 0;  
                                                            --don't perturb fillfactor on Saturdays or Sundays
    DECLARE @WorkDay                             BIT = 	CASE WHEN DATEPART(dw,@Date) IN (1,7) THEN 0
                                                            ELSE 1 END;  
    DECLARE @Online_On_String                    NVARCHAR(75) = N'';
    DECLARE @MaxID                               INT;
    DECLARE @MinID                               INT;
    DECLARE @MaxRowNumber                        INT;
    DECLARE @MinRowNumber                        INT;
    DECLARE @FragRowNumber                       INT;
    DECLARE @MinFrag                             FLOAT;
    DECLARE @MinFragID                           INT;
    DECLARE @MinFragBadPageSplits                INT;
    DECLARE @NewFillFactor                       INT;
	DECLARE @Error								 INT = 0;
	DECLARE @ErrorMessage						 NVARCHAR(4000);
	DECLARE @ErrorSeverity						 INT;
	DECLARE @ErrorState							 INT, @ErrorLine		INT;
	DECLARE @Message							 NVARCHAR(4000) = '';
	DECLARE @StartTime							 DATETIME = GETDATE();
	DECLARE @DeadLockFound						 BIT = 0;
	DECLARE @RedoFlag                            BIT = 0;
	DECLARE @xml								 NVARCHAR(MAX);
	DECLARE @body                                NVARCHAR(MAX);
	DECLARE @Counter							 INT;
    SET NOCOUNT ON;     
    SET QUOTED_IDENTIFIER ON;					--needed for XML ops in BadPageSplit query below


 
    --Check to see if ONLINE option available and for SS2014 or greater then also wait_at_low_priority option
	/*  may want to use resume option on index rebuild when SS2017 or higher; would probably want to change code in BEGIN TRY CATCH block below. */
	IF EXISTS (SELECT 1 FROM [master].[sys].[databases] WHERE database_id = @DatabaseID AND [compatibility_level] < 120)
            IF LOWER(@@VERSION) LIKE '%enterprise edition%' OR LOWER(@@VERSION) LIKE '%developer edition%' 
                SET @Online_On_String = N'ONLINE = ON,'
	ELSE 
            IF LOWER(@@VERSION) LIKE '%enterprise edition%' OR LOWER(@@VERSION) LIKE '%developer edition%' 
                SET @Online_On_String = N'ONLINE = ON(WAIT_AT_LOW_PRIORITY(MAX_DURATION = 1, ABORT_AFTER_WAIT=SELF)),'
 
    IF @ShowProcessSteps = 1 
        SELECT 'Retrieving top ' + cast(@TopWorkCount as varchar(5)) + ' indexes to rebuild', GETDATE(),@Date [@Date],DAY(@Date) % 2;

    IF ((DAY(@Date) % 2 = 1) OR (@WorkDay =0))  --rotate result set by frag one day, then BadPageSplits the next, but on weekends only do frag result set
      BEGIN
        INSERT [Admin].AgentIndexRebuilds
            (CreateDate,DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation,New_Fragmentation,BadPageSplits
            ,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,LagDays,FixFillFactor,DelFlag,DeadLockFound,IndexRebuildDuration,RedoFlag,ActionTaken)
--            OUTPUT inserted.ID,inserted.TableName,Inserted.IndexName,inserted.Current_Fragmentation,inserted.BadPageSplits
            SELECT TOP (@TopWorkCount)
                        @Date CreateDate, @Database DBName,s.[name] SchemaName,o.[name] TableName,i.[name] IndexName,ps.partition_number partitionnum
                        ,ps.avg_fragmentation_in_percent Current_Fragmentation, NULL New_Fragmentation
                        ,ISNULL(tab.split_count,0) BadPageSplits
                        ,CASE WHEN i.Fill_Factor = 0 THEN 100 
                              ELSE i.Fill_Factor END [FillFactor]
                        ,ps.[object_id],ps.index_id,ps.page_count,ps.record_count,NULL LagDays, NULL FixFillFactor
                        ,0 DelFlag,0 DeadLockFound,Null IndexRebuildDuration,0 RedoFlag,NULL ActionTaken
                FROM sys.dm_db_index_physical_stats (@DatabaseID,NULL,NULL,NULL,'SAMPLED') ps 
                JOIN sys.partitions p
                  ON  p.[object_id]        = ps.[object_id]
                  AND p.index_id           = ps.index_id
                  AND p.partition_number   = ps.partition_number
                JOIN sys.indexes i
                  ON  i.index_id           = ps.index_id
                  AND i.[object_id]        = ps.[object_id]
                JOIN sys.objects o    
                  ON o.[object_id]         = i.[object_id]
                JOIN sys.schemas as s 
                  ON s.[schema_id]  = o.[schema_id] 
                JOIN sys.allocation_units au
                  ON au.container_id = p.[hobt_id]
                LEFT JOIN (SELECT n.value('(value)[1]', 'bigint') AS alloc_unit_id,
                                  n.value('(@count)[1]', 'bigint') AS split_count
                              FROM (SELECT CAST(target_data as XML) target_data
                                        FROM sys.dm_xe_sessions AS s 
                                        JOIN sys.dm_xe_session_targets t
                                          ON s.[address] = t.event_session_address
                                        WHERE s.[name] = 'SQLskills_TrackPageSplits'
                                          AND t.target_name = 'histogram' ) as tab
                              CROSS APPLY target_data.nodes('HistogramTarget/Slot') as q(n) ) AS tab
                  ON tab.alloc_unit_id = au.allocation_unit_id
                WHERE ps.avg_fragmentation_in_percent > 0.0                                    -- found single case where index was rebuilt right before SQL Agent ran this script
                  AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
                  AND ps.index_level       = 0                                                    -- only look at leaf level
                  AND i.index_id           > 0
                  AND o.[type]             = 'U'
                  AND o.is_ms_shipped      = 0
                  AND s.[name]                <> 'Admin'
                  AND au.[type]            = 1                                                    -- IN_ROW_DATA only
                  AND ps.avg_fragmentation_in_percent > 1.20        --this is rebuild condition  
                 -- logic added to still defrag on weekends and tweak fillfactor on weekdays
                  AND (@WorkDay = 0 OR (@WorkDay = 1  AND NOT EXISTS (SELECT 1 FROM [Admin].AgentIndexRebuilds air        -- logic to keep from getting fillfactor already set
                                                                               WHERE air.DBName            = @Database
                                                                                 AND air.[Object_ID]        = ps.[object_id]
                                                                                 AND air.Index_ID            = ps.index_id
                                                                                 AND air.PartitionNum        = ps.partition_number
                                                                                 AND (air.FixFillFactor    IS NOT NULL OR air.CreateDate = @Date)
                                                                                 AND air.DelFlag            = 0)))    
                ORDER BY  ps.avg_fragmentation_in_percent DESC;    
        SET @RowCount = @@ROWCOUNT;
	  END              --BEGIN at Line 180
    ELSE
	  BEGIN
        INSERT [Admin].AgentIndexRebuilds
            (CreateDate,DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation,New_Fragmentation,BadPageSplits
            ,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,LagDays,FixFillFactor,DelFlag,DeadLockFound,IndexRebuildDuration,RedoFlag,ActionTaken)
--            OUTPUT inserted.ID,inserted.TableName,Inserted.IndexName,inserted.Current_Fragmentation,inserted.BadPageSplits
            SELECT TOP (@TopWorkCount)
                        @Date CreateDate, @Database DBName,s.[name] SchemaName,o.[name] TableName,i.[name] IndexName,ps.partition_number partitionnum
                        ,ps.avg_fragmentation_in_percent Current_Fragmentation, NULL New_Fragmentation
                        ,ISNULL(tab.split_count,0) BadPageSplits
                        ,CASE WHEN i.Fill_Factor = 0 THEN 100 
                              ELSE i.Fill_Factor END [FillFactor]
                        ,ps.[object_id],ps.index_id,ps.page_count,ps.record_count,NULL LagDays, NULL FixFillFactor
                        ,0 DelFlag,0 DeadLockFound,Null IndexRebuildDuration,0 RedoFlag,NULL ActionTaken
                FROM (SELECT n.value('(value)[1]', 'bigint') AS alloc_unit_id,
                             n.value('(@count)[1]', 'bigint') AS split_count
                          FROM (SELECT CAST(target_data as XML) target_data
                                    FROM sys.dm_xe_sessions AS s 
                                    JOIN sys.dm_xe_session_targets t
                                      ON s.address = t.event_session_address
                                    WHERE s.name = 'SQLskills_TrackPageSplits'
                                      AND t.target_name = 'histogram' ) as tab
                          CROSS APPLY target_data.nodes('HistogramTarget/Slot') as q(n)
                     ) AS tab
                JOIN sys.allocation_units AS au
                  ON tab.alloc_unit_id = au.allocation_unit_id
                JOIN sys.partitions AS p
                 ON au.container_id = p.hobt_id
                JOIN sys.indexes AS i
                  ON  p.object_id = i.object_id
                  AND p.index_id = i.index_id
                JOIN sys.objects AS o
                  ON p.object_id = o.object_id
                JOIN sys.schemas as s 
                  ON s.[schema_id]  = o.[schema_id] 
				JOIN sys.dm_db_index_physical_stats (@DatabaseID,NULL,NULL,NULL,'SAMPLED') ps
                  ON  p.[object_id]        = ps.[object_id]
                  AND p.index_id           = ps.index_id
                  AND p.partition_number   = ps.partition_number
                WHERE ps.alloc_unit_type_desc = 'IN_ROW_DATA'
                  AND ps.index_level       = 0                                                    -- only look at leaf level
                  AND i.index_id           > 0
                  AND o.[type]             = 'U'
                  AND o.is_ms_shipped      = 0
                  AND s.[name]             <> 'Admin'
                  AND au.[type]            = 1                                                    -- IN_ROW_DATA only
                  AND ISNULL(tab.split_count,0) >= 20        --this is rebuild condition  --20 bad page splits is (for now) just a guess!!!
                 -- logic added to still defrag on weekends and tweak fillfactor on weekdays
                  AND (@WorkDay = 0 OR (@WorkDay = 1  AND NOT EXISTS (SELECT 1 FROM [Admin].AgentIndexRebuilds air        -- logic to keep from getting fillfactor already set
                                                                               WHERE air.DBName            = @Database
                                                                                 AND air.[Object_ID]        = ps.[object_id]
                                                                                 AND air.Index_ID            = ps.index_id
                                                                                 AND air.PartitionNum        = ps.partition_number
                                                                                 AND (air.FixFillFactor    IS NOT NULL OR air.CreateDate = @Date)
                                                                                 AND air.DelFlag            = 0)))    
                ORDER BY  ISNULL(tab.split_count,0) DESC,ps.avg_fragmentation_in_percent DESC ;    
        SET @RowCount = @@ROWCOUNT;
	  END                 --BEGIN at Line 238

IF @ShowProcessSteps = 1
    SELECT 'AgentIdexRebuilds, Line 308',GETDATE(),* FROM [Admin].AgentIndexRebuilds WHERE CreateDate = @Date;

/**********************************************************************
     Reset TrackPageSplits Extended Event (make this a 24 hour capture)
***********************************************************************/
 SET @command = N'
            -- Stop the Event Session to clear the target
            ALTER EVENT SESSION [SQLskills_TrackPageSplits]
            ON SERVER
            STATE=STOP'

    IF @ShowDynamicSQLCommands = 1 SELECT GETDATE(),@command
    EXEC sys.sp_executesql @command     

SET @command = N'
            -- Start the Event Session Again
            ALTER EVENT SESSION [SQLskills_TrackPageSplits]
            ON SERVER
            STATE=START'

    IF @ShowDynamicSQLCommands = 1 SELECT GETDATE(),@command
    EXEC sys.sp_executesql @command   
    

    IF OBJECT_ID(N'tempdb..#Temp2') IS NOT NULL DROP TABLE #Temp2 
	IF @WorkDay = 1              --only get redo row on weekdays
	  BEGIN
		/************************************************************************
		    Go back and find oldest index (>@RedoPeriod) with @FixFillFactor 
		        and add it to [Admin].AgentIndexRebuilds
		        (to keep index fill factors from getting "stale").
		***********************************************************************/
	    SELECT  TOP(1) r.ID,r.CreateDate,r.DBName,r.SchemaName,r.TableName,r.IndexName
	            ,r.PartitionNum,r.Current_Fragmentation,r.New_Fragmentation,r.BadPageSplits
	            ,r.[FillFactor],r.[Object_ID],r.Index_ID,r.Page_Count
	            ,r.Record_Count,r.LagDays,r.FixFillFactor
				,i.is_primary_key, i.[type]
	        INTO #Temp2
	        FROM [Admin].AgentIndexRebuilds r
			JOIN sys.indexes i
			  ON  i.[Name] = r.IndexName
			  AND i.index_id = r.index_id
	        WHERE r.CreateDate <= CONVERT(DATE,DATEADD(dd,-@RedoPeriod,GETDATE()))    
	          AND r.DBName = @Database
	          AND r.FixFillFactor IS NOT NULL		--don't get indexes still being perturbed
	          AND r.DelFlag = 0
	          AND @WorkDay  = 1
			  AND r.ActionTaken = 'F'
	          AND NOT EXISTS (SELECT 1  FROM [Admin].AgentIndexRebuilds r2
	                                    WHERE r2.DBName          = @Database
										  AND r2.SchemaName      = r.SchemaName
	                                      AND r2.[Object_ID]     = r.[Object_ID]
	                                      AND r2.Index_ID        = r.Index_ID
	                                      AND r2.PartitionNum    = r.PartitionNum
										  AND r2.DelFlag         = 0
										  AND r2.DeadLockFound   = 1
	                                      AND r2.ID              > r.ID)
	          --don't get partitioned tables (no adjusting fill factor)
	          -- select top 1 * from [Admin].AgentIndexRebuilds
	          AND NOT EXISTS (SELECT 1 FROM sys.partitions p 
	                                   WHERE p.object_id = r.Object_ID
	                                     AND p.index_id  = r.Index_ID
	                                     AND p.partition_number > 1)
	        ORDER BY ID DESC, CREATEDATE DESC
	        SET @RowCount = @@ROWCOUNT    

	        IF @ShowProcessSteps = 1 SELECT '#Temp2',* FROM #Temp2

			/**********************************************************************
			    Go back and recalculate FillFactor for oldest Table/Index 
			        in Admin.AgentIndexRebuilds
			***********************************************************************/
	        IF @RowCount = 1         
	          BEGIN
			    SET @RedoFlag = 1;
	            UPDATE #Temp2
	                -- start pertubation cycle over again by resetting starting FillFactor  
	                SET [FillFactor] = CASE WHEN Index_ID > 1 
	                                         AND is_primary_key = 1 
	                                            THEN 100
	                                        WHEN Index_ID > 1 
	                                         AND FixFillFactor > 90 
	                                            THEN 100
	                                        WHEN Index_ID > 1 
	                                         AND FixFillFactor > 80 
	                                            THEN 94
	                                        WHEN Index_ID > 1 
	                                         AND FixFillFactor >=70 
	                                            THEN 90
	                                        ELSE 100 END, --reset CI back to 100
	                        FixFillFactor = NULL,          --reset FixFillFactor so that 
	                                                      --  regression can start
							CreateDate = @Date
	
			/**********************************************************************
			    Reset fixfillfactor from previous passes (need to reset it for 
			    all rows with Object_ID, Index_ID, & PartitionNum
			***********************************************************************/
	            UPDATE r
	                SET DelFlag = 1
	                FROM [Admin].AgentIndexRebuilds r
	                JOIN #Temp2 t
	                  ON  t.DBName       = r.DBName
					  AND t.SchemaName   = r.SchemaName  
					  AND t.[Object_ID]  = r.[Object_ID]
	                  AND t.Index_ID     = r.Index_ID
	                  AND t.PartitionNum = r.PartitionNum
	                  AND (r.DelFlag     IS NULL OR r.DelFlag = 0)
					  AND r.CreateDate   < @Date;
	
	            --add new row to start regression
		           INSERT [Admin].AgentIndexRebuilds
	                   (CreateDate,DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation,New_Fragmentation,BadPageSplits
	                   ,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,LagDays,FixFillFactor,DelFlag,DeadLockFound
					   ,IndexRebuildDuration,RedoFlag,ActionTaken)
	--				OUTPUT INSERTED.ID, INSERTED.CreateDate,INSERTED.TableName,INSERTED.IndexName
	                SELECT DISTINCT @Date CreateDate,DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation
	                       ,NULL New_Fragmentation,BadPageSplits,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,NULL LagDays
						   ,NULL FixFillFactor,0 DelFlag,0 DeadLockFound,0 IndexRebuildDuration,1 RedoFlag,NULL ActionTaken
	                    FROM #Temp2;
					SET @RowCount = @@ROWCOUNT;
				
	            IF @ShowProcessSteps = 1 
				    SELECT 'Redo row added to Admin.AgentIndexRebuilds = ',@RowCount [@RowCount]
	          END			--Begin at Line 370
	  END                   --BEGIN at Line 323

    -- Declare the cursor for the list of indexes to be processed. 
    IF EXISTS (SELECT 1 FROM [Admin].AgentIndexRebuilds WHERE CreateDate = @Date) 
      BEGIN
        DECLARE [workcursor]  CURSOR FOR 
            SELECT  DISTINCT w.ID, w.DBName,w.SchemaName,w.[object_id], w.index_id
                    , w.partitionnum, w.Current_Fragmentation,w.[FillFactor]
                    ,w.TableName, w.IndexName
                    ,DATEDIFF(dd,(SELECT TOP 1 CreateDate
							          FROM [Admin].AgentIndexRebuilds r 
							          WHERE r.DBName        = @Database
							            AND r.SchemaName    = w.SchemaName
                                        AND r.TableName     = w.TableName
						                AND r.IndexName     = w.IndexName
						           	    AND r.PartitionNum  = w.PartitionNum
							            AND r.DelFlag       = 0
							            AND r.DeadLockFound = 0
							            AND r.ActionTaken   = 'F'
							            AND r.CreateDate    < @Date
							          ORDER BY r.ID DESC)    ,@Date) LagDate
                    ,w.RedoFlag
                FROM [Admin].AgentIndexRebuilds w
				WHERE w.CreateDate = @Date
                ORDER BY w.TableName DESC, w.IndexName DESC;    

		IF @ShowProcessSteps = 1 
            SELECT  DISTINCT 'CursorDefinition Line 448',w.ID, w.DBName,w.SchemaName,w.[object_id], w.index_id
                    , w.partitionnum, w.Current_Fragmentation,w.[FillFactor]
                    ,w.TableName, w.IndexName
                    ,DATEDIFF(dd,(SELECT TOP 1 CreateDate
							          FROM [Admin].AgentIndexRebuilds r 
							          WHERE r.DBName        = @Database
							            AND r.SchemaName    = w.SchemaName
                                        AND r.TableName     = w.TableName
						                AND r.IndexName     = w.IndexName
						           	    AND r.PartitionNum  = w.PartitionNum
							            AND r.DelFlag       = 0
							            AND r.DeadLockFound = 0
							            AND r.ActionTaken   = 'F'
							            AND r.CreateDate    < @Date
							          ORDER BY r.ID DESC)    ,@Date) LagDate
                    ,w.RedoFlag
                FROM [Admin].AgentIndexRebuilds w
				WHERE w.CreateDate = @Date
                ORDER BY w.TableName DESC, w.IndexName DESC;    

            -- Open the cursor. 
        OPEN [workcursor]     

        -- Loop through the [workcursor]. 
        FETCH NEXT FROM [workcursor] 
            INTO @ID,@Database,@SchemaName,@objectid, @indexid, @partitionnum, @frag, @FillFactor
                ,@objectname,@indexname,@LagDate,@RedoFlag;    

        WHILE @@FETCH_STATUS = 0 
          BEGIN 
		 	SET @Retry = 6;							--this can be changed
			SET @DeadLockFound = 0;
			SET @Error = 0;
			SET @StartTime = GETDATE();
            IF @ShowProcessSteps = 1 
                SELECT 'WorkCursor parameters, Line 489',GETDATE(),@objectid [@objectid], @indexid [@indexid], @partitionnum [@partitionnum]
						, @frag [@frag], @FillFactor [@FillFactor], @objectname [@objectname]
						, @indexname [@indexname], @LagDate [@LagDate], @RedoFlag [@RedoFlag] 

            IF OBJECT_ID(N'tempdb..#Temp3') IS NOT NULL DROP TABLE #Temp3    

/**********************************************************************
    New logic:  Check at least 6 rebuilds in history table and select
         fillfactor where the last 2 rebuilds had a larger fragmentation.

         This was asked for at SQL Saturday Baton Rouge as a means for
         a more stable solution.

         Also added logic to only perturb fillfactors on weekdays.
***********************************************************************/

        --check index rebuild table for at least six entries for this index (this logic sets FixFillFactor when appropriate)
        IF @WorkDay = 1
          BEGIN 
		    SET @NewFillFactor = NULL;
            IF OBJECT_ID(N'tempdb..#Temp4') IS NOT NULL DROP TABLE #Temp4
            SELECT ID,Current_Fragmentation
                    ,Row_Number() OVER (ORDER BY ID ASC) RowNumber,[FillFactor]
                    ,FixFillFactor,TableName,IndexName,BadPageSplits
                INTO #Temp4
                FROM [Admin].AgentIndexRebuilds
                WHERE DBName                = @Database
                  AND [OBJECT_ID]			= @objectid
                  AND Index_ID				= @indexid
                  AND PartitionNum			= @partitionnum
				  AND DelFlag				= 0
				  AND DeadLockFound         = 0
				  AND ActionTaken           = 'F'
				  AND Current_Fragmentation	> 0.0
                ORDER BY ID ASC;
			SET @RowCount = @@ROWCOUNT;
    
            IF @ShowProcessSteps = 1 
			  BEGIN
				SELECT 'Checking from previous perturbs on this index, Line 523',GETDATE(),* FROM #Temp4;
				SELECT '@RowCount = ',@RowCount;
			  END             --BEGIN at Line 522

			IF @RowCount >= 6
				BEGIN
		            SELECT  @MaxID           = MAX(ID),
		                    @MinID           = MIN(ID),
		                    @MaxRowNumber    = MAX(RowNumber),
		                    @MinRowNumber    = MIN(RowNumber)
		                FROM #Temp4;
					SET @MinFrag = (SELECT MIN(Current_Fragmentation) FROM #Temp4);
					SET @MinFragID = (SELECT MIN(ID) FROM #Temp4 WHERE Current_Fragmentation = @MinFrag);
        
		            SELECT @MinFragBadPageSplits    = BadPageSplits,
		                   @NewFillFactor            = [FillFactor],
		                   @FragRowNumber            = RowNumber
		                FROM #Temp4
		                WHERE ID               = @MinFragID
		                  AND RowNumber        <= @MaxRowNumber - 2;  --This was suggested during presentation at SQL Saturday Baton Rouge
    
		            IF @NewFillFactor IS NOT NULL
		              BEGIN
		                SET @FixFillFactor = @NewFillFactor
		                UPDATE r
		                    SET FixFillFactor = @NewFillFactor,
								DelFlag       = CASE WHEN r.ID = @MinFragID THEN 0 ELSE 1 END	--DelFlag all other rows
							FROM [Admin].AgentIndexRebuilds r
							JOIN #Temp4 t
							  ON t.ID = r.ID

                        IF @ShowProcessSteps = 1
							SELECT 'New FixFillFactor set', * FROM [Admin].AgentIndexRebuilds WHERE ID = @ID;
					  END	--Begin at Line 545
				END			--BEGIN at Line 528
          END				--Begin at Line 502

			IF @ShowProcessSteps = 1
				SELECT 'FixFillFactor, line 561',GETDATE(),@FixFillFactor [@FixFillFactor], @indexname [@indexname]

/**********************************************************************
    Cannot reset fillfactor if table is partitioned, but can rebuild 
        the specified partition number
***********************************************************************/
            IF @partitionnum > 1 OR 
                 EXISTS (SELECT 1 FROM sys.partitions p 
                                  WHERE p.object_id = @objectid
                                    AND p.Index_ID  = @indexid
                                    AND p.partition_number > 1)
                SET @PartitionFlag = 1
            ELSE
                SET @PartitionFlag = 0    

            SET @OldFillFactor = @FillFactor    
            IF @ShowProcessSteps = 1 
				SELECT 'check for partitioned, line 578',GETDATE(),@PartitionFlag [@PartitionFlag], @OldFillFactor [@OldFillFactor]


/**********************************************************************
     This is the logic for changing fill factor per index

     Clustered Indexes are perturbed by decrementing the current fill 
     factor by 1 and nonclustered indexes fill factors are decremented 
     by one or two depending on the length of time since the index was 
     last rebuilt.  Code is there to ensure the perturbed fill factor 
     is never less than 70%.  This is an arbitrary number I set and 
     can be changed if required.
***********************************************************************/
            IF @FixFillFactor IS NULL AND @WorkDay = 1
              BEGIN
                SET @FillFactor = CASE  WHEN @RedoFlag = 1 
                                            THEN 100  --to catch redo index
                                        WHEN @indexid = 1 AND 
                                             @LagDate IS NULL 
                                             THEN 100
                                        WHEN @indexid = 1 AND 
                                             @LagDate IS NOT NULL			-- was @LagDate < 30 
                                             THEN @FillFactor -1
                                        --nonclustered indexes, 
                                        --  decrement fill factor 
                                        --  depending on Lag days.
                                        ----if already 100 then ratchet down
 --                                       WHEN @indexid > 1 AND 
 --                                            @LagDate IS NULL AND 
 --                                            @FillFactor = 100 
 --                                           THEN 98    
                                        WHEN @indexid > 1 AND 
                                             @LagDate IS NULL 
                                            THEN 100
                                        WHEN @indexid > 1 AND 
                                             @LagDate <  14 
                                            THEN @FillFactor -2
                                        WHEN @indexid > 1 AND 
                                             @LagDate >= 14 
                                            THEN @FillFactor -1
                                        ELSE @FillFactor END    

                        -- never let FillFactor get to less than 70
                IF @FillFactor < 70 
                  BEGIN    
                    SET @FillFactor = 70    
                    SELECT 'FillFactor adjusted back to 70.', GETDATE();    
                    UPDATE [Admin].AgentIndexRebuilds
                        SET FixFillFactor  = @FillFactor
                        WHERE DBName       = @Database
                          AND Object_ID    = @objectid
                          AND Index_ID     = @indexid
                          AND PartitionNum = @partitionnum
						  AND DelFlag      = 0
                  END		--Begin at Line 622
                END			--Begin at Line 592
            ELSE
                SET @FillFactor = CASE WHEN @FixFillFactor IS NOT NULL
                                       THEN  @FixFillFactor
                                       ELSE @FillFactor END 
        IF @ShowProcessSteps = 1 
			SELECT 'calculate new fillfactor, line 639',GETDATE(),@FillFactor [@FillFactor],@FixFillFactor [@FixFillFactor],@PartitionFlag [@PartitionFlag],@WorkDay [@WorkDay]

            /**********************************************************
                Index is not partitioned
            ***********************************************************/
            IF @PartitionFlag = 0 AND @WorkDay = 1
              BEGIN
			    UPDATE [Admin].AgentIndexRebuilds
					SET ActionTaken = 'F'
					WHERE ID       = @ID
                SET @command = N'SET QUOTED_IDENTIFIER ON     
				    SET LOCK_TIMEOUT 20000;
                    ALTER INDEX ' + @indexname +' ON [' + @schemaname + 
                    N'].[' + @objectname + N'] REBUILD WITH (' + @online_on_string +
                    ' DATA_COMPRESSION = ROW,MAXDOP = 1,FILLFACTOR = '+
                    CONVERT(NVARCHAR(5),@FillFactor) + ');';   
              END		--BEGIN at Line 645

            /**********************************************************
            IF Index is partitioned or this is Saturday or Sunday, 
            rebuild, but don't perturb with fill factor
            ***********************************************************/    
            IF @PartitionFlag = 1   
               BEGIN
			    UPDATE [Admin].AgentIndexRebuilds
					SET ActionTaken = 'R'
					WHERE ID       = @ID
                 SET @FillFactor = @OldFillFactor    
                 SET @command = N'SET QUOTED_IDENTIFIER ON     
				     SET LOCK_TIMEOUT 20000;
                     ALTER INDEX ' + @indexname +' ON [' + @schemaname 
                     + N'].[' + @objectname + N'] REBUILD PARTITION = ' 
                     + CONVERT(VARCHAR(25),@PartitionNum) + 
                     N' WITH (' + @online_on_string + 'DATA_COMPRESSION = ROW,MAXDOP = 1);';    
               END		--BEGIN at Line 662

            IF @WorkDay = 0 AND @PartitionFlag = 0   
               BEGIN
			    UPDATE [Admin].AgentIndexRebuilds
					SET ActionTaken = 'R'
					WHERE ID       = @ID
                 SET @command = N'SET QUOTED_IDENTIFIER ON 
				     SET LOCK_TIMEOUT 20000;
                     ALTER INDEX ' + @indexname +' ON [' + @schemaname 
                     + N'].[' + @objectname + N'] REBUILD ' + 
                     N' WITH (' + @online_on_string + 'DATA_COMPRESSION = ROW,MAXDOP = 1);';    
               END		--BEGIN at Line 676

            IF @ShowDynamicSQLCommands = 1 SELECT GETDATE(),@command [@command]

            --Setup loop to see if any index is currently being rebuilt, if so loop until no result set
			SET @Counter = 1
  			--Try Catch logic added because of errors caused by other on-going processes
            SET @Message = '';
			WHILE (@Retry > 0)
				BEGIN
					BEGIN TRY
						SELECT GETDATE(), @Retry [@Retry];
                        --Setup loop to see if any index is currently being rebuilt ONLINE, if no loop until no result set
			            SET @Counter = 1
                        WHILE @Counter = 1
                        BEGIN				--this loop needed to prevent object concurrency error, checks to see if ONLINE operation already in existence
						                    -- for the current index. If so, then loops every 5 seconds until ONLINE operation finished. (SS2012 issue only)
                            IF NOT EXISTS (SELECT * FROM (SELECT r.session_id
                                                                ,CONVERT(VARCHAR(1000),(SELECT SUBSTRING(text,r.statement_start_offset/2,
                                                                CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)
                                                              FROM sys.dm_exec_sql_text(sql_handle))) AS IndexScript
                                                FROM sys.dm_exec_requests r WHERE command IN ('Alter Index')) sub
                                                WHERE sub.IndexScript LIKE '%ONLINE%'
												  AND sub.IndexScript LIKE ('%' + @objectname + '%') )
                            BEGIN
							    SET @Counter = 0;
								BREAK;
                            END         -- BEGIN at Line 709
							SELECT 'Looping', GETDATE()
			                WAITFOR DELAY '00:00:05.000'				--5 second delay
	                    END              -- BEGIN at Line 700
						EXEC sys.sp_executesql @command
                        CHECKPOINT;                                                      --added to ensure transaction log backup getting everything
						SET @Retry = 0;
					END TRY
					BEGIN CATCH
						SELECT @Error = @@ERROR,@ObjectName = Object_Name(@@ProcID),@ErrorMessage = ERROR_MESSAGE(),@ErrorSeverity	= ERROR_SEVERITY(),@ErrorState = ERROR_STATE(),@ErrorLine = ERROR_LINE(); 
						SET @Message = N'********************ErrorMsg: ' + @ObjectName + N', Line ' + CONVERT(NVARCHAR(20),@ErrorLine) + N', ' + @ErrorMessage;
		                SELECT @Error [@Error], @Message [@Message];
		                SET @retry = @retry - 1;

					    IF (@retry > 0 )
						        -- use a delay if there is a high rate of write conflicts (41302)
						        --   length of delay should depend on the typical duration of conflicting transactions
							BEGIN
								WAITFOR DELAY '00:01:00.000'				--60 seconds delay
								SELECT @@SPID [@@SPID];
								CONTINUE;
							END           -- BEGIN at 729
						ELSE       
							BEGIN
								SELECT 'Error at Line 736',GETDATE(),@Message;
								SELECT 'Retry errored out for', @Command;
								SET @DeadLockFound = CASE WHEN @Error = 1205 THEN 1
											 ELSE 0 END;
	                            UPDATE [Admin].AgentIndexRebuilds
	                                SET ActionTaken   = 'E',
                				        DeadLockFound = @DeadLockFound,
									    LagDays       = @LagDate
	                                WHERE ID          = @ID;
								BREAK;
						    END    --BEGIN at Line 735
                    END CATCH
				END		           --BEGIN at Line 694


			IF @Error = 0
			  UPDATE air
			    SET IndexRebuildDuration = DATEDIFF(second,@StartTime,GETDATE()),
				    New_Fragmentation = ps.avg_fragmentation_in_percent,
					LagDays = @LagDate,
					[FillFactor] = @FillFactor
				FROM [Admin].AgentIndexRebuilds air
			    JOIN sys.dm_db_index_physical_stats 
			         (@DatabaseID,@objectid,@indexid,@partitionnum,'SAMPLED') ps
			      ON  ps.index_id         = air.index_id
			      AND ps.[object_id]      = air.[object_id]
			      AND ps.partition_number = air.partitionnum
			      AND ps.index_level      = 0
				WHERE air.ID                    = @ID
			      AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
			      AND ps.index_level          = 0;   

			IF @ShowProcessSteps = 1
				BEGIN
				  SELECT 'Row complete, Line 770',GETDATE(),* 
					FROM [Admin].AgentIndexRebuilds r
					WHERE r.ID              = @ID;
				END		--BEGIN at Line 769

             SET @PartitionFlag = 0; 
             FETCH NEXT FROM [workcursor] 
                 INTO @ID,@Database,@SchemaName,@objectid, @indexid, @partitionnum, @frag, @FillFactor
                     ,@objectname,@indexname,@LagDate,@RedoFlag;    
          END		--Begin at line 478 (start of cursor while block)
          -- Close and deallocate the cursor. 
            CLOSE [workcursor];     
            DEALLOCATE [workcursor];    
         IF @ShowProcessSteps = 1 
			SELECT 'CLOSE [workcursor] ',GETDATE();      

            --clean up
            IF OBJECT_ID(N'tempdb..#Temp2') IS NOT NULL 
                DROP TABLE #Temp2;   
            IF OBJECT_ID(N'tempdb..#Temp3') IS NOT NULL 
                DROP TABLE #Temp3;  
      END --Begin at Line 426  
    IF @ShowProcessSteps = 1 
		SELECT 'cleanup', GETDATE();

    --Data retention
    DELETE [Admin].AgentIndexRebuilds
        WHERE  CreateDate < DATEADD(yy,-3,@Date)
		   OR (CreateDate < DATEADD(yy,-1,@Date) AND DelFlag = 1);
    IF @ShowProcessSteps = 1 
		SELECT 'Data retention',GETDATE();

	--Send email report of Indexes touched
	IF OBJECT_ID(N'tempdb..#Temp5') IS NOT NULL DROP TABLE #Temp5
	SELECT DISTINCT IndexName
		, CONVERT(DEC(6,2),Current_Fragmentation) Frag
		,ISNULL(BadPageSplits,0) BadPageSplits
		,[FillFactor]
		,ISNULL(CONVERT(VARCHAR(7),LagDays),'       ') LagDays
		,ISNULL(CONVERT(VARCHAR(4),RedoFlag),'     ') Redo
		,ActionTaken [Action]
	INTO #Temp5
	FROM [Admin].AgentIndexRebuilds
	WHERE CreateDate = @Date
	ORDER BY IndexName;
	SET @RowCount = @@ROWCOUNT;

    IF @ShowProcessSteps = 1 
		SELECT 'EMail query',GETDATE(),* from #Temp5

	SET @xml = CAST(( SELECT [IndexName] AS 'td','',[Frag] AS 'td','', [BadPageSplits] AS 'td','', [FillFactor] AS 'td','', [LagDays] AS 'td','', [Redo] AS 'td','', [Action] AS 'td'
		FROM #Temp5
		ORDER BY IndexName 
	FOR XML PATH('tr'), ELEMENTS ) AS NVARCHAR(MAX))


	SET @body ='<html><body><H3>SQLAgent FillFactor for '+CONVERT(NVARCHAR(10),@Date,112) + '</H3>
<table border = 1> 
<tr>
<th> IndexName </th> <th> Frag </th> <th> BadPageSplits </th> <th> FillFactor </th>  <th> LagDays </th>  <th> Redo </th>  <th> Action </th> </tr>'    
 
	SET @body = @body + @xml +'</table></body></html>';
	SET @Message = 'SQL Agent FillFactor Report from ' + @SvrName;

    IF @ShowProcessSteps = 1
		SELECT 'Sending Fillfactor email ',GETDATE(),@Database [@Database];

	EXEC msdb.dbo.sp_send_dbmail
	@profile_name = @ProfileName, -- replace with your SQL Database Mail Profile (see lines 97-100)
	@body = @body,
	@body_format ='HTML',
	@recipients = 'mbyrdtx@gmail.com', -- replace with your email address
	@subject = @Message ;

	IF OBJECT_ID(N'tempdb..#Temp5') IS NOT NULL DROP TABLE #Temp5

	--Big $64 question is do I want to keep the R (rebuild) and E (Error) ActionTaken rows in the history table.  Right now they remain!
	--DELETE [Admin].AgentIndexRebuilds
	--	WHERE ActionTaken IN ('R','E');

  END		--Begin at Line 111
RETURN;
GO
