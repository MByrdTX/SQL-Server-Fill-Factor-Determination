--© 2020 | ByrdNest Consulting

-- ensure a USE <databasename> statement has been executed first. 
--USE <Database>    
GO    
/****************************************************************************************

    Index Rebuild (defrag) script with logic from Jeff Moden and
        additional logic to change fillfactor as needed

    Designed to work with SS2012 and later, Enterprise Edition and 
        Developer Edition.  If you are using Standard Edition, you
        will need to modify the dynamic SQL and remove ONLINE = ON

    There is a setup script that needs to be run first and then never
        again -- FillFactorIndexSetup.sql.  It creates an Admin schema,
        a history table (AgentIndexRebuilds) and a SQL Server 
        transaction_log extended event (to track mid-page splits).
    
    You may alter this code for your own *non-commercial* purposes. You may
     republish altered code as long as you include this copyright and give 
     due credit. 


    THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
    ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
    TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
    PARTICULAR PURPOSE. 

    Created      By           Comments
    20190424     Mike Byrd    Created
    20190513     Mike Byrd    Added additional data columns to 
                                  AgentIndexRebuilds table
    20190604     Mike Byrd    Added additional logic for setting FixFillFactor
    20190616     Mike Byrd    Revised FillFactor logic
    20190718     Mike Byrd      Added logic to get bad page splits (thanks to Jonathan Kehayias)
                                 https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/
	20200712	 Mike Byrd	  Revised Admin.AgentIndexRebuilds table for better daily reporting, removed/added columns

****************************************************************************************/

DECLARE @Database     SYSNAME = (SELECT DB_NAME());   
DECLARE @DatabaseID   SMALLINT = DB_ID(@Database);
SET QUOTED_IDENTIFIER OFF    


--check to see if migrating from old AgentIndexRebuilds table or creating new AgentIndexRebuilds table
IF EXISTS (SELECT 1 FROM sys.sysobjects o JOIN sys.columns c ON c.object_id = o.id 
                    WHERE o.[name] = 'AgentIndexRebuilds' AND o.xtype  = 'U' AND c.[name] = 'PageSplitForIndex')
	BEGIN
		-- this is migration path to migrate data from old AgentIndexRebuilds table to new AgentIndexRebuilds table
		--	this path also assumes Admin schema has already been generated.  
		EXEC sp_rename 'Admin.AgentIndexRebuilds','AgentIndexRebuildsOld';
		EXEC sp_rename '[Admin].AgentIndexRebuildsOld.PK_AgentIndexRebuilds','PK_AgentIndexRebuildsOld';

		--need to use dynamic SQL to generate new AgentIndexRebuilds table
        IF OBJECT_ID(N'Admin.AgentIndexRebuilds') IS NULL
            EXEC sp_executesql N'
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [Admin].AgentIndexRebuilds(
	ID INT IDENTITY(1,1) NOT NULL,		--Primary Key
	CreateDate DATE NOT NULL,		    --Create date for row
	DBName SYSNAME NOT NULL,			--Database Name	
	SchemaName SYSNAME NOT NULL,		--Table/Index Schema
	TableName SYSNAME NOT NULL,			--Table Name
	IndexName SYSNAME NOT NULL,			--Index Name
	PartitionNum INT NOT NULL,			--Partition Number 
	Current_Fragmentation FLOAT NOT NULL,	--Index fragmentation in %
	New_Fragmentation FLOAT NULL,		--Index fragmentation after rebuild
	BadPageSplits BIGINT NULL,			--Bad Page Split Count
	[FillFactor] INT NULL,				--Current Fill Factor
	[Object_ID] INT NULL,				--Object ID
	Index_ID INT NULL,					--Index ID
	Page_Count BIGINT NULL,				--Page count for index
	Record_Count BIGINT NULL,			--Record count for index
	LagDays INT NULL,					--# of days since last rebuild
	FixFillFactor INT NULL,				--Final fill factor determination
	DelFlag BIT NULL,					-- 0 - active, 1 = soft delete
	DeadLockFound BIT NULL,				-- 0 - no deadlocks, 1 - errored out after deadlock retries
	IndexRebuildDuration INT NULL,		-- Duration (seconds) for each index rebuild
	RedoFlag BIT NULL,					-- 0 - no redo; 1 - Redo (more than 90 days since last tweaked)
	ActionTaken CHAR(1) NULL            -- R - ReBuild, E - Error, F - FillFactor tweaked
 CONSTRAINT PK_AgentIndexRebuilds PRIMARY KEY NONCLUSTERED 
	(ID ASC) )

ALTER TABLE [Admin].AgentIndexRebuilds ADD  DEFAULT (CONVERT(DATE,getdate())) FOR CREATEDATE

CREATE UNIQUE CLUSTERED INDEX CIX_AgentIndexRebuilds ON [Admin].AgentIndexRebuilds
	(CreateDate ASC, DBName ASC, SchemaName ASC, TableName ASC, IndexName ASC, PartitionNum ASC)
	WITH (DATA_COMPRESSION = Row, FILLFACTOR = 100)';

		--now migrate data
		SET IDENTITY_INSERT [Admin].AgentIndexRebuilds ON;
		INSERT [Admin].AgentIndexRebuilds
			(ID,CreateDate,DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation,New_Fragmentation,BadPageSplits
			,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,LagDays,FixFillFactor,DelFlag,DeadLockFound,IndexRebuildDuration
			,RedoFlag,ActionTaken)
			SELECT ID,CONVERT(DATE,CreateDate),DBName,SchemaName,TableName,IndexName,PartitionNum,Current_Fragmentation,New_Fragmentation,BadPageSplits
					,[FillFactor],[Object_ID],[Index_ID],Page_Count,Record_Count,LagDays,FixFillFactor,DelFlag, NULL DeadLockFound, NULL IndexRebuildDuration,NULL,'F'
				FROM [Admin].AgentIndexRebuildsOld;
		SET IDENTITY_INSERT [Admin].AgentIndexRebuilds OFF;

--		DROP TABLE [Admin].AgentIndexRebuildsOld;
	END
ELSE
    BEGIN
        --define Admin schema if not exists
        IF NOT EXISTS (SELECT 1 from sys.schemas WHERE [name] = 'Admin')
            EXEC sp_executesql N'CREATE SCHEMA [Admin] AUTHORIZATION [dbo]'    
        --define Admin.AgentIndexRebuilds if not exists
        IF OBJECT_ID(N'Admin.AgentIndexRebuilds') IS NULL
            EXEC sp_executesql N'
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [Admin].AgentIndexRebuilds(
	ID INT IDENTITY(1,1) NOT NULL,		--Primary Key
	CreateDate DATE NOT NULL,		    --Create date for row
	DBName SYSNAME NOT NULL,			--Database Name	
	SchemaName SYSNAME NOT NULL,		--Table/Index Schema
	TableName SYSNAME NOT NULL,			--Table Name
	IndexName SYSNAME NOT NULL,			--Index Name
	PartitionNum INT NOT NULL,			--Partition Number 
	Current_Fragmentation FLOAT NOT NULL,	--Index fragmentation in %
	New_Fragmentation FLOAT NULL,		--Index fragmentation after rebuild
	BadPageSplits BIGINT NULL,			--Bad Page Split Count
	[FillFactor] INT NULL,				--Current Fill Factor
	[Object_ID] INT NULL,				--Object ID
	Index_ID INT NULL,					--Index ID
	Page_Count BIGINT NULL,				--Page count for index
	Record_Count BIGINT NULL,			--Record count for index
	LagDays INT NULL,					--# of days since last rebuild
	FixFillFactor INT NULL,				--Final fill factor determination
	DelFlag BIT NULL,					-- 0 - active, 1 = soft delete
	DeadLockFound BIT NULL,				-- 0 - no deadlocks, 1 - errored out after deadlock retries
	IndexRebuildDuration TIME NULL,		-- Duration (seconds) for each index rebuild
	RedoFlag BIT NULL,					-- 0 or null - normal, 1 - Redo (after 90 days with fixed fillfactor)
	ActionTaken CHAR(1) NULL            -- R - ReBuild, E - Error, F - FillFactor tweaked
 CONSTRAINT PK_AgentIndexRebuilds PRIMARY KEY CLUSTERED 
	(ID ASC) )

ALTER TABLE [Admin].AgentIndexRebuilds ADD  DEFAULT (CONVERT(DATE,getdate())) FOR CREATEDATE

CREATE UNIQUE CLUSTERED INDEX CIX_AgentIndexRebuilds ON [Admin].AgentIndexRebuilds
	(CreateDate ASC, DBName ASC, SchemaName ASC, TableName ASC, IndexName ASC, PartitionNum ASC)
	WITH (DATA_COMPRESSION = Row, FILLFACTOR = 100)';
	END


-- Below code from https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/
IF NOT EXISTS (SELECT 1 
            FROM sys.server_event_sessions 
            WHERE [name] = 'SQLskills_TrackPageSplits')
	BEGIN

		-- Create the Event Session to track LOP_DELETE_SPLIT transaction_log operations in the server
		CREATE EVENT SESSION [SQLskills_TrackPageSplits]
		ON    SERVER
		ADD EVENT sqlserver.transaction_log(
		    WHERE operation = 11  -- LOP_DELETE_SPLIT 
		      AND database_id = 9 -- CHANGE THIS BASED ON TOP SPLITTING DATABASE!
		)
		ADD TARGET package0.histogram(
		    SET filtering_event_name = 'sqlserver.transaction_log',
		        source_type = 0, -- Event Column
		        source = 'alloc_unit_id');


		-- Start the Event Session
		ALTER EVENT SESSION [SQLskills_TrackPageSplits]
		ON SERVER
		STATE=START;
	END	
GO

