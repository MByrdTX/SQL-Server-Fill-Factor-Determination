--© 2024 | ByrdNest Consulting

-- ensure a USE <databasename> statement has been executed first. 
USE <TestDatabaseName>    
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

****************************************************************************************/

SET QUOTED_IDENTIFIER OFF    
GO

--check to see if Admin schema exists
DECLARE @Database SYSNAME = (SELECT DB_NAME())    
/********************************************************************
code setup for Always On Primary Node; comment out next 4 statements
      if not an Always On Node
**********************************************************************/
--  DECLARE @preferredReplica INT
--  SET @preferredReplica 
--    = (SELECT [master].sys.fn_hadr_backup_is_preferred_replica(@Database))
--  IF (@preferredReplica = 0)
    BEGIN		--Dynamic SQL because of SS restriction on CREATE TABlE being first statement in query batch
        --define Admin schema if not exists
        IF NOT EXISTS (SELECT 1 from sys.schemas WHERE [name] = 'Admin')
            EXEC sp_executesql N'CREATE SCHEMA [Admin] AUTHORIZATION [dbo]'    
        --define Admin.AgentIndexRebuilds if not exists
        IF OBJECT_ID(N'Admin.AgentIndexRebuilds') IS NULL
            EXEC sp_executesql N'
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
CREATE TABLE [Admin].[AgentIndexRebuilds](
	[ID] [int] IDENTITY(1,1) NOT NULL,          --Primary Key
	[CreateDate] [date] NOT NULL,               --Create date for row   
	[DBName] [sysname] NOT NULL,                --Database Name
	[SchemaName] [sysname] NOT NULL,            --Table/Index Schema
	[TableName] [sysname] NOT NULL,             --Table name
	[IndexName] [sysname] NOT NULL,             --Index Name
	[PartitionNum] [int] NOT NULL,              --Partition Number
	[Current_Fragmentation] [float] NOT NULL,   --Index fragmentation in %
	[New_Fragmentation] [float] NULL,           --Index fragmentation after rebuild
	[BadPageSplits] [bigint] NULL,              --Good & Bad page split count
	[FillFactor] [int] NULL,                    --Current FillFactor
	[Object_ID] [int] NULL,                     --Object ID
	[Index_ID] [int] NULL,                      --Index ID
	[Page_Count] [bigint] NULL,                 --Page Count for Index
	[Record_Count] [bigint] NULL,               --Record Count for index
	[LagDays] [int] NULL,                       --# of days since last rebuild
	[FixFillFactor] [int] NULL,                 --Final fill factor determination
	[DelFlag] [bit] NULL,                       --0 - active; 1 = soft delete
	[DeadLockFound] [bit] NULL,                 --Deadlock found during Rebuild
	[IndexRebuildDuration] [int] NULL,          --Rebuild duration in seconds
	[RedoFlag] [bit] NULL,                      --If deadlock found, RedoFlag = 1
	[ActionTaken] [char](1) NULL,               --R = Rebuild only; E = Error; F = Find FillFactor 
 CONSTRAINT [PK_AgentIndexRebuilds] PRIMARY KEY CLUSTERED 
	([ID] ASC)
	WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 98, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF, DATA_COMPRESSION = ROW) ON [PRIMARY]
	) ON [PRIMARY]

ALTER TABLE Admin.AgentIndexRebuilds ADD  DEFAULT (getdate()) FOR CREATEDATE'
    END
GO

-- Below code from https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/
-- Drop the Event Session so we can recreate it 
IF EXISTS (SELECT 1 
            FROM sys.server_event_sessions 
            WHERE name = 'SQLskills_TrackPageSplits')
    DROP EVENT SESSION [SQLskills_TrackPageSplits] ON SERVER

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
GO

-- Start the Event Session
ALTER EVENT SESSION [SQLskills_TrackPageSplits]
ON SERVER
STATE=START;
GO



SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE Admin.BadPageSplits(
	ID INT IDENTITY(1,1) NOT NULL,
	CREATEDATE datetime NOT NULL,
	TableName sysname NOT NULL,
	IndexName sysname NOT NULL,
	PartitionNum int NOT NULL,
	Current_Fragmentation float NOT NULL,
	BadPageSplits bigint NULL,
	[FillFactor] int NULL,
	[Object_ID] int NULL,
	Index_ID int NULL,
	Page_Count bigint NULL,
	Record_Count bigint NULL,
 CONSTRAINT PK_BadPageSplits PRIMARY KEY 
	(ID ASC)
	WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = ROW) 
) 
GO

ALTER TABLE Admin.BadPageSplits ADD  DEFAULT (getdate()) FOR CREATEDATE
GO