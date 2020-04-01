--© 2019 | ByrdNest Consulting

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
    BEGIN
        --define Admin schema if not exists
        IF NOT EXISTS (SELECT 1 from sys.schemas WHERE [name] = 'Admin')
            EXEC sp_executesql N'CREATE SCHEMA [Admin] AUTHORIZATION [dbo]'    
        --define Admin.AgentIndexRebuilds if not exists
        IF OBJECT_ID(N'Admin.AgentIndexRebuilds') IS NULL
            EXEC sp_executesql N'
SET ANSI_NULLS ON
--GO
SET QUOTED_IDENTIFIER ON
--GO
CREATE TABLE [Admin].AgentIndexRebuilds(
	ID INT IDENTITY(1,1) NOT NULL,		--Primary Key
	CREATEDATE DATETIME NOT NULL,		--Create date for row
	DBName SYSNAME NOT NULL,			--Database Name	
	SchemaName SYSNAME NOT NULL,		--Table/Index Schema
	TableName SYSNAME NOT NULL,			--Table Name
	IndexName SYSNAME NOT NULL,			--Index Name
	PartitionNum INT NOT NULL,			--Partition Number 
	Current_Fragmentation FLOAT NOT NULL,	--Index fragmentation in %
	New_Fragmentation FLOAT NULL,		--Index fragmentation after rebuild
	PageSplitForIndex BIGINT NULL,		--Good & Bad Page Split Count
	BadPageSplits BIGINT NULL,			--Bad Page Split Count
	New_PageSplitForIndex BIGINT NULL,	--Good & Bad Page Split Count after rebuild
	PageAllocationCausedByPageSplit BIGINT NULL,	--Page splits at intermediate level
	New_PageAllocationCausedByPageSplit BIGINT NULL,  --Page splits int lvl after rebuild
	[FillFactor] INT NULL,				--Current Fill Factor
	[Object_ID] INT NULL,				--Object ID
	Index_ID INT NULL,					--Index ID
	Page_Count BIGINT NULL,				--Page count for index
	Record_Count BIGINT NULL,			--Record count for index
	Forwarded_Record_Count BIGINT NULL, --n/a (heaps)
	New_Forwarded_Record_Count BIGINT NULL,	--n/a (heaps)
	LagDays INT NULL,					--# of days since last rebuild
	FixFillFactor INT NULL,				--Final fill factor determination
	DelFlag INT NULL,					--0 - active, 1 = soft delete
 CONSTRAINT PK_AgentIndexRebuilds PRIMARY KEY NONCLUSTERED 
	(ID ASC) )

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


USE ROICore
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
	Object_ID int NULL,
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


