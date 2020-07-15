# Fill Factor Readme

Within this folder are two TSQL scripts.  FillFactorIndexSetup.sql specifies a Admin schema if not already 
there, builds a rebuild history table, and establishes an extended event to count number of bad page 
splits.

SQLAgentScriptRebuildIndexes.sql is a script I run as a SQL Agent job nightly that perturbs indexes with 
average fragmentation greater than 1.2% and tracks fragmentation to determine and set a fill factor.  
This script is still evolving and may change daily.

I am working on getting both of these scripts into GitHub and use it as a versioning tool.  My GitHub is is 
MByrdTX.  Look for it there in the near future.

Also in this folder are two word documents that have been published at SQLServerCentral.com.  The 
script attached to Part1 has a typo in it and has since evolved as described above.

Also in this folder is a GroupBy PowerPoint presentation I gave on May 13.  A video recording should be available from GroupBy.com.

20200515:
Wish List for Improvements:
1.  Add code for multiple databases on same server.
2.  Would be nice to identify a performance factor for fill factor value vs pages used.
3.  Rewrite code (current code is descended from original proof of concept) with additional documentation as needed.
4.  Current fill factor approach not implemented for partitioned indexes (it would really be nice to have fill factor per partition; that way old data partitions could be set to 100% and the active partitions as needed. {this should be placed on Microsoft's wish list :) )
5.  Add additional features as suggested by the SQL Server community
6.  Need more testing to validate performance improvements with firm numbers.
7.  Need better way of monitoring table Admin.AgentIndexRebuilds -- both manually and also self reviewing.  Any volunteers out there to implement a dash board?
8.  Compare index growth vs fill factor tweaks.

20200616:
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

Also today revised logic (commented out some lines) to ensure first pass at an index is always with fill factor = 100.


20200713:

Revised Scripts for Setup (including AgentIndexRebuilds) table, and SQLAgentScriptRebuildIndexes scripts.  This was something on my wish letter for rewriting the code.  I am still experimenting and tweaking as I find new edge conditions.  One of the major changes in the AgentIndexRebuilds table was to add additional column to help with reporting.  This whole script has evolved to a combined defrag and fill factor determination process.  Current it wil only defrag partitioned scripts and on Saturday/Sunday top 20 worst fragmentation/BadPageSplits.  

I've added the following new columns to AgentIndexRebuilds table:
     DeadLockFound is a bit field, 0 means no deadlocks errors (number of retries < 6) and 1 means there were 6 retries with no luck in running the dynamic SQL script.  When 1, then that row should be treated as an error.
     RedoFlag is a bit field, when = 1 means this is an idex that the fillfactor has been static for more than 90 days and is undergoing a new evaluation.
     ActionTaken is a CHAR(1) field with following values:  R indicates a successful rebuild, E indicates a possible error, and F indicates the fillfactor was tweaked.

20200715:
Thanks for the many comments I've received from my SQLFriday presentation last Friday.  They've inspired me to satisfy two of the items on my wish list above -- rewrite the code (also involved changes to the history table (Admin.AgentIndexRebuilds) and added an email report at the end of the script.  As usual I welcome all comments.

