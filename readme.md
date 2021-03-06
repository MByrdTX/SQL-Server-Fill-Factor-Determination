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


20200719:
Found major error in clustered index from code for 20200713.  Error is fixed, but if you have run the FillFactorIndexSetup.sql script since 20200713, you should drop the PK and CIX for Admin.AgentIndexRebuils and create a new PK (clustered) based just on ID as originally done.  

20200803:
Finally found best and correct way (if I just have read the documentation) to JOIN sys.allocation_units to sys.partitions.  Also discovered that my transaction log was filling up from the index rebuilds.  This was causing weird errors in my SQLAgentScriptRebuildIndexes script.  But all is resolved now and I think the code will be stable.  I welcome any and all comments (good or bad); please send them to mbyrd@byrdnest-tx.com.


20200808:

I am truely sorry for all the recent updates to the script.  I have been  battling an issue and it turns out it is a SS2012 issue (online index rebuilds) that Microsoft never completely resolved.  The code appears to be stable for the newer versions, and I am still going to continue to trouble-shoot the SS2012 issue.
 

20200812:

I am slowly sneaking up on the SS2012 issues for ONLINE concurrency issues I've encountered.  Yesterday I added code to check if there is an existing ONLINE operation on the applicable index before rebuilding.  IF so, I do a 5 sec loop until the existing ONLINE operation is complete. AND it worked -- I did not get the concurrency errors as before!

I am still encountering duplicate rows and I am not sure where they are coming from.  It is as if the script is running a second time in close time scenario as the original scheduled script (but I am only getting one set of data in the defrag log).  What is interesting is that frequently the duplicate row (but not all the time) has the bad page splits from the intermediate level = 1 instead of level 0.

If anyone else is encountering these issues (or others) please email me at mbyrd@byrdnest-tx.com and I'll keep on plugging away until the code is works as designed.  Your inputs and ideas are most welcome.

I've been working on this now for ~17 months and have seen much improved performance benefits on the very active OLTP database.  I just want it to get finished.

Others areas that I've found that you may want to consider if you implement this approach:

     * Index rebuilds are logged in the transaction log.
          * Make sure transaction log can accomodate the rebuilds without running out of space.
          * ONLINE rebuilds take more space than OFFLINE rebuilds (also take longer).
          * If in full recovery mode, ensure transaction log backups are frequent enough so that you don't run out of file space.


20200818:

Turns out I've been battling a Microsoft Alway On feature (bug?) for the last 2 months.  The script as checked in today should be very stable and has worked successfully last 5 days.  

I'll talk more about the feature I've found in a forth-coming SQLServerCentral.com article.  What was happening was that the SQL Agent job for my fill factor script on the secondary node (passive) was running in parallel but acting on the primary node -- this was completely an unexpected behavior that I just didn't even consider until the evidence was right before me.  Stand by, I'll document this "feature" hopefully within next 2-3 weeks.


Cheers,
Mike

