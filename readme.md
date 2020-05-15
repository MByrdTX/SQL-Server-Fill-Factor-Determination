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

I (Mike Byrd) am still monitoring and analyzing the code and results from Admin.AgentIndexRebuilds and Admin.BadPageSplits.  This resulted in a minor tweak to the SQL Agent code (SQLAgentScriptRebuildIndexes.sql) today.



