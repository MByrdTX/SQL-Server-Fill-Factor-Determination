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

Also in this folder is a PowerPoint presentation I gave on this topic at the local Austin SQL Server User?s 
Group (CACTUSS). This session was taped and can be found on [usergroup.tv](http://usergroup.tv/videos/a-self-tuning-fill-factor-technique-for-sql-server)


