# SQL-Server-Fill-Factor-Determination
A procedure to determine and assign fill factor to every index in a SQL Server database

A Self-Tuning Fill Factor Technique for SQL Server – Part 1
										By Mike Byrd
										     ByrdNest Consulting

Intoduction
So what’s all the fuss about Fill Factor?  It is a SQL Server parameter I’ve ignored for 20+ years.  The main reason was/is I’ve had no idea about what value to use.  There is just no documented definitive guidance.  
Back in April at SQL Saturday 830 (Colorado Springs) I attended two sessions by Jeff Moden titled “Black Arts” Index Maintenance: How the “Best Practice” Methods are Silently Killing Performance.  These were outstanding and are downloadable from the SQLSaturday web site (https://www.sqlsaturday.com/830/Sessions/Schedule.aspx).  A Reader’s Digest version of his presentations is contrary to the popular concept to reorganize at 10% fragmentation and rebuild indexes at 30%.  He showed (in the 2 presentations) that page splits caused tremendous performance degradations and that you should probably rebuild indexes at 1% fragmentation.  I don’t want to steal his “thunder” (and it is forthcoming in another series by him).  
But on the flight back home I got to thinking about everything he said and how fill factor also “factors” into index performance and maintenance.  It got me to thinking about a brute force optimization technique I implemented way back when I worked at the Air Force Rocket Propulsion Laboratory (yes, I was once a rocket scientist and still a geek!).  It was a technique that involved tweaking various parameters (plus and minus percentages) and reducing the tweaks to an “optimum” solution.  It was not always perfect as in a multi-dimensional scenario there are valleys that this solution could drill down on, but it did offer improved solutions that let us explore options.
Frankly speaking, what happened is somewhat amazing.  Using the “still experimental” methods that I explain in this article on a client’s system, the end result is that the overall database wait times showed improvement (decreases) of about 30%.  That’s not a trivial improvement, and I hope this article will serve as an impetus for others to try these and other experiments on their indexes to come up with other improvements they might wish to share with the community at large.  
Overview
So what was needed was to collect index parametrics and look for patterns.  This involved using the sys.dm_db_index_physical_stats and sys.dm_db_index_operational_stats views and capturing these index parametrics before and after each index rebuild.  Prior to this task, a modified version of Ola Hallengren’s defrag script only starting rebuilds at 10% and no reorganizes was my choice for index fragmentation.  For this project, this script was modified to capture and store in a table the before and after parametrics for each index rebuild (heaps excepted) within a specified database.  Initially I started looking at the top 15 average fragmentation indexes greater than 1.0%, but eventually changed it to 1.2% because of logical fragmentation issues.  In the meantime, if a fill factor is not established, the previous fill factor (for that index) is decremented and the resulting average fragmentation compared to the previous average fragmentation.  (All indexes prior to this task had 100% fill factors.)  This process is repeated every 24 hours (SQL Agent job) until the index’s new average fragmentation is greater than the previous one.  The previous fill factor is notated and fixed for that specific index.  A secondary look at the data over 90 days (since the fill factor was established) inserts that record for a once again review (data collection).  While this may temporarily degrade that index’s performance it ensures that a once again “near optimum” fill factor can be established taking into account any new data skew and application utilization of the database.    
Analysis
Initially this project started mainly as data collection to ascertain patterns in the index parametrics.  As discussed earlier, sys.dm_db_index_physical_stats, sys.dm_db_index_operational_stats, sys.indexes, and sys.objects are used to gather parametrics for subsequent storage in the table [Admin].AgentIndexRebuilds.  In this section we will look at some of the patterns encountered.  The testing was done on a very active OLTP production database which we will call TestDB (to protect my client’s confidentiality).  TestDB contains ~225gb of data (real data, not stored procedures, views, etc.), 249 tables, and 694 indexes in a 24x7 environment.  There are few hard deletes as most of the data is soft-deleted (DelFlag). Only 164 indexes exceeded the average fragmentation specification (1.2%) and were recorded within the AgentIndexRebuilds table (over a 3 month period).  While each index’s pattern was examined, in the interest of brevity, we will only consider the following typical indexes (names changed to protect client):

Table	Index	Type	Rows	Size (MB)	Comments
A	A_C	Clustered	6,749,754	2,219	Active table, many updates, CI/PK is identity column
B	B_C	Clustered	12,890,405	424	Bridge Table, PK not ascending, CI/PK 2 columns randomly inserted
C	C_N	NonClustered	469,370	14	 Non-Clustered Index randomly inserted
D	D_N	NonClustered	56,571	1	 Non-Clustered Index randomly inserted
Table 1:  Sample Indexes

The A_C index is a Clustered Index/Primary Key based on an identity column.  Data is regularly added to it through the day (24 hours).  Data collected for this index is shown below:

Date	ID	Index Name	Current Fragmentation	New Fragmentation	Page Split For Index	New Page Split For Index	Fill Factor
4/17/2019	125	A_C	5.5529	 	 	 	91
4/28/2019	324	A_C	1.2012	 	 	 	89
5/7/2019	551	A_C	1.0641	 	 	 	88
5/19/2019	745	A_C	1.3925	0.0214	6246	1	87
6/1/2019	940	A_C	1.4646	0.0254	6781	1	88
6/12/2019	1130	A_C	1.2421	0.0336	5822	1	88
Table 2:  Index A_C Data
Missing data is from new columns added to table after data collection started.  The value of 1 for New Page Split For Index was puzzling at first.  Upon further investigation it appears that after a rebuild it always starts with a value of 1.  Bad news is that the Page Split For Index parameter yields both good and bad page splits.  (A good explanation of good and bad page splits can be found at http://www.sqlballs.com/2012/08/how-to-find-bad-page-splits.html.)  A little research indicates that extended events may be able to break out the bad page splits, but that is a future project.  Plotting out Current Fragmentation vs Fill Factor gives:
 
Figure 1: Fragment for Index A_C
The trend in Figure 1 is very typical.  Fill Factor usually decreases until a minimum is met.  In this case the code subsequently tried an 87 fill factor, but since the resulting fragmentation was larger than before the fill factor was fixed at 88%.  The second 88 fill factor is from a subsequent index rebuild after the fill factor was fixed.
Looking at another clustered index B_C, this one is in a bridge table where rows are continuously inserted (out of order).  Looking at the data we get:

Date	ID	Index Name	Current Fragmentation	New Fragmentation	Page Split For Index	New Page Split For Index	Fill Factor
4/17/2019	113	B_C	6.548705	 	 	 	100
4/25/2019	247	B_C	1.599545	 	 	 	99
4/30/2019	377	B_C	5.436387	 	 	 	98
5/1/2019	407	B_C	4.856919	 	 	 	97
5/7/2019	549	B_C	1.174713	 	 	 	96
5/16/2019	691	B_C	1.756837	0.221820894	1037	1	95
5/24/2019	815	B_C	1.523979	0.233713691	897	1	96
6/2/2019	950	B_C	1.214119	0.220436191	644	1	96
6/7/2019	1046	B_C	1.151839	0.232396003	656	1	96
6/14/2019	1167	B_C	1.212156	0.226648484	751	1	96
Table 3: Index B_C Data
Missing data is from new columns added to table after data collection started.  Plotting out Current Fragmentation vs date gives:
 
Figure 2:  Index B_C
This is also a typical trend for the Clustered Index.  The only data glitch was the dip in fragmentation at 99 fill factor.  This would have normally been selected as the fixed fill factor, but the select code (as before) was not in place.  Thus 96 was selected as the fixed fill factor.  I’m not sure how to handle this condition going forward; any ideas from the readers would be appreciated.
Looking at non-clustered index C_N:

Date	ID	Index Name	Current Fragmentation	New Fragmentation	Page Split For Index	New Page Split For Index	Fill Factor
4/12/2019	47	C_N	10.92865	 	 	 	93
4/17/2019	120	C_N	5.816024	 	 	 	92
4/21/2019	172	C_N	3.196622	 	 	 	91
4/24/2019	225	C_N	1.979109	 	 	 	89
4/26/2019	270	C_N	1.603421	 	 	 	87
4/28/2019	328	C_N	1.261167	 	 	 	85
4/29/2019	366	C_N	1.029336	 	 	 	83
5/1/2019	427	C_N	1.201803	 	 	 	81
5/3/2019	465	C_N	1.214772	 	 	 	79
5/5/2019	500	C_N	1.145038	 	 	 	77
5/8/2019	568	C_N	1.340111	 	 	 	75
5/15/2019	685	C_N	2.558348	0.262582057	53	1	73
5/18/2019	730	C_N	1.598963	0.252631579	31	1	71
5/21/2019	776	C_N	1.445578	0.298359025	22	1	83
5/24/2019	823	C_N	2.347188	0.29455081	37	1	83
5/25/2019	834	C_N	1.597289	0.290697674	30	1	83
5/26/2019	848	C_N	1.456311	0.292255236	27	1	83
5/27/2019	864	C_N	1.681884	0.288461538	29	1	83
5/28/2019	877	C_N	1.379638	0.285986654	23	1	83
5/31/2019	926	C_N	1.556604	0.283822138	26	1	83
6/3/2019	968	C_N	1.342616	0.3	16	1	83
6/5/2019	1015	C_N	1.535414	0.298210736	22	1	83
6/7/2019	1055	C_N	1.330705	0.296735905	17	1	83
6/9/2019	1082	C_N	1.118568	0.281531532	8	1	83
6/11/2019	1115	C_N	1.563372	0.280269058	17	1	83
6/13/2019	1156	C_N	1.551247	0.277932185	21	1	83
6/15/2019	1193	C_N	1.102536	0.276243094	16	1	83
Table 4: Index C_N
Again the code was not in place to fix the fill factor; hence the empty data slots.  In this case, the fill factor was set at 83 after an extended excursion below that.  What is really interesting is the data hopping around once the fill factor is set.  Doesn’t seem so much to be a factor of new page splits, but more so of logical fragmentation (more than one index using same page).  There is no easy way to mitigate the logical fragmentation so I’ll leave that to someone else.  As before, the fragmentation vs fill factor trend for this index is similar to before (Figure 3).  

 
Figure 3: Fragmentation for Index C_N
And finally, another non-clustered index example is D_N and collected data for it is shown below:

Date	ID	Index Name	Current Fragmentation	New Fragmentation	Page Split For Index	New Page Split For Index	Fill Factor
4/15/2019	86	D_N	8.433735	 	 	 	85
4/28/2019	349	D_N	1.075269	 	 	 	83
4/29/2019	375	D_N	1.052632	 	 	 	81
4/30/2019	404	D_N	1.030928	 	 	 	79
5/13/2019	658	D_N	9.009009	0.884955752	11	1	77
Table 5: Index D_N
In this specific case, had the fix fill factor code been in place to catch the 4/28 case, the fill factor should have been set to 83.  But you can see that it continues to decrease some.  Not sure which is best, 83, 81, or 79, but at least we are in the ball park and the fill factor is not 100 as it started out to be.  Looking at the graph for this index we get
 
Figure 4:  Index D_+N
Obviously the optimum fill factor for the current data is between 79-83%.  In this particular case I would want to err on the bigger fill factor to reduce number of logical IOs on the index.  This is what the code currently does.
There are other instances (after studying Admin.AgentIndexRebuilds) that the code just doesn’t handle.  I will eventually figure out what works, but now I review the table weekly to see what indexes the code is not catching.
For a short time I considered using a least squares quadratic match to the data to determine best fill factor, but disregarded it to keep the current design as simple as it is.
Unfortunately I do not have hard numbers to measure performance improvements.  But I did have some screen shots from the free version of DPA (Solarwinds Database Performance Analyzer).  
 
Figure 5:  DPA screen shot from late Feb 2019
and from June 2019 (after 3 months of Fill Factor adjustment):
 Figure 6:  DPA screen shot from late June 2019
Notice that the vertical axis decreased from 500 seconds to 350 seconds.  Each of the screen shots were very typical throughout the day for periods referenced. DPA showed an overall decrease in database wait times of about 30%.  This is a significant number.  I attribute it to 2 factors:  
1.	With decreased fragmentation, there are less page splits to wade through for IO range searches.
2.	With improved fill factor, there are less bad page splits.  The cost of a page split can be significant.  First the new page has to be identified, pointers made to it, data movement from the old page to the new page and finally all of this is logged.  
Requirements:
This code was developed and tested with SQL Server 2017 Enterprise Version.  However, it is applicable to all versions of SQL Server from 2012 and upward.  It can also be used for Standard Editions (same versions) except the WITH Statement in the Index Rebuilds needs to have the “ONLINE = ON, Data_Compression = ROW,” statement removed (3 occurrences).  Data_Compression = ROW option can (and should be) retained in SS 2016, SP 2 and upward for Standard Edition.  In the event you are using Standard Edition consider scheduling this task when there is minimal database activity as without the ONLINE option you will have schema locking during the index rebuilds.  Otherwise, there are no other restrictions that I am aware of.
Caveats:
This methodology is not complete, but does represent a start on determining fill factor.  Hopefully it will evolve (along with the community’s help) to represent a much better product than it currently is.  Some of the features (not in any particular priority and not to be considered complete) that still need to addressed are:
•	Add code for multiple databases on same server
•	Finalize and catch all edge conditions not currently accounted for
•	Research extended events to identity “bad” page splits
•	Rewrite code (current code is from proof of concept) – I intend to clean it up in the near future
•	Current fill factor approach not implemented for partitioned indexes (it would really be nice to have fill factor per partition; that way the old data partitions could be set to 100% and the active partitions set as needed {this should be placed on Microsoft’s wish list } ).
•	Try and build in additional features as suggested by the community
•	More testing on performance improvements with firm numbers.
•	Need better way of monitoring Admin.AgentIndexRebuilds – both manually and also self-reviewing.
As I progressed through the data analysis and fill factor determination I realized that I my criteria for perturbing the fill factor also included good page splits.  As discussed above, good page splits are a normal growth pattern of an index where data is added to the end of a table (like with an identity column).  This really has nothing to do with fill factor.  However, bad page splits have everything to do with affecting the fill factor of choice.  The problem is though that you just don’t want to make a small fill factor that has no bad page splits at the expense of increased index size and IOs.  Somewhere during my research I found an article by Jonathan Kehayias at https://www.sqlskills.com/blogs/jonathan/tracking-problematic-pages-splits-in-sql-server-2012-extended-events-no-really-this-time/ that led me to another possible solution for determining fill factor.  This will be discussed in Part 3 to be published later this fall. 
Part 2:
Part 2 will cover a detailed description of the history table and code to implement this solution plus the actual scripts.  It has been very reliable, and I have noticed no performance degradation while the fill factor/defrag SQL Agent job is running.  
Ongoing Work:
I’ve already alluded to redoing this process to look at Bad Page Splits for fill factor perturbations rather than average fragmentation.  This process is already underway, and I hope to have published results in the mid-fall timeframe.  
Conclusion:
This whole project started as a proof of concept and as most POCs has made its way into production.  While I agree that it’s not complete and isn’t perfect, it did result in a substantial decrease in wait time on a real system.  It also demonstrates that effective automatic determination of Fill Factor IS possible.
I look forward to constructive critique and suggestions for improvements from the SQL Server Community.  


