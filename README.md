Use Map Reduce to build/search on Lucene index

Map Reduce Job to build lucene index
https://github.com/henryzhao81/Lucene-MapReduce/tree/master/src/main/java/com/openx/audience/xdi/lucence/mapred

Reducer for query lucene index
https://github.com/henryzhao81/Lucene-MapReduce/blob/master/src/main/java/com/openx/audience/xdi/xdiReport/XdiReportReducer.java

Standalone lucene index builder
https://github.com/henryzhao81/Lucene-MapReduce/tree/master/src/main/java/com/openx/audience/xdi/lucene

Why Lucene:

We have large volume of data (serveral Terabytes) which generated hourly, and other data which is relatively small less than 1T and generated everyday. Those data need to join and generate to HFile load into HBase for service everyday. 

The daily data for previous day will be ready at 4am, and join with large dataset, we want to load to database as quick as possible, ideally less than one hour. But the job to join those 2 dataset usually cause more than 3 ~ 4 hours.

To Make process faster, due the large volume of data was genereted hourly and  field 




