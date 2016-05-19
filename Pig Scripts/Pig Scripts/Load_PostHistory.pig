raw_data = LOAD 'postHistory.csv' USING PigStorage(',') AS (id:chararray,postHistoryTypeId:long,postId:long,revisionGUID:chararray,creationDate:chararray,userId:long);

filter_data = FILTER raw_data BY id is not null;

postHistoryTypeId = FOREACH filter_data GENERATE ($1 IS NOT NULL ? $1 : -1);
postId = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
userId = FOREACH filter_data GENERATE ($5 IS NOT NULL ? $5 : -1);

STORE filter_data INTO 'hbase://posthistory' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('posthistory_data:postHistoryTypeId posthistory_data:postId posthistory_data:revisionGUID posthistory_data:creationDate posthistory_data:userId');