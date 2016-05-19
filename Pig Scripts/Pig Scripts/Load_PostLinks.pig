raw_data = LOAD 'postLinks.csv' USING PigStorage(',') AS (id:long,creationDate:chararray,postId:long,relatedPostId:long,linkTypeId:long);

filter_data = FILTER raw_data BY id is not null;

postId = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
relatedPostId = FOREACH filter_data GENERATE ($3 IS NOT NULL ? $3 : -1);
linkTypeId = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1);

STORE filter_data INTO 'hbase://postlinks' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('postlinks_data:creationDate  postlinks_data:postId postlinks_data:relatedPostId postlinks_data:linkTypeId');