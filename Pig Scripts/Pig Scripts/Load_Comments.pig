raw_data = LOAD 'comments.csv' USING PigStorage(',') AS (id:long,postId:long,score:long,creationDate:chararray,userId:long);

filter_data = FILTER raw_data BY id is not null;

postId = FOREACH filter_data GENERATE ($1 IS NOT NULL ? $1 : -1);
score = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
userId = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1);

STORE filter_data INTO 'hbase://comments' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('comments_data:postId comments_data:score comments_data:creationDate comments_data:userId');