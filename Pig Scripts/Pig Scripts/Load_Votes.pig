raw_data = LOAD 'votes.csv' USING PigStorage(',') AS (id:long, postId:long,voteTypeId:long,creationDate:chararray,userId:long);

--dump raw_data; 

filter_data = FILTER raw_data BY id is not null;

postId = FOREACH filter_data GENERATE ($1 IS NOT NULL ? $1 : -1);
voteTypeId = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
userId = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1);

STORE filter_data INTO 'hbase://votes' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('votes_data:postId votes_data:voteTypeId votes_data:creationDate votes_data:userId');