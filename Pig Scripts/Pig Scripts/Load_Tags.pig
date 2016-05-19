raw_data = LOAD 'tags.csv' USING PigStorage(',') AS (id:long,tagname:chararray,count:long,excerptpostid:long,wikipostid:long);

--dump raw_data; 

filter_data = FILTER raw_data BY id is not null;

count = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
excerptpostid = FOREACH filter_data GENERATE ($3 IS NOT NULL ? $3 : -1);
wikipostid = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1);

STORE filter_data INTO 'hbase://tags' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(' tags_data:tagname tags_data:count tags_data:excerptpostid tags_data:wikipostid');