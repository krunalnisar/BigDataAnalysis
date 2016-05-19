raw_data = LOAD 'badges.csv' USING PigStorage(',') AS (id:long,userid:long,name:chararray,date:chararray,class:long,tagbased:chararray);

filter_data = FILTER raw_data BY id is not null;

userid = FOREACH filter_data GENERATE ($1 IS NOT NULL ? $1 : -1);
class = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1);

--dump raw_data; 

STORE filter_data INTO 'hbase://badges' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('badges_data:userid badges_data:name badges_data:date badges_data:class badges_data:tagbased');