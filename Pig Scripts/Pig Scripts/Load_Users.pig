raw_data = LOAD 'users.csv' USING PigStorage(',') AS (id:long,reputation:long,creationDate:chararray,displayName:chararray,lastAccessDate:chararray,websiteUrl:chararray,views:long,upVotes:long,downVotes:long,accountId:long,age:long,profileImageURL:chararray);

filter_data = FILTER raw_data BY id is not null;

reputation = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1);
views = FOREACH filter_data GENERATE ($8 IS NOT NULL ? $8 : -1);
upVotes = FOREACH filter_data GENERATE ($9 IS NOT NULL ? $9 : -1);
downVotes = FOREACH filter_data GENERATE ($10 IS NOT NULL ? $10 : -1);
accountId = FOREACH filter_data GENERATE ($11 IS NOT NULL ? $11 : -1);

STORE raw_data INTO 'hbase://users' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('users_data:reputation  users_data:creationDate users_data:displayName users_data:lastAccessDate users_data:websiteUrl users_data:views users_data:upVotes users_data:downVotes users_data:accountId users_data:age users_data:profileImageURL');