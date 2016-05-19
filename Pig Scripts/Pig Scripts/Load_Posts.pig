raw_data = LOAD 'posts.csv' USING PigStorage(',') AS (id:long,postTypeId:long,acceptedAnswerId:long,creationDate:chararray,score:long,viewCount:long,ownerUserId:long,lastEditorUserId:long,lastEditDate:chararray,lastActivityDate:chararray,title:chararray,tags:chararray,answerCount:long,commentCount:long,favoriteCount:long,parentId:chararray,communityOwnedDate:chararray,closedDate:chararray,ownerDisplayName:chararray,lastEditorDisplayName:chararray);

filter_data = FILTER raw_data BY id is not null; 

-- To dump the data from PIG Storage to stdout --dump raw_data;

postTypeId = FOREACH filter_data GENERATE ($1 IS NOT NULL ? $1 : -1); acceptedAnswerId = FOREACH filter_data GENERATE ($2 IS NOT NULL ? $2 : -1); score = FOREACH filter_data GENERATE ($4 IS NOT NULL ? $4 : -1); viewCount = FOREACH filter_data GENERATE ($5 IS NOT NULL ? $5 : -1); ownerUserId = FOREACH filter_data GENERATE ($6 IS NOT NULL ? $6 : -1); lastEditorUserId = FOREACH filter_data GENERATE ($7 IS NOT NULL ? $7 : -1); answerCount = FOREACH filter_data GENERATE ($12 IS NOT NULL ? $12 : -1); commentCount = FOREACH filter_data GENERATE ($13 IS NOT NULL ? $13 : -1); favoriteCount = FOREACH filter_data GENERATE ($14 IS NOT NULL ? $14 : -1);

--REGISTER ../lib/piggybank.jar; --DEFINE A org.apache.pig.piggybank.evaluation.string.RegexExtractAll(); category = FOREACH filter_data GENERATE $0 as id,$11 as line; 

tags_data = FOREACH tags GENERATE FLATTEN(REGEX_EXTRACT_ALL(line, '([^>]+)')); 

--dump category; 

tags = FOREACH category GENERATE $0 as tagid,FLATTEN(STRSPLIT(line,'>')); 

--dump tags; 

store tags into 'output';

--cat 'output/part-m-00000' >> 'output/output.txt' --cat 'output/part-m-00001' >> 'output/output.txt' /* 

p_id = LOAD 'output/output.txt' USING PigStorage('\t') AS (tag_id:long,tag_val1:chararray,tag_val2:chararray,tag_val3:chararray,tag_val4:chararray,tag_val5:chararray); --dump p_id;
tag_val1 = FOREACH p_id GENERATE $1; tag_val1_1 = FILTER tag_val1 By $0 is not null;
tag_val2 = FOREACH p_id GENERATE $2; tag_val2_1 = FILTER tag_val2 By $0 is not null;
tag_val3 = FOREACH p_id GENERATE $3; tag_val3_1 = FILTER tag_val3 By $0 is not null;
tag_val4 = FOREACH p_id GENERATE $4; tag_val4_1 = FILTER tag_val4 By $0 is not null;
tag_val5 = FOREACH p_id GENERATE $5; tag_val5_1 = FILTER tag_val5 By $0 is not null;
-- Use HBase storage handler to map data from PIG to HBase --NOTE: In this case, custno (first unique column) will be considered as row key. */ 
STORE filter_data INTO 'hbase://posts' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('posts_data:postTypeId posts_data:acceptedAnswerId posts_data:creationDatepost_tags_eachdata posts_data:score posts_data:viewCount posts_data:ownerUserId posts_data:lastEditorUserId posts_data:lastEditDate posts_data:lastActivityDate posts_data:title posts_data:tags posts_data:answerCount posts_data:commentCount posts_data:favoriteCount posts_data:parentId posts_data:communityOwnedDate posts_data:closedDate posts_data:ownerDisplayName posts_data:lastEditorDisplayName');