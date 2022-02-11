-- load access_logs dataset
A = LOAD'/user/ds503/input/access_logs_nh.csv' USING PigStorage(',') AS (index:long, id:long, bywho:long, whatpage:long, typeofaccess:chararray, accesstime:long);
B = FOREACH A GENERATE whatpage;
C = GROUP B BY (whatpage);
D = FOREACH C GENERATE group, COUNT(B.whatpage) AS count;
out = ORDER D BY count DESC;
limit_out = LIMIT out 8;
-- load my_pages dataset
alpha = LOAD'/user/ds503/input/my_pages_nh.csv' USING PigStorage(',') AS (index:long, id:long, name:chararray, nationality:chararray, countrycode:int, hobby:chararray);
beta = FOREACH alpha GENERATE id, name, nationality;
gamma = JOIN limit_out BY group, beta BY id parallel 20;
join_out = FOREACH gamma GENERATE id, name, nationality;
STORE out INTO '/pig_out/taskB_output.csv' USING PigStorage(',');
