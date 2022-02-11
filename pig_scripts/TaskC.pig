A = LOAD '/user/ds503/input/my_pages_nh.csv' USING PigStorage(',') AS (index:long, id:long, name:chararray, nationality:chararray, countrycode:int, hobby:chararray);
B = FOREACH A GENERATE id, countrycode;
C = GROUP B BY countrycode;
D = FOREACH C GENERATE group, COUNT(B.id) AS count;
out = ORDER D BY count DESC;
STORE out INTO '/pig_out/taskC_output.csv' USING PigStorage(',');
