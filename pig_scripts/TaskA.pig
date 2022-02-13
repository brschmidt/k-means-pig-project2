A = LOAD '/user/ds503/input/my_pages_nh.csv' USING PigStorage(',') AS (index:long, id:long, name:chararray, nationality:chararray, countrycode:int, hobby:chararray);
B = FILTER A BY (nationality == 'United States of America');
D = FOREACH C GENERATE name, hobby;
out = D;
STORE out INTO '/pig_out/taskA_output.csv' USING PigStorage(',');
