-- load friends dataset
A = LOAD '/user/ds503/input/friends_nh.csv' USING PigStorage(',') AS (index:long, id:long, personid:long, myfriend:long, dateoffriendship:long, description:chararray);
B = FOREACH A GENERATE personid, myfriend;
C = GROUP B BY myfriend;
D = FOREACH C GENERATE group, (long)COUNT(B.personid) AS count;
-- load my_pages dataset
alpha = LOAD '/user/ds503/input/my_pages_nh.csv' USING PigStorage(',') AS (index:long, id:long, name:chararray, nationality:chararray, countrycode:int, hobby:chararray);
beta = FOREACH alpha GENERATE id, name;
-- full join count of myfriend occurnces (D) with column subset of my_pages dataset (beta) to ensure anyone not listed as anyone's friend in friends dataset is accounted for
gamma = JOIN D BY group FULL, beta BY id;
-- remove duplicate records from full join
E = DISTINCT gamma;
-- fill any null counts with 0
F = FOREACH E GENERATE name, (long)(count is null ? 0: count) AS friend_count;
-- sort counts in descending order
out = ORDER F BY friend_count DESC;
STORE out INTO '/pig_out/taskD_output.csv' USING PigStorage(',');
