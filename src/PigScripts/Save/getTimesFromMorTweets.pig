tweets = LOAD 'Datasets/morTweetsSmallPercSep.csv' USING PigStorage('%') AS (text,source,date,time);
times = FOREACH tweets GENERATE time AS tweetTime;
DUMP times;
