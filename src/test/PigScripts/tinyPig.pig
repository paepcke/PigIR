A = LOAD 'tinyData.csv' USING PigStorage(',') AS (c1:chararray, c2:chararray, c3:chararray);
STORE A INTO 'tinyDataOut.csv';
