REGISTER /usr/local/pig-0.8.0-SNAPSHOT/contrib/piggybank/java/piggybank.jar;
REGISTER /home/paepcke/PigScripts/pigUtils.jar;

tweets = LOAD 'Datasets/morTweetsSmall.csv' USING
         org.apache.pig.piggybank.storage.CSVLoader AS (txt:chararray,
         source:chararray, dateTime:chararray);

texts = FOREACH tweets GENERATE txt;

tokens = FOREACH texts GENERATE FLATTEN(TOKENIZE($0));

tokensNoTrails = FOREACH tokens GENERATE pigutils.ChopChars(token);

dump tokensNoTrails;
