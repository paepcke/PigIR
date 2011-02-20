REGISTER /usr/local/pig-0.8.0-SNAPSHOT/contrib/piggybank/java/piggybank.jar;
REGISTER /home/paepcke/PigScripts/pigUtils.jar;

tweets = LOAD '$infile' USING
         org.apache.pig.piggybank.storage.CSVLoader AS (txt:chararray,
         source:chararray, dateTime:chararray);
texts = FOREACH tweets GENERATE txt;
tokens = FOREACH texts GENERATE FLATTEN(TOKENIZE($0));
wordGroups = GROUP tokens BY pigutils.ChopChars(token);
wordCount = FOREACH wordGroups GENERATE group, COUNT(tokens) as count;
tokensSorted = ORDER wordCount BY count DESC;

STORE tokensSorted into '$outfile' USING BinStorage();

/*
wordCount = LOAD 'Datasets/morTweetsSmall' 
	    USING BinStorage() 
	    AS (txt:chararray, count:long);
*/