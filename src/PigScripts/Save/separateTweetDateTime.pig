REGISTER /usr/local/pig-0.8.0-SNAPSHOT/contrib/piggybank/java/piggybank.jar;
REGISTER /home/paepcke/PigScripts/pigUtils.jar;

/*
tweets = LOAD 'Datasets/FDADataSortedCRLFFixed.csv' USING
         org.apache.pig.piggybank.storage.CSVLoader AS (txt:chararray,
         source:chararray, dateTime:chararray);
*/
tweets = LOAD 'Datasets/morTweetsSmall.csv' USING
         org.apache.pig.piggybank.storage.CSVLoader AS (txt:chararray,
         source:chararray, dateTime:chararray);

-- Generate ({(2/14/2010), (11:43)} for each tweet's date/time:
/*
dateTimeTokens = FOREACH tweets GENERATE dateTime, FLATTEN(TOKENIZE(dateTime));
*/
/*
dateTimeTokens = FOREACH tweets 
                 GENERATE dateTime, TOKENIZE(dateTime) 
		 AS date:chararray, time:chararray;
*/

dateTimeTokens = FOREACH tweets {
	         dateAndTimesBag = TOKENIZE(dateTime);
                 GENERATE dateTime, 
			  dateAndTimesBag AS dateAndTimeBag;
                 }
res = FOREACH dateTimeTokens GENERATE
         dateAndTimeBag;		 

dump res;
DESCRIBE res;

--dump dateTimeTokens;
--DESCRIBE dateTimeTokens;

--STORE tokensSorted into '$outfile' USING BinStorage();

/*
wordCount = LOAD 'Datasets/morTweetsSmall' 
	    USING BinStorage() 
	    AS (txt:chararray, count:long);
*/