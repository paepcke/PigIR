REGISTER /usr/local/pig-0.8.0-SNAPSHOT/contrib/piggybank/java/piggybank.jar;
REGISTER /home/paepcke/PigScripts/pigUtils.jar;

tweets = LOAD 'Datasets/morTweetsSmall.csv' USING
         org.apache.pig.piggybank.storage.CSVLoader AS (txt:chararray,
         source:chararray, dateTime:chararray);

sourceDateTime = FOREACH tweets {
	          dateAndTime = pigutils.SplitField(dateTime);
		  GENERATE source, 
		  	   (chararray) dateAndTime.$0 AS date, 
			   (chararray) dateAndTime.$1 AS time;
		}

DESCRIBE sourceDateTime;
dump sourceDateTime;
