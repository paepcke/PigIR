REGISTER contrib/piggybank.jar;
REGISTER PigScripts/pigUtils.jar;

docs = LOAD '$infile' USING
         org.apache.pig.piggybank.storage.CSVLoader AS 
	    (txt:chararray,
             source:chararray, 
	     dateTime:chararray);

docTokens = FOREACH docs {
              dateAndTime = pigutils.SplitDateTime(dateTime);
              uuid = (chararray) pigutils.GetUUID();
  	      GENERATE
	         (chararray) dateAndTime.$0 AS date, 
			 uuid AS tweetID;
         }

/*
docTokens = FOREACH docs {
              dateAndTime = pigutils.SplitDateTime(dateTime);
              uuid = (chararray) pigutils.GetUUID();
  	      GENERATE
	         (chararray) dateAndTime.$0 AS date, 
			 (chararray) dateAndTime.$1 AS time,
                 pigutils.TFs(pigutils.RegexpTokenize(txt,null,1,1))
			    AS wordTfs,
		 uuid AS tweetID;
         }
*/
dump docTokens;
DESCRIBE docTokens;
