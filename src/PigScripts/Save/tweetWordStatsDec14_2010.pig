REGISTER /usr/local/pig-0.8.0-SNAPSHOT/contrib/piggybank/java/piggybank.jar;
REGISTER /home/paepcke/PigScripts/pigUtils.jar;

docs = LOAD '$infile' USING
         org.apache.pig.piggybank.storage.CSVLoader AS 
	    (txt:chararray,
             source:chararray, 
	     dateTime:chararray);

-- Get {(doc1Word1), (doc1Word2), (doc1Word3),...}:
docTokens = FOREACH docs GENERATE
            pigutils.RegexpTokenize(txt,null,1,1) AS wordTuplesBag;

-- Get {(doc1Word1,tf1), (doc1Word2,tf2), ...}
--     {(doc2Word1,tf1), (doc2Word2,tf2), ...}
--[works]docTfs = FOREACH docTokens GENERATE pigutils.TFs(wordTuplesBag);

docTfs = FOREACH docTokens {
          dateAndTime = pigutils.SplitField(dateTime);
          uuid = (chararray) pigutils.GetUUID();
	  GENERATE (chararray) dateAndTime.$0 AS date, 
		   (chararray) dateAndTime.$1 AS time,
		   pigutils.TFs(wordTuplesBag),
		   uuid AS tweetID;
        };

dump docTfs;
DESCRIBE docTfs;



/* ************

texts = FOREACH tweets {
          dateAndTime = pigutils.SplitField(dateTime);
          uuid = (chararray) pigutils.GetUUID();
	  GENERATE txt,
	  	   uuid AS tweetID,
	   	   (chararray) dateAndTime.$0 AS date, 
		   (chararray) dateAndTime.$1 AS time;
        }



-- Tokenize with standard token regexp (null),
-- do eliminate stopwords (the first '1'), and
-- do preserve URLs, rather than tokenizing them
-- into pieces (second 1):
tokens = FOREACH texts GENERATE 
              FLATTEN(pigutils.RegexpTokenize(txt,null,1,1) )
        	      AS (word:chararray), 
              date, 
	      time,
              tweetID;

wordGroups = GROUP tokens BY pigutils.ChopChars(word);
wordCount = FOREACH wordGroups GENERATE 
	        group AS word, 
		(int) COUNT(tokens) as count;

wordStatsJoin = JOIN tokens BY word, wordCount BY word;

wordStats = FOREACH wordStatsJoin GENERATE
	      tokens::word AS word,
	      tokens::date AS date,
	      tokens::time AS time,
	      wordCount::count AS count,
	      tokens::tweetID AS tweetID;

DUMP wordStats;
DESCRIBE wordStats;
***************** */

--wordCount = FOREACH wordGroups GENERATE group, COUNT(tokens) as count;
--tokensSorted = ORDER wordCount BY count DESC;

--STORE tokensSorted into '$outfile' USING BinStorage();

