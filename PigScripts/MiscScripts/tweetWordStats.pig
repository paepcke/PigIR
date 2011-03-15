SET pig.usenewlogicalplan 'false';
--REGISTER contrib/piggybank/java/piggybank.jar;
REGISTER contrib/piggybank.jar;
--REGISTER PigScripts/pigUtils.jar;
REGISTER contrib/pigUtils.jar;
--docs = LOAD 'Datasets/morTweetsSmall.csv' USING 
docs = LOAD '$infile' USING 
		       org.apache.pig.piggybank.storage.CSVLoader AS
			   (txt:chararray,
			    source:chararray, 
		       	    dateTime:chararray);

/*			
Create data with the following schema: 
  {date: chararray,time: chararray,
   wordTfs: {(word: chararray,tf: double)},
   tweetID: chararray}
*/
docTokens = FOREACH docs {
	          dateAndTime = pigutils.SplitDateTime(dateTime);
	          uuid = (chararray) pigutils.GetUUID();
	          GENERATE
	            (chararray) dateAndTime.date AS date,
	            (chararray) dateAndTime.time AS time,
	            pigutils.TFs(pigutils.RegexpTokenize(
	                       txt,
     			       null, -- Use default regex for splitting
	      		       1,    -- Preserve URLs as whole tokens
	      		       1)),  -- Kill stopwords
	            uuid AS tweetID;
	          };

theWordTfs  = FOREACH docTokens GENERATE tweetID, wordTfs;
docIdPlusWordTfsBag = GROUP theWordTfs all;
tfidfs = FOREACH docIdPlusWordTfsBag GENERATE FLATTEN(pigutils.TfIdf(theWordTfs));
--tfidfsByWord = ORDER tfidfs BY word;

dump tfidfs;
--dump tfidfsByWord;
--DESCRIBE tfidfs;

--STORE tfidfsByWord INTO 'Datasets/FDATweetsTfIdfs';

