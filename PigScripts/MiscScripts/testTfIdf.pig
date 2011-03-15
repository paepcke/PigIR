SET	pig.usenewlogicalplan 'false';
REGISTER contrib/piggybank.jar;
REGISTER contrib/pigUtils.jar;

docs = LOAD 'Datasets/morTweetsSmall.csv' USING 
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
      pigutils.TFs(pigutils.RegexpTokenize(txt,
  										   null, -- Use default regex for splitting
  										   1,    -- Preserve URLs as whole tokens
  										   1))   -- Kill stopwords.
  								  AS tokenTfs,
     uuid 						  AS tweetID;
   };
			
-- Get ({(w1,tf1),(w2,tf2)}, {(w2,tf3), (w3,tf4)}, ...):
theTokenTfs  = FOREACH docTokens GENERATE tweetID, FLATTEN(tokenTfs) AS (token,tf);
theTokenGroups = GROUP theTokenTfs BY token;  
theTokenTfAndDfs = FOREACH theTokenGroups {
								  df = COUNT(theTokenTfs);
								  GENERATE
										FLATTEN(theTokenTfs) AS (docId,token,tf),
										df AS df;
							   };
theTokenTfAndIdfs = FOREACH theTokenTfAndDfs {
						uniqueDocIds = DISTINCT  docId;
						collSize = COUNT(uniqueDocIds);
						--thetfIdf = tf * org.apache.pig.piggybank.evaluation.math.LOG10(collSize/(1.0 + df);
						--thetfIdf = tf * pigutils.log10(collSize/(1.0 + df);
						thetfIdf = tf * pigutils.log10(4);
						GENERATE docId,token,thetfIdf;
						};
		    
DUMP theTokenTfs;
DUMP theTokenGroups;
DUMP theTokenTfAndDfs;
DUMP theTokenTfAndIdfs;
			
DESCRIBE theTokenTfs
DESCRIBE theTokenGroups;
DESCRIBE theTokenTfAndDfs;
DESCRIBE theTokenTfAndIdfs;
