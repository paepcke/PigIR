/* 
   Given:
   1. An ngrams file of format: word,follower,followerCount
   2. A GoodTuring result file of format: frequency,probability

   output: word,follower,probability

   Where probability is the smoothed version of the input.

   Documents longer than 700000 are skipped.

   Start this Pig script via the smoothNgrams, like this:
      smoothNgrams [options] <ngramSourceFile> <freqProbabilitiesFile>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  	   points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    	   points to location project root (above target dir)
      * DEST_SMOOTH_NGRAMS Where to put the result
      * NGRAM_FILE     	   HDFS location of ngrams
      * FREQS_FILE     	   HDFS location of frequency file
      * LIMIT 	       	   -1 if all output requested, else k of top-k probably for each root.

For testing:

smoothNgrams Datasets/trigramsForMixWithGoodTuring.csv Datasets/goodTuringForMixWithTrigrams.csv

*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;


-- The following file will be AS (ngramCount:int, word1, word2, word3,...).
-- We'd like to declare the field names and types, but we don't know the ngram arity:
ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',');

-- Get the Good-Turing smoothed frequency/probability file:
freqs = LOAD '$FREQS_FILE'
   USING PigStorage(',') AS (freq:int, probability:double);

-- Get freq,freqOfFreqs,freq,word1,word2,word3,...
-- (ngrams.$0 is the ngram count):
-- Example for result:
--     (2,0.056,2,ABATE,feels,that)
--     (2,0.056,2,Teapot,Scandal,He)
--     (44,3.8E-4,44,Teapot,Tea,in)
--     (67,3.5E-4,67,Teapot,Tea,Blog)
--     (82,4.0E-4,82,Teapot,Tea,Fight)
--     (577,2.5E-4,577,Teapot,Tea,Library)

ngramPlusProbability = JOIN freqs BY freq, ngrams BY $0;

-- Project out the FREQS_FILE's 'freq' column, and the NGRAM_FILE's frequency count
-- to be left with probability,word1,word2,word3,...:

ngramPlusProbsOnly = FOREACH ngramPlusProbability GENERATE 
		     	     $1 AS probability:double,
			     $3 AS rootWord:chararray,
			     $4..; -- all the remaining words in the ngram

-- Create: 
-- ****this schema example is out of date (but the code works):
--    wordGroups: {group: chararray,ngramPlusProbsOnly: {(ngrams::rootWord: chararray,
--                                                        ngrams::follower: chararray,
--                                                        freqs::probability: double)}}
-- Example: (six,{(six,seven,0.056),(six,apples,0.056),(six,pairs,0.1234)})
--          (one,{(one,brown,2.4E-5),(one,blue,0.056),(one,two,0.1234),(one,red,0.1234),(one,yellow,0.1234)})
-- $1 is the rootWord of a row's ngram:

wordGroups = GROUP ngramPlusProbsOnly BY $1;

-- Isolate the bags of tuples (see Example above).
-- Each bag contains all ngrams with one particular word
-- as the starting word:
ngramBags = FOREACH wordGroups GENERATE ngramPlusProbsOnly;

-- Within each bag of Ngrams with the same start word,
-- find the tuples with the top-n probability fields.
-- If no 'n' was specified in the command line ($LIMIT==-1),
-- all ngrams are retained:
topKNgrams = FOREACH ngramBags {
                       result = ($LIMIT == -1 ? $0 : TOP($LIMIT, 2, $0));
		       GENERATE FLATTEN(result);
             }
	        
STORE topKNgrams INTO '$DEST_SMOOTH_NGRAMS' USING PigStorage(',');