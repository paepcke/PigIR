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
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',') AS (word:chararray, follower:chararray, followerCount:int);

freqs = LOAD '$FREQS_FILE'
   USING PigStorage(',') AS (freq:int, probability:double);

-- Get word,follower,freq,freq,freqOfFreqs
ngramPlusProbability = JOIN ngrams BY followerCount, freqs BY freq;

-- Project out the FREQS_FILE's 'freq' column, and the NGRAM_FILE's followerCount
-- to be left with word,follower,probability:

ngramPlusProbsOnly = FOREACH ngramPlusProbability GENERATE word, follower, probability;

-- Create: 
--    wordGroups: {group: chararray,ngramPlusProbsOnly: {(ngrams::word: chararray,
--                                                        ngrams::follower: chararray,
--                                                        freqs::probability: double)}}
-- Example: (six,{(six,seven,0.056),(six,apples,0.056),(six,pairs,0.1234)})
--          (one,{(one,brown,2.4E-5),(one,blue,0.056),(one,two,0.1234),(one,red,0.1234),(one,yellow,0.1234)})

wordGroups = GROUP ngramPlusProbsOnly BY word;

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