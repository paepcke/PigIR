/* 
#resultAlias ngramsNoStopWords -- DO NOT MOVE OR DELETE

   Given a bigram file or a directory of compressed
   or non-compressed ngram files, filter the file(s)
   such that stopwords are removed from both ngram
   columns. Also, ngrams with constituent words of
   length less than two are removed.

   Start this Pig script via the cullNgrams bash script, like this:
      cullNgrams [options] <ngramFile>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  	 points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    	 points to location project root (above target dir)
      * CULLED_DEST    	 destination WARC name (directory if source is a directory, else dest file name).
      * NGRAM_FILE     	 the ngram file or directory to strip
      * FILTER_STOPWORDS 0 if stopwords are to remain in the ngrams, else another value
      * WORD_LEN_MIN     minimum char length of words in each ngram
      * WORD_LEN_MAX     maxmimum char length of words in each ngram
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',') AS (word:chararray, follower:chararray, followerCount:int);

-- IsStopword(word) returns True if word is *not* a stopword (unfortunate choice of mine):
ngramsNoStopWords = FILTER ngrams BY (edu.stanford.pigir.pigudf.IsStopword(word)) AND
		    	   	     (edu.stanford.pigir.pigudf.IsStopword(follower)) AND
				     (SIZE(word) >= 2) AND
				     (SIZE(follower) >= 2);
STORE ngramsNoStopWords INTO '$CULLED_DEST' USING PigStorage(',');

