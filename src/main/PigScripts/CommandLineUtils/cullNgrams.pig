/* 
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
      * $USER_CONTRIB  points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    points to location project root (above target dir)
      * CULLED_DEST    destination WARC name (directory if source is a directory, else dest file name).
      * NGRAM_FILE     the ngram file or directory to strip
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

ngrams = LOAD '$NGRAM_FILE'
       USING USING PigStorage() AS (word:chararray, follower:chararray, followerCount:int);

ngramsNoStopWords = FILTER ngrams BY (edu.stanford.pigir.pigudf.IsStopword(word)) OR
		    	   	     (edu.stanford.pigir.pigudf.IsStopword(follower)) OR
				     (SIZE(word) < 2) OR
				     (SIZE(follower) < 2);

STORE ngramsNoStopWords INTO '$CULLED_DEST' USING PigStorage();

