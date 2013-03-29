/* 
   Given a WARC file or a directory of compressed
   or non-compressed WARC files, create two new archives:
   one containing a sample of the given archive, and another
   of the original archive with the samples removed.
   Write the result to a target directory, 

   Documents longer than 700000 are skipped.

   Start this Pig script via the stripHTML bash script, like this:
      stripHTML [options] <warcSourceFilePathOnHDFS>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    points to location project root (above target dir)
      * $DEST_SAMPLE   destination of sample file
      * $DEST_MAIN     destination of the archive copy with sample removed
      * $WARC_FILE     archive to take sample from
      * $PERCENTAGE    sample size as percentage (may be real number or int)
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',') AS (word:chararray, follower:chararray, followerCount:int);

freqs = LOAD '$FREQS_FILE'
   USING PigStorage(',') AS (freq:int, freqOfFreqs:double);

-- Get word,follower,freq,freq,freqOfFreqs
ngramPlusFreqOfFreqs = JOIN ngrams BY followerCount FULL, freqs BY freq;

-- Project out the FREQS_FILE's 'freq' column, and the NGRAM_FILE's followerCount
-- to be left with word,follower,probability:

ngramPlusProbs = FOREACH ngramPlusFreqOfFreqs GENERATE word,follower,freqOfFreqs;

DUMP ngramPlusFreqOfFreqs;
DUMP ngramPlusProbs;
