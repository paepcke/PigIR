/* 
   Given a bigram file or a directory of compressed
   or non-compressed ngram files, produce a new file
   of two columns: frequency, and frequency-of-frequency.
   This output is useful for GoodTuring smoothing, and 
   feeds right into the respective program at http://www.grsampson.net/D_SGT.c

   The output file will be a series of lines separated by newline
   characters, where all lines contain two positive integers:
   one of the ngram frequencies of the input file, followed by the 
   frequency of that frequency. Separator is comma.
   The lines will be in ascending order of frequency.

   Note: supposedly, using the code cited above, the first frequency
   	 in the file is to be 1. Not sure that's truly a must. But 
	 this Pig script will not assure this condition.

   Start this Pig script via the ngramFrequencies bash script, like this:
      ngramFrequencies [options] <ngramFile>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    points to location project root (above target dir)
      * $FREQS_DEST    destination of output: directory if source is a directory, else dest file name.
      * NGRAM_FILE     the ngram file or directory to analyze
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',') AS (word:chararray, follower:chararray, followerCount:int);

-- Create intermediate result of the form:
--    freqGroups: {group: int,ngrams: {(word: chararray,follower: chararray,followerCount: int)}}

freqGroups = GROUP ngrams BY followerCount PARALLEL 5;

-- Generate relation of form (freq, freqOfFreq):
freqOfFreqs = FOREACH freqGroups GENERATE group AS freq:int, COUNT(ngrams) as freqOfFreq:int;

-- Order by frequency:
freqOfFreqsOrdered = ORDER freqOfFreqs BY freq ASC;

STORE freqOfFreqsOrdered INTO '$FREQS_DEST' USING PigStorage(',');
