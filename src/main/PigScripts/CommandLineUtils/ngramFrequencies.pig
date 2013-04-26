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
      * $NGRAM_FILE    the ngram file or directory to analyze
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

-- We cannot define a schema, because we don't know the arity
-- of the ngrams. We do know that the first field is an ngram
-- occurrence count.

ngrams = LOAD '$NGRAM_FILE'
   USING PigStorage(',');

-- Create intermediate result, one tuple for each ngram occurrence count:
-- 	  freqGroups: {group: int,ngrams: {()}}
-- From:
-- 	3,one,two,three
-- 	2,one,blue,red
-- 	3,one,yellow,blue
-- 	1,one,red,pink
-- 	3,four,five,six
-- 	2,seven,eight,nine
-- 	2,six,apples,pairs
-- 	3,six,pairs,apples
-- 	3,ten,eleven,thirteen
-- 	2,fourteen,fifteen,fifteen

-- (2,{(2,seven,eight,nine),
--     (2,one,blue,red),
--     (2,fourteen,fifteen,fifteen),
--     (2,six,apples,pairs)})
-- (3,{(3,four,five,six),
--     (3,one,yellow,blue),
--     (3,six,pairs,apples),
--     (3,ten,eleven,thirteen),
--     (3,one,two,three)})
-- (1,{(1,one,red,pink)})

freqGroups = GROUP ngrams BY (int)$0 PARALLEL 5;

-- Generate relation of form (freq, freqOfFreq);
-- get freqOfFreqsOrdered: {freq: int,freqOfFreq: int}

freqOfFreqs = FOREACH freqGroups GENERATE group AS freq:int, COUNT(ngrams) as freqOfFreq:int;

-- Order by frequency:
freqOfFreqsOrdered = ORDER freqOfFreqs BY freq ASC;

STORE freqOfFreqsOrdered INTO '$FREQS_DEST' USING PigStorage(',');
