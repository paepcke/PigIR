/* 
   Given one or more WARC file(s), extract the contained Web pages.
   In the current form, it is assumed that the HTML tags have been
   removed earlier. 

   Output is a CSV file of bigram counts:
   	  (word1,word2,count)

  To do the stripping inside this script instead:
     1. Uncomment 
          --ngrams = FOREACH strippedDocs GENERATE FLATTEN(edu.stanford.pigir.pigudf.NGramGenerator(content));
     2.   Comment the line following the above commented line.


   Start this Pig script via the ngrams bash script, like this:
      ngrams [options] <warcSourceFilePathOnHDFS>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME   points to location project root (above target dir)
*/       

-- STORE command for the final output:
%declare NGRAM_STORE_COMMAND "STORE sortedNgrams INTO '$NGRAM_DEST' USING PigStorage(',');";
--%declare NGRAM_STORE_COMMAND "STORE ngramsGreaterOne INTO '$NGRAM_DEST' USING PigStorage(',');";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:bytearray);

docsLenFiltered = FILTER docs BY SIZE(content) < 700000;
--strippedDocs = FOREACH docsLenFiltered GENERATE edu.stanford.pigir.pigudf.StripHTML(content);

/*
   Get this data structure:
   foo,bar
   blue,gray
   foo,bar
   1,4
*/
--ngrams = FOREACH strippedDocs GENERATE FLATTEN(edu.stanford.pigir.pigudf.NGramGenerator(content));
ngrams = FOREACH docsLenFiltered GENERATE FLATTEN(edu.stanford.pigir.pigudf.NGramGenerator(content));

-- Keep only ngrams with alpha chars...no numbers:
ngramsAlphaFiltered = FILTER ngrams by edu.stanford.pigir.pigudf.CSVOnlyLetters($0);

-- Keep only fields shorter than 20 chars. More than that is garbage:
ngramsLenFiltered = FILTER ngramsAlphaFiltered by edu.stanford.pigir.pigudf.CSVMaxLength($0,20);

/*
   Get the following data structure:
   a,step,{(a,step,)}
   a,team,{(a,team,),(a,team,),(a,team,),(a,team,)}
   A DESCRIBE of the following groupedNGrams would return:
      groupedNgrams: groupedNgrams: {group: chararray,
       		     		     ngramsFiltered: {(edu.stanford.pigir.pigudf.ngramgenerator...: chararray)}}
*/
groupedNgrams = GROUP ngramsLenFiltered BY $0;

/*
   Now generate:
   (man,woman,5)
   (this,that,20)
   Where the number is the ngram count.
*/

countedNgrams = FOREACH groupedNgrams GENERATE group AS wordPair:chararray, SIZE(ngramsLenFiltered) AS count:long;

-- Keep only ngrams with counts > 1:
ngramsGreaterOne = FILTER countedNgrams by $1>1;

sortedNgrams  = ORDER ngramsGreaterOne BY wordPair PARALLEL 5;

$NGRAM_STORE_COMMAND;