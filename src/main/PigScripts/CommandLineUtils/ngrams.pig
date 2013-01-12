/* 
   Given one or more WARC file(s), extract the contained Web pages,
   remove their HTML tags. Output is a CSV file of bigram counts:
   	  (word1,word2,count)

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

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:chararray);

strippedDocs = FOREACH docs GENERATE edu.stanford.pigir.pigudf.StripHTML(content);

/*
   Get this data structure:
   foo,bar
   blue,gray
   foo,bar
   1,4
*/
ngrams = FOREACH strippedDocs GENERATE FLATTEN(edu.stanford.pigir.pigudf.NGramGenerator(content));

-- Keep only words with alpha chars...no numbers:
ngramsFiltered = FILTER ngrams by edu.stanford.pigir.pigudf.CSVOnlyLetters($0);

/*
   Get the following data structure:
   a,step,{(a,step,)}
   a,team,{(a,team,),(a,team,),(a,team,),(a,team,)}
   A DESCRIBE of the following groupedNGrams would return:
      groupedNgrams: groupedNgrams: {group: chararray,
       		     		     ngramsFiltered: {(edu.stanford.pigir.pigudf.ngramgenerator...: chararray)}}
*/
groupedNgrams = GROUP ngramsFiltered BY $0;

/*
   Now generate:
   (man,woman,5)
   (this,that,20)
   Where the number is the ngram count.
*/

countedNgrams = FOREACH groupedNgrams GENERATE group AS wordPair:chararray, SIZE(ngramsFiltered) AS count:long;
sortedNgrams  = ORDER countedNgrams BY wordPair PARALLEL 5;

$NGRAM_STORE_COMMAND;
