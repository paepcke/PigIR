/* 
   Given one or more WARC file(s), extract the contained Web pages,
   remove their HTML tags, and remove stopwords. Then
   *****output a two-column table: (token, wordCount). Output
   either to an HDFS file, or to stdout.

   It is convenient to start this script via warcWordCount,
   or---less conveniently---via pigrun. warcWordCount calls
   pigrun.

   Running via warcWordCount:
   
      warcWordCount <srcFile> [<target>]
   
   where <target> defaults to stdout, but may be an HDFS target file. 
   SrcFile is also on HDFS.

   Examples:
   
      warcWordCount /user/me/foo.warc
      warcWordCount /user/me/foo.warc /user/me/fooCounts warcWordCount.pig

   
   Running via pigrun:
    
      srcFile=<warcFile> [target=<destFile>]
      
   where <target> defaults to stdout.
   Examples:
   
      pigrun srcFile=/user/me/foo.warc warcWordCount.pig
      pigrun srcFile=/user/me/foo.warc target=/user/me/fooCounts warcWordCount.pig
      
   Environment assumptions (all taken care of by pigrun, if 
    you initialized it):
    
      * $USER_CONTRIB points to location of piggybank.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar
      * $PIGIR_HOME   points to location project root (above target dir)
      
   $USER_CONTRIB is assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by warcWordCount takes care of this. Additionally,
   the following env vars must be passed in via -param:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
          $NGRAM_DEST  : Full destination path for the word count.
	   		Example: /home/doe/gov-03-2007_wordCount.cnt

   The warcWordCount script constructs these parameters from its
   command line parameters.
      
*/       

-- STORE command for the word count:
%declare NGRAM_STORE_COMMAND "STORE countedNgrams INTO '$NGRAM_DEST' USING PigStorage(',');";

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

/*
Get the following data structure:
a,step,{(a,step,)}
a,team,{(a,team,),(a,team,),(a,team,),(a,team,)}

*/
groupedNgrams = GROUP ngrams BY $0;

/*
Generate:
(man,woman,5)
(this,that,20)
Where the number is the ngram count.
*/

/* A DESCRIBE of groupedNGrams would return:
groupedNgrams: {group: chararray,ngrams: {(edu.stanford.pigir.pigudf.ngramgenerator_content_4::ngram: chararray)}}
*/
--DESCRIBE groupedNgrams;

--countedNGrams = FOREACH groupedNgrams GENERATE $0 AS word:chararray, $1 AS word:chararray, COUNT($3) AS count:long;
countedNgrams = FOREACH groupedNgrams GENERATE group AS wordPair:chararray, SIZE(ngrams) AS count:long;

--ngrams = FOREACH strippedGroupedWords GENERATE $0 AS word:chararray ,COUNT($1) AS count:long;
--sorted     = ORDER wordCounts BY word PARALLEL 5;

$NGRAM_STORE_COMMAND;
