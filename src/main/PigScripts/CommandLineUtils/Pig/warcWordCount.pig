/* 
   Given a WARC file, extract the contained Web pages,
   remove their HTML tags, and remove stopwords. Then
   output a two-column table: (token, wordCount). Output
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
          $WORD_COUNT_DEST  : Full destination path for the word count.
	   		Example: /home/doe/gov-03-2007_wordCount.cnt

   The warcWordCount script constructs these parameters from its
   command line parameters.
      
*/       

-- STORE command for the word count:
%declare WORD_COUNT_STORE_COMMAND "STORE sorted INTO '$WORD_COUNT_DEST' USING PigStorage(',');";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar

docs = LOAD '$WARC_FILE'
		USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:bytearray);

strippedDocs = FOREACH docs GENERATE edu.stanford.pigir.pigudf.StripHTML(content);

/* Tokenize, using default regexp for splitting (the null), eliminiating
   stopwords (first '1' in parameter list), and preserving URLs 
   (second '1' in parms):
*/   
strippedWords = FOREACH strippedDocs GENERATE FLATTEN(edu.stanford.pigir.pigudf.RegexpTokenize(content, null, 1, 1));

strippedGroupedWords = GROUP strippedWords BY $0;

wordCounts = FOREACH strippedGroupedWords GENERATE $0 AS word:chararray ,COUNT($1) AS count:long;
sorted     = ORDER wordCounts BY word PARALLEL 5;

$WORD_COUNT_STORE_COMMAND;

