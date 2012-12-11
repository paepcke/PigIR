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
    
      * $PIG_HOME points to root of Pig installation
      * $USER_CONTRIB points to location of PigIR.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar
      
*/       

%declare OUTPUT_COMMAND `echo ${OUTPUT_COMMAND}`;
%default OUTPUT_COMMAND 'DUMP wordCounts';

/* $PIG_HOME and $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. Using pigrun to 
   start this script will do that automatically.
*/   
/*REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;*/
REGISTER $PIG_HOME/share/pig/contrib/piggybank/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

docs = LOAD '$srcFile'
		USING pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:chararray);


strippedDocs = FOREACH docs GENERATE pigir.pigudf.StripHTML(content);

/* Tokenize, using default regexp for splitting (the null), eliminiating
   stopwords (first '1' in parameter list), and preserving URLs 
   (second '1' in parms):
*/   
strippedWords = FOREACH strippedDocs GENERATE FLATTEN(pigir.pigudf.RegexpTokenize(content, null, 1, 1));


strippedGroupedWords = GROUP strippedWords BY $0;

wordCounts = FOREACH strippedGroupedWords GENERATE $0,COUNT($1);

/*********/
--DUMP wordCounts
/*********/


$OUTPUT_COMMAND;

