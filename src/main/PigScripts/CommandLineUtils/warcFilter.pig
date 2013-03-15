/* 
   Given a WARC file  or a directory of compressed
   or non-compressed WARC files, extract the contained Web pages.
   Write a new version of the WARC files to a target directory.
   The new version contains only those WARC records for which
   a given WARC field matches a given regular expression.

   Regular expressions should use Java regex syntax.

   Documents longer than 700000 are skipped.

   Start this Pig script via the warcFilter bash script, like this:
      warcFilter [options] <warcSourceFilePathOnHDFS> <warcFieldName> <regex>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    points to location project root (above target dir)
      * $FILTERED_DEST destination WARC name (directory if source is a directory, else dest file name).
      * $WARC_FILE     the WARC file or directory to strip
      * $WARC_FIELD    name of the WARC field to check against the regex
      * $REGEX         the Java regular expression.
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bag{fldNameVal:tuple(fldName:chararray,fldVal:chararray)}, content:bytearray);

docsLenFiltered = FILTER docs BY SIZE(content) < 700000;

extended = FOREACH docsLenFiltered GENERATE
	     warcRecordId,contentLength,date,warc_type,optionalHeaderFlds,content,'$WARC_FIELD','$REGEX';

keepers = FILTER extended BY edu.stanford.pigir.pigudf.KeepWarcIf(*);

--DUMP keepers;

STORE keepers INTO '$FILTERED_DEST' USING edu.stanford.pigir.warc.PigWarcStorage();

