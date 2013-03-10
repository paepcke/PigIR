/* 
   Given one or more WARC file(s) (including a directory of compressed
   or non-compressed WARC files), extract the contained Web pages, and
   remove their HTML tags. Write the result to a target directory, 

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
      * $STRIPPED_HTML destination WARC name (directory if source is a directory, else dest file name).
*/       

-- STORE command for the final output:
%declare STRIPPED_STORE_COMMAND "STORE strippedWarc INTO '$STRIPPED_DEST' USING PigWarcStorage();";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:chararray);

docsLenFiltered = FILTER docs BY SIZE(content) < 700000;
strippedWarc = FOREACH docsLenFiltered GENERATE edu.stanford.pigir.pigudf.StripHTML(content);


$STRIPPED_STORE_COMMAND;
