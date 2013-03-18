/* 
   Given a WARC file  or a directory of compressed
   or non-compressed WARC files, extract the contained Web pages, and
   remove their HTML tags. Write the result to a target directory, 

   Documents longer than 700000 are skipped.

   Start this Pig script via the warcStripHTML bash script, like this:
      warcStripHTML [options] <warcSourceFilePathOnHDFS>
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
      * $WARC_FILE     the WARC file or directory to strip
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:bytearray);

docsLenFiltered = FILTER docs BY SIZE(content) < 700000;

strippedWarc = FOREACH docsLenFiltered {
	          stripped = edu.stanford.pigir.pigudf.StripHTML(content) AS
		  		  strippedContent:bytearray, newContentLength:int;
		  GENERATE
		     warcRecordId,newContentLength,date,warc_type,
 		     optionalHeaderFlds, strippedContent;
	       }

STORE strippedWarc INTO '$STRIPPED_DEST' USING edu.stanford.pigir.warc.PigWarcStorage();

