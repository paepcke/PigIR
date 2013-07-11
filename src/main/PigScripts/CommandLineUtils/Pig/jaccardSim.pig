/* 
   Given a bigram file or a directory of compressed
   or non-compressed relations, create a new relation
   with one column. The column will be the concatenation
   of two or more adjacent (string-typed) columns in the
   original relation. Callers may specify the desired slice, using
   a Pythonic array slice syntax. Callers may also provide a 
   concatenation separator. Syntax:
    
         ConcatColumns(<sliceSpecStr>, {separatorStr | null}, <tuple>)
         
    Examples:
       t = ("foo", "bar", 'fum')
       ConcatColumns('1:2', '', t)  ==> "bar"
       ConcatColumns('0:2', '', t)  ==> "foobar"
       ConcatColumns(':2', '', t)   ==> "foobar"
       ConcatColumns('1:', '', t)   ==> "barfum"
       ConcatColumns('0:-1', '', t)   ==> "foobar"
       ConcatColumns('2:2', '', t)   ==> "fum"

   	  ConcatColumns('0:-1', '|', t)   ==> "foo|bar"

   Start this Pig script via the concatColumns bash script, like this:
      concatColumns [options] <sliceSpec> <concatSeparator> <relationFile>

   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  	 points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    	 points to location project root (above target dir)
      * CONCAT_DEST    	 destination WARC name (directory if source is a directory, else dest file name).
      * SLICE_SPEC       string with specification of the relation tuple slicing
      * CONCAT_SEPARATOR separator string to insert between concatenated columns
      * RELATION_FILE 	 the ngram file or directory to strip
*/       

REGISTER $PIGIR_HOME/target/pigir.jar;


set1 = LOAD '$SET1' USING PigStorage(',') AS (jaccEl:bytearray);
set2 = LOAD '$SET2' USING PigStorage(',') AS (jaccEl:bytearray);

set1Group = GROUP set1 ALL;
set2Group = GROUP set2 ALL;

set1Cardinality = FOREACH set1Group GENERATE COUNT_STAR(set1);
set2Cardinality = FOREACH set2Group GENERATE COUNT_STAR(set2);

setsJoined = JOIN set1 BY jaccEl, set2 BY jaccEl;

DUMP setsJoined;
DESCRIBE setsJoined;

--STORE fused INTO '$CONCAT_DEST' USING PigStorage(',');
