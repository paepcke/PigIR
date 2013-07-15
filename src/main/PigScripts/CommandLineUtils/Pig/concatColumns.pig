/* 
#resultAlias fused  -- DO NOT MOVE OR DELETE

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


-- Don't know input tuple cardinality, so cannot create input schema:

relation = LOAD '$RELATION_FILE' USING PigStorage(',');

fused = FOREACH relation {
            tupleToFuse = TOTUPLE(*);
	    GENERATE
	      edu.stanford.pigir.pigudf.ConcatColumns('$SLICE_SPEC','$CONCAT_SEPARATOR', tupleToFuse);
	    }

STORE fused INTO '$CONCAT_DEST' USING PigStorage(',');

