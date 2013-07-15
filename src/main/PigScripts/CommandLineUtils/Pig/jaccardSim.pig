/* 
#resultAlias similarity  -- DO NOT MOVE OR DELETE

   Given two one-column relations, find their Jaccard similarity.
   The relations may contain duplicates, but the number of duplicates
   are not used for weighting.
    
         jaccardSim <file1> <file2>
         
   Start this Pig script via the jaccardSim bash script, like this:
      jaccardSim [options] <file1> <file2>

   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  	 points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    	 points to location project root (above target dir)
      * DEST    	 destination WARC name (directory if source is a directory, else dest file name).
      * BAG1		 the file path to the relation1. Local if -x local was specified, else on HDFS
      * BAG2		 the file path to the relation2. Local if -x local was specified, else on HDFS

   For the running example in the comments below, assumeing the following
   two relations:
      BAG1:         BAG2:
	 foo           foo
	 foo           fum
	 bar
	 fum
	 fum
	 fum

   We use the modified Jaccard simlarity form:

               |A cut B|
	 ---------------------
         |A| + |B| - |A cut B|

*/

REGISTER $PIGIR_HOME/target/pigir.jar;


bag1 = LOAD '$BAG1' USING PigStorage(',') AS (jaccEl:bytearray);
bag2 = LOAD '$BAG2' USING PigStorage(',') AS (jaccEl:bytearray);

-- Get:
--   {group: chararray, bag1:{(foo),(foo),(bar),(fum),(fum),(fum)},bag2:{(foo),(fum)}}

bag1Bag2Group = COGROUP bag1 ALL, bag2 ALL;

-- Turn bags into sets, and get (|Set1| + |Set2|), 
-- as well as two bags, one with the Set1 tuples, the
-- other with the Set2 tuples (three columns total):
--    5, {(bar),(foo),(fum)}, {(foo),(fum)}
--
-- Schema:  {sumCards: long,set1: {(jaccEl: bytearray)},set2: {(jaccEl: bytearray)}}

cardSumAndSets = FOREACH bag1Bag2Group {
		   set1 = DISTINCT bag1;
		   set2 = DISTINCT bag2;
		   cardSet1 = COUNT_STAR(set1);
		   cardSet2 = COUNT_STAR(set2);
		   cardSet1PlusCardSet2 = cardSet1 + cardSet2;
		   GENERATE
		   	  cardSet1PlusCardSet2 AS sumCards, set1 AS set1, set2 AS set2;
	};

-- Get bar
--     foo
--     fum
--   Schema: theSet1: {set1::jaccEl: bytearray}
theSet1 = FOREACH cardSumAndSets GENERATE FLATTEN(set1);

-- Get foo
--     fum
--   Schema: theSet2: {set2::jaccEl: bytearray}
theSet2 = FOREACH cardSumAndSets GENERATE FLATTEN(set2);

-- Get foo,foo
--     fum,fum
setsJoined = JOIN theSet1 BY jaccEl, theSet2 BY jaccEl; 

-- Get all,{(foo,foo),(fum,fum)}
--   Schema: setsJoinedGrp: {group: chararray,setsJoined: {(theSet1::set1::jaccEl: bytearray,theSet2::set2::jaccEl: bytearray)}}
setsJoinedGrp = GROUP setsJoined ALL;

similarity = FOREACH cardSumAndSets {
	       cardCutSet1Set2 = COUNT_STAR(setsJoinedGrp.setsJoined);
	       jaccSim = (double)cardCutSet1Set2 / ((double)sumCards - (double)cardCutSet1Set2);
	       GENERATE jaccSim;
	       }

-- DUMP similarity;
STORE similarity INTO '$DEST' USING PigStorage(',');
