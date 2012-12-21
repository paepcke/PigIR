package edu.stanford.pigir.pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import edu.stanford.pigir.Common;

public class TestIndexOneDocument {
	
	Properties props = new Properties();
	PigServer pserver = null;

	public TestIndexOneDocument() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}
		
	public void doTest0() {
		
		try {

			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");

			pserver.registerQuery(
					"docs = LOAD 'gov-03-2008' " +    // Gary test
					"USING pigir.webbase.WebBaseLoader() " +
					"AS (url:chararray, " +
					"	 date:chararray, " +
					"	 pageSize:int, " +
					"	 position:int, " +
					"	 docidInCrawl:int, " +
					"	 httpHeader:chararray, " +
					"	 content:chararray);"
			);
			Common.print(pserver, "docs");
					
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void doTest1() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);

			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");

			pserver.registerQuery(
					//"docs = LOAD '2006-08:2' " +    // Gary test
					"docs = LOAD 'gov-03-2008:2' " +
					"USING pigir.webbase.WebBaseLoader() " +
					"AS (url:chararray, " +
					"	 date:chararray, " +
					"	 pageSize:int, " +
					"	 position:int, " +
					"	 docidInCrawl:int, " +
					"	 httpHeader:chararray, " +
					"	 content:chararray);"
			);
			//Common.print(pserver, "docs");
			pserver.registerQuery(
					"wordIndexTuple = FOREACH docs GENERATE" +
					"           pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);"
			);

			//pserver.registerQuery(
			//		"STORE wordIndexTuple INTO '/user/paepcke/tmp/index.csv' USING PigStorage(',');"
			//);
			
			/*
			pserver.registerQuery(
					//"wordIndexTuple = LOAD '/user/paepcke/tmp/index.csv' USING PigStorage(',');"
					"wordIndexTuple = LOAD 'c:/users/paepcke/dldev/EclipseWorkspaces/PigIR/Datasets/index.csv' USING PigStorage(',');"
			);
			//Common.print(pserver, "wordIndexTuple");
			*/

			pserver.registerQuery(
					"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten($0);"
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple.$0);" // Scalar has more than one row in the output
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple);"    // Scalars can only be used for projection
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE wordIndexTuple.$0;"            //Scalar has more than one row in the output
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE pigir.pigudf.First(wordIndexTuple);" 
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE " +                               // Scalars can only be used for projection
					//"								pigir.pigudf.First(wordIndexTuple),pigir.pigudf.Rest(wordIndexTuple) ;"
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten($0);" // ({(1000_0))
																					      // ({(1000_1))
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE $0;"          // ({(1000_0))
																						  // ({(1000_1))
					//"flatWordIndexTuple = GROUP wordIndexTuple BY *;"                   // (({(1000_1),(HTML,1000_1,0),(HEAD,1000_1,1),
					//"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE COUNT($0);"     // DataByteArray cannot be cast to org.apache.pig.data.DataBag
			);

			pserver.registerQuery(
					"summaries = FOREACH flatWordIndexTuple GENERATE summary;"
			);
			pserver.dumpSchema("wordIndexTuple");
			Common.print(pserver, "wordIndexTuple");
			//pserver.dumpSchema("wordIndexTuple");
			Common.print(pserver, "flatWordIndexTuple");
			Common.print(pserver, "summaries");
			

/*			
			pserver.registerQuery(
					"flatRawIndex = FOREACH rawIndex GENERATE flatten($0) AS (token:chararray, docID:chararray, tokenPos:int);"
			);

			pserver.registerQuery(
					//"index = ORDER flatRawIndex BY token ASC, docID ASC PARALLEL 5;"
					"index = ORDER flatRawIndex BY token ASC;"
			);

			pserver.registerQuery(
					"STORE index INTO 'Datasets/gov-03-2011-wwwStateGov-30000Pages-index.csv' " +
					//"STORE flatRawIndex INTO 'Datasets/gov-03-2011-wwwStateGov-30000Pages-index.csv' " +
                        "USING PigStorage(',');"
			);	
*/			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void doTests2() {
		try {

			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");

			pserver.registerQuery(
					"flatRawIndex = LOAD 'Datasets/gov-03-2011-wwwStateGov-30000Pages-index.csv'" +
					"USING PigStorage(',')" +
					"AS (token:chararray, docID:chararray, tokenPos:int);"
			);
			
			pserver.registerQuery(
					"index = ORDER flatRawIndex BY token ASC, docID ASC PARALLEL 5;"
			);			
			
			pserver.registerQuery(
					"STORE index INTO 'Datasets/gov-03-2011-indexSorted.csv' " +
                        "USING PigStorage(',');"
			);	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new TestIndexOneDocument().doTest1();
		//new TestIndexOneDocument().doTests2();
	}

}
