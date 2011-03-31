package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

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
			pserver = new PigServer(ExecType.MAPREDUCE, props);
			//pserver = new PigServer(ExecType.LOCAL, props);

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

			pserver.registerQuery(
					"flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple.$0);"
			);
			Common.print(pserver, "flatWordIndexTuple");

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
