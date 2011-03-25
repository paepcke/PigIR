package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

class QuickTests {
	PigServer pserver;
	Properties props = new Properties();

	public QuickTests() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}
	
	public void doTests() {
		try {
			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			pserver.registerQuery(
					"docs = LOAD 'Datasets/usColleges.csv' " +
					"		USING org.apache.pig.piggybank.storage.CSVExcelStorage() " +
					"       AS (abbrev:chararray, name:chararray, country:chararray, state:chararray," +
					"           city:chararray);"
			);
			pserver.registerQuery(
					"luid = FOREACH docs GENERATE pigir.pigudf.GetLUID();");
			pserver.dumpSchema("docs");
			Common.print(pserver, "luid");
	
		} catch (Exception e) {
			
		}
	}

	void doTests1() {
		try {
			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");

			pserver.registerQuery(
					"docs = LOAD 'gov-03-2008:11' " +
					"USING pigir.webbase.WebBaseLoader() " +
					"AS (url:chararray, " +
					"	 date:chararray, " +
					"	 pageSize:int, " +
					"	 position:int, " +
					"	 docidInCrawl:int, " +
					"	 httpHeader:chararray, " +
					"	 content:chararray);"
			);
 			pserver.registerQuery(
					"rawIndex = FOREACH docs GENERATE " + 
					"pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);"
			);

			pserver.registerQuery(
					"flatRawIndex = FOREACH rawIndex GENERATE flatten($0);"
			);
			
			pserver.dumpSchema("rawIndex");
			pserver.dumpSchema("flatRawIndex");
			
			pserver.registerQuery(
					//"index = ORDER flatRawIndex BY token ASC, docID ASC PARALLEL 5;" 
					//"index = ORDER flatRawIndex BY token ASC, docID ASC;" 
					//"index = ORDER flatRawIndex BY postingsInDoc::token;"
					"index = ORDER flatRawIndex BY $0;"
			);

			Common.print(pserver, "docs");
			//Common.print(pserver, "index");
			/*
			pserver.registerQuery(
					//"STORE index INTO 'tmp/fivePages.csv' " +
					//"STORE flatRawIndex INTO 'tmp/fivePages.csv' " +
					//"USING PigStorage(',');"
					//"USING org.apache.pig.piggybank.storage.CSVExcelStorage();"
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
					"A = LOAD 'tmp/fivePages.csv' " +
					"		USING PigStorage(',') " +
					"       AS (token:chararray, docID:chararray, tokenPos:int);"
			);
			
			pserver.registerQuery(
					"B = ORDER A BY $0;"
			);
			
			Common.print(pserver, "B");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void doTests3() {
		int[] theCharInts = new int[] {
				37, 33, 80, 83, 45, 65, 100, 111, 98, 101, 45, 50, 46, 48, 10, 37, 37, 67, 114, 101, 
				97, 116, 111, 114, 58, 32, 100, 118, 105, 112, 115, 40, 107, 41, 32, 53, 46, 56, 54, 
				32, 67, 111, 112, 121, 114, 105, 103, 104, 116, 32, 49, 57, 57, 57, 32, 82, 97, 100, 
				105, 99, 97, 108, 32, 69, 121, 101, 32, 83, 111, 102, 116, 119, 97, 114, 101, 10, 37, 
				37, 84, 105, 116, 108, 101, 58, 32, 97, 114, 88, 105, 118, 58, 97, 115, 116, 114, 111, 
				45, 112, 104, 47, 48, 48, 48, 53, 49, 50, 51, 32, 118, 51, 32, 32, 32, 50, 32, 79, 99, 
				116, 32, 50, 48, 48, 48, 10, 37, 37, 80, 97, 103, 101, 115, 58, 32, 55, 10, 37, 37, 80, 
				97, 103, 101, 79, 114, 100, 101, 114, 58, 32, 65, 115
		};
		char[] theChars = new char[theCharInts.length];
		int i=0;
		for (int oneCharInt : theCharInts) {
			theChars[i] = (char) oneCharInt;
			i++;
		}
		String res = new String(theChars);
		System.out.println("Res: '" + res + "'");
	}
	
	public static void main(String[] args) {
		//new QuickTests().doTests();
		new QuickTests().doTests1();
		//new QuickTests().doTests2();
		//new QuickTests().doTests3();
	}
 }