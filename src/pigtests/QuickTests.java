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
					"docs = LOAD 'gov-03-2008:5' " +
					"USING pigir.webbase.WebBaseLoader() " +
					"AS (url:chararray, " +
					"date:chararray, " +
					"pageSize:int, " +
					"position:int, " +
					"docidInCrawl:int, " +
					"httpHeader:chararray, " +
					"content:chararray);"
			);
					
			pserver.registerQuery(			
					"index = FOREACH docs GENERATE " + 
					"pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);"
			);

			//Common.print(pserver, "docs");
			//Common.print(pserver, "index");
			pserver.registerQuery(
					"STORE index INTO 'tmp/fivePages.csv' " +
					"USING org.apache.pig.piggybank.storage.CSVExcelStorage();"
			);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//new QuickTests().doTests();
		new QuickTests().doTests1();
	}
 }