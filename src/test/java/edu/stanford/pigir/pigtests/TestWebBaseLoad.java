package edu.stanford.pigir.pigtests;

import java.io.File;
import java.net.URI;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import edu.stanford.pigir.Common;

class TestWebBaseLoad {

	PigServer pserver;
	Properties props = new Properties();

	public TestWebBaseLoad() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	void doTests() {
		try {
			URI piggybankPath = new File("target/classes/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			String pigirJarPath  = Common.findVersionedFileName("target", "pigir", "jar");
			pserver.registerJar(pigirJarPath);
			
			pserver.registerQuery(
					//"docs = LOAD '2003-06:1' " +
					//"docs = LOAD '2003-06-tx:1:www.hp.com' " +
					//"docs = LOAD '2003-06:2:www.hp.com:www.ssa.gov' " +
					//"docs = LOAD '2005-08:2' " +
					//"docs = LOAD '2006-04:2' " +
					//"docs = LOAD '2006-05:2' " +
					// Get five pages:
					"docs = LOAD '04-2009:5' " +
					"		USING edu.stanford.pigir.webbase.WebBaseLoader() " +
					"       AS (url:chararray, date:chararray, pageSize:int, position:int, docidInCrawl:int, httpHeader:chararray, content:chararray);"
					);
			pserver.registerQuery(
					"stripped = FOREACH docs {" +
   	                      "stripped = edu.stanford.pigir.pigudf.StripHTML(content); " +
		                  "GENERATE " +
		                  "stripped.$1, stripped.$0;" +
		                  "}"
					);
			
			//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,date;");
			//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,content;");
			//Common.print(pserver, "docs");
			Common.print(pserver, "stripped");
			//Common.print(pserver, "docsCulled");
			pserver.dumpSchema("docs");
			//pserver.dumpSchema("docsCulled");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pserver.shutdown();
		}
	}

	public static void main(String[] args) {
		new TestWebBaseLoad().doTests();
	}
}