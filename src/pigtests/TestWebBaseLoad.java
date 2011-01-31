package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

class TestWebBaseLoad {

	PigServer pserver;
	Properties props = new Properties();

	public TestWebBaseLoad() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	void doTests() {
		try {
			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			pserver.registerQuery(
					"docs = LOAD 'crawled_hosts.gov-12-2009.tx:text' " +
					"		USING pigir.webbase.WebBaseLoader" +
					"       AS (url:chararray, date:chararray, position:int, docid:int," +	// 
					"           optionalHeaderFlds:bytearray, content:chararray);"
			);
			pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,date;");
			//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,content;");
			//Common.print(pserver, "docs");
			Common.print(pserver, "docsCulled");
			pserver.dumpSchema("docs");
			//pserver.dumpSchema("docsCulled");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new TestWebBaseLoad().doTests();
	}
}