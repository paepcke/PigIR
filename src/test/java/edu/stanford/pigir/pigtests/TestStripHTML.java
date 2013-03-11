package edu.stanford.pigir.pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import edu.stanford.pigir.Common;

class TestStripHTML {

	PigServer pserver;
	Properties props = new Properties();

	public TestStripHTML() {
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
					//"docs = LOAD 'Datasets/ClueWeb09_English_Sample.warc' " +
					"docs = LOAD 'resources/Datasets/ClueWeb09_English_Sample.warc' " +
					"		USING edu.stanford.pigir.warc.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
					"           optionalHeaderFlds:bytearray, content:bytearray);"
			);
			pserver.registerQuery(
					"strippedDocs = FOREACH docs GENERATE pigir.pigudf.StripHTML(content);");
			// Cut down to 3 tuples for output:
			pserver.registerQuery("docsCulled = LIMIT strippedDocs 3;");

			Common.print(pserver, "docsCulled");
			
			pserver.dumpSchema("docs");
			pserver.dumpSchema("strippedDocs");
			//pserver.dumpSchema("docsCulled");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new TestStripHTML().doTests();
	}
}