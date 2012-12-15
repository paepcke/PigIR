package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

public class TestWordCount {
	PigServer pserver;
	Properties props = new Properties();

	public TestWordCount() {
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
					"docs = LOAD 'resources/Datasets/ClueWeb09_English_Sample.warc' " +
					"		USING pigir.warc.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
					"           optionalHeaderFlds:bytearray, content:chararray);"
			);
			pserver.registerQuery(
					"strippedDocs = FOREACH docs GENERATE pigir.pigudf.StripHTML(content);");
			
			// Tokenize, using default regexp for splitting (the null), eliminiating
			// stopwords (first 1 in parameter list), and preserving URLs (second 1 in parms):
			pserver.registerQuery(
					"strippedWords = FOREACH strippedDocs GENERATE " +
					                    "FLATTEN(pigir.pigudf.RegexpTokenize(content, null, 1, 1));"
			);

			pserver.registerQuery(
					"strippedGroupedWords = GROUP strippedWords BY $0;"
			);
			
			pserver.registerQuery(
					"wordCounts = FOREACH strippedGroupedWords GENERATE " +
					"$0,COUNT($1);"
			);
			// Cut down to 3 tuples for output:
			//pserver.registerQuery("docsCulled = LIMIT strippedGrouped 3;");

			//Common.print(pserver, "docsCulled");
			//Common.print(pserver, "strippedWords");
			//Common.print(pserver, "strippedGroupedWords");
			Common.print(pserver, "wordCounts");
			
			//pserver.dumpSchema("docs");
			//pserver.dumpSchema("strippedWords");
			//pserver.dumpSchema("strippedGrouped");
			pserver.dumpSchema("wordCounts");

			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new TestWordCount().doTests();
	}
}
