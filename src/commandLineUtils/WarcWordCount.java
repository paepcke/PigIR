package commandLineUtils;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

public class WarcWordCount {
	PigServer pserver;
	Properties props = new Properties();

	public WarcWordCount() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	void execute(String warcFilePath) {
		try {
			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			pserver.registerQuery(
					"docs = LOAD '" + warcFilePath + "' " +
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

			Common.print(pserver, "wordCounts");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		final String usage = "Usage: WarcWordCount <warcFilePath>";
		if (args.length != 1) {
			System.out.println(usage);
			System.exit(-1);
		}
		String filePath = args[0];
		File fileObj = new File(filePath);
		if (!fileObj.exists() ||
			!fileObj.canRead()) {
			System.out.println("File " + filePath + " does not exist, or is not readable.");
			System.exit(-1);
		}
		new WarcWordCount().execute(filePath);
	}
}
