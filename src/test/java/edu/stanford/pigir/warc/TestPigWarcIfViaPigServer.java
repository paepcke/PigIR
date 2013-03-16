package edu.stanford.pigir.warc;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import edu.stanford.pigir.Common;

public class TestPigWarcIfViaPigServer {

	static PigServer pserver;
	static Properties props = new Properties();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
			URI piggybankPath = new File("target/classes/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			String pigirJarPath  = Common.findVersionedFileName("target", "pigir", "jar");
			pserver.registerJar(pigirJarPath);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	// The following test tests the same as
	// TestWarcFilter.java. The latter calls a shell script
	// do drive the Pig warcFilter.pig script. This method
	// instead uses PigServer. It was only used for debugging
	// the unit test. Use TestWarcFilter.java.
	@Test
	@Ignore
	public void test() throws IOException {
		pserver.registerQuery(
				"docs = LOAD 'src/test/resources/mixedContent.warc' " +
						"		USING edu.stanford.pigir.warc.WarcLoader" +
						"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
						"           optionalHeaderFlds:bytearray, content:bytearray);"
				);
		pserver.registerQuery(
				"docsLenFiltered = FILTER docs BY SIZE(content) < 700000; " 
				);
		pserver.registerQuery(
                "extended = FOREACH docsLenFiltered GENERATE " +
                "     warcRecordId,contentLength,date,warc_type,optionalHeaderFlds,content,'content','(?s)(?!.*frankbeecostume).*';"
                );
		
		pserver.registerQuery(
                "keepers = FILTER extended BY edu.stanford.pigir.pigudf.KeepWarcIf(*);"
				);

		pserver.registerQuery(
                "finalKeepers = FOREACH keepers GENERATE" +
                "     warcRecordId,contentLength,date,warc_type,optionalHeaderFlds,content;"                			
				);
		
		
		//pserver.registerQuery("DUMP keepers;");
		pserver.registerQuery("STORE finalKeepers INTO '/tmp/test/foo.warc' USING edu.stanford.pigir.warc.PigWarcStorage();");
	}

}
