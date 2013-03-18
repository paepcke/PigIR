package edu.stanford.pigir.pigtests;

import java.io.File;
import java.net.URI;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.Common;

public class TestStripHTML{

	static PigServer pserver;
	static Properties props = new Properties();
	static File tmpFile = null;
	static String tmpFilePath = null;

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
		// Create a tmp file, then delete it. The tests
		// will use that file, and they throw an error
		// if the file already exists. Note that this
		// scheme would fail if multiple processes asked
		// for a tmp file near-simultaneously:
		tmpFile = File.createTempFile("stripHtmlTest", null);
		//tmpFile.deleteOnExit();
		tmpFilePath = tmpFile.getAbsolutePath();
		tmpFile.delete();
	}
	
	@After
	public void cleanup() {
		try {
			tmpFile = new File(tmpFilePath);
			//********tmpFile.delete();
		} catch (Exception e) {
			
		}
	}

	@Test
	public void testStripHTML() {
		try {
			pserver.registerJar("target/classes/piggybank.jar");
			pserver.registerJar("target/pigir.jar");
			
			pserver.registerQuery(
					//"docs = LOAD 'Datasets/ClueWeb09_English_Sample.warc' " +
					"docs = LOAD 'src/test/resources/oneRecord.warc' " +
					"		USING edu.stanford.pigir.warc.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
					"           optionalHeaderFlds:bytearray, content:bytearray);"
			);
			pserver.registerQuery(
					"strippedDocs= FOREACH docs GENERATE edu.stanford.pigir.pigudf.StripHTML(content);\n" +
					"STORE strippedDocs INTO '" + tmpFilePath + 
					"' USING edu.stanford.pigir.warc.PigWarcStorage();");
			
			
			
			// Cut down to 3 tuples for output:
			//pserver.registerQuery("docsCulled = LIMIT strippedDocs 3;");

			//Common.print(pserver, "docsCulled");
			//Common.print(pserver, "strippedDocs");
			
			//pserver.dumpSchema("docs");
			//pserver.dumpSchema("strippedDocs");
			//pserver.dumpSchema("docsCulled");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}