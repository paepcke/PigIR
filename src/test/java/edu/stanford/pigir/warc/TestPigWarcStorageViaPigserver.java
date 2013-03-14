package edu.stanford.pigir.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.Common;

public class TestPigWarcStorageViaPigserver {

	static PigServer pserver;
	static Properties props = new Properties();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		props.setProperty("pig.usenewlogicalplan", "false");
		//pserver = new PigServer(ExecType.MAPREDUCE, props);
		pserver = new PigServer(ExecType.LOCAL, props);
		URI piggybankPath = new File("target/classes/piggybank.jar").toURI();
		pserver.registerJar(piggybankPath.toString());
		String pigirJarPath  = Common.findVersionedFileName("target", "pigir", "jar");
		pserver.registerJar(pigirJarPath);
	}

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testTrueLoadThenStore() throws IOException {
		
		System.out.println("Reading WARC records via Hadoop, then writing them back, and checking result ...please wait...");

		pserver.registerQuery(
				"docs = LOAD '/tmp/test/mixedContent.warc' " +
						"USING edu.stanford.pigir.warc.WarcLoader " +
						"AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
						"optionalHeaderFlds:bytearray, content:bytearray);"
				);
		
		pserver.registerQuery(
				"STORE docs INTO '/tmp/test/testPigWarcStorageResult.warc' " +
						"USING edu.stanford.pigir.warc.PigWarcStorage();"
						);
		
		// Compare the original file with the one that was written.
		// The order of the header fields will differ, but the lengths
		// should match:
		
		long refSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent.warc"));
		long newSize = FileUtils.sizeOf(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		assertEquals(refSize, newSize);
	}
}
