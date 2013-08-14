package edu.stanford.pigir.irclientserver.irclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

public class TestIRClient {
	
	static IRClient pigClient = null;
	String resultFile      =          "/tmp/pigtestResult007.txt";
	String[] trueResult = new String[] {
			"a	1",
			"as	1",
			"to	1",
			"and	1",
			"go.	1",
			"had	1",
			"its	1",
			"the	1",
			"was	2",
			"Mary	2",
			"lamb	2",
			"snow	1",
			"sure	1",
			"that	1",
			"went	1",
			"white	1",
			"fleece	1",
			"little	1",
			"everywhere	1"
	};
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		pigClient = new IRClient();
		// Tell PigScriptRunner.java where to look for the 
		// Script files.
		@SuppressWarnings("unused")
		ServiceResponsePacket response = pigClient.setScriptRootDir("src/test/");
	}

	@Test
	public void test() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		pigClient.sendProcessRequest("pigtestStoreResult", null);
		ensureFileAsExpected(); 
	}

	// ------------------------------------ Utility Methods -----------------------

	private void ensureFileAsExpected() throws IOException {
		Iterator<String> resultIt = null;		
		try {
		resultIt = FileUtils.lineIterator(new File(resultFile + "/part-r-00000"));
		} catch (IOException e) {
			fail(e.getMessage());
		}
		for (String expectedLine : trueResult) {
			if (! resultIt.hasNext()) {
				fail("Result does not include line '" + expectedLine + "'.");
			}
			String actualLine = resultIt.next();
			assertEquals(expectedLine, actualLine);
		}
	}
}
