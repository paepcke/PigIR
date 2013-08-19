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
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.ResultRecipient_I;

public class TestIRClient implements ResultRecipient_I {
	
	static IRClient pigClient = null;
	static ServiceResponsePacket testRespPaket = null;
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
		// Tell PigScriptRunner.java where to look for the 
		// Script files.
		JobHandle_I response = IRLib.setScriptRootDir("src/test/");
		if (response.getStatus() != JobStatus.SUCCEEDED)
			fail("Could not set script root to src/test");
		pigClient = IRClient.getInstance();
	}

	@Test
	public void testScriptInvocationLocalhostServer() throws IOException, InterruptedException {
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServiceConfiguration.IR_SERVER;
		try {
			IRServiceConfiguration.IR_SERVER = "localhost";
			// Script takes no params (null in 2nd parm). Pass this instance
			// as recipients of responses:
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", null, this);
			JobHandle_I jobHandle = resp.getJobHandle();
			if (jobHandle.getStatus() == JobStatus.FAILED)
				fail(String.format("Request for pigtestStoreResult returned FAILED: '%s'", jobHandle.getMessage()));
			
			long startWaitTime = System.currentTimeMillis();
			while (true) {
				if ((System.currentTimeMillis() - startWaitTime) > IRServiceConfiguration.STARTUP_TIME_MAX)
					fail("Did not receive response within IRServiceConfiguration.STARTUP_TIME_MAX");
				jobHandle = IRLib.getProgress(jobHandle);
				if (jobHandle.getStatus() == JobStatus.SUCCEEDED)
					break;
				if (jobHandle.getStatus() == JobStatus.FAILED)
					fail(String.format("Call IRLib.getProgress() returned failure (error code %d): %s", jobHandle.getErrorCode(), jobHandle.getMessage()));
				Thread.sleep(1000);
			}
		} finally {
			IRServiceConfiguration.IR_SERVER = origServerURI;
		}
		ensureFileAsExpected();
	}

	// ------------------------------------ Utility Methods -----------------------

	/**
	 * Called when IRClient receives a response from a request.
	 * @param resp
	 */
	public void resultAvailable(ServiceResponsePacket resp) {
		testRespPaket = resp;
	}
	
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
