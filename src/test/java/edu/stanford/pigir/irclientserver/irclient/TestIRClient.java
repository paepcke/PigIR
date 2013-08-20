package edu.stanford.pigir.irclientserver.irclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.pig.PigRunner;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServConf;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.ResultRecipient_I;

public class TestIRClient implements ResultRecipient_I {
	
	static IRClient pigClient = null;
	static ServiceResponsePacket testRespPaket = null;
	static ServiceResponsePacket pushResultReceived = null; 
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
	@Ignore
	public void testScriptInvocationLocalhostServer() throws IOException, InterruptedException {
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServConf.IR_SERVER;
		try {
			IRServConf.IR_SERVER = "localhost";
			@SuppressWarnings("serial")
			Map<String,String> params = new HashMap<String,String>() {{
				put("exectype", "local");
			}};
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", params, this);
			JobHandle_I jobHandle = resp.getJobHandle();
			if (jobHandle.getStatus() == JobStatus.FAILED)
				fail(String.format("Request for pigtestStoreResult returned FAILED: '%s'", jobHandle.getMessage()));
			
			long startWaitTime = System.currentTimeMillis();
			while (true) {
				if ((System.currentTimeMillis() - startWaitTime) > IRServConf.STARTUP_TIME_MAX)
					fail("Did not receive response within IRServConf.STARTUP_TIME_MAX");
				jobHandle = IRLib.getProgress(jobHandle);
				if (jobHandle.getStatus() == JobStatus.SUCCEEDED)
					break;
				if (jobHandle.getStatus() == JobStatus.FAILED)
					fail(String.format("Call IRLib.getProgress() returned failure (error code %d): %s", jobHandle.getErrorCode(), jobHandle.getMessage()));
				Thread.sleep(1000);
			}
		} finally {
			IRServConf.IR_SERVER = origServerURI;
		}
		ensureFileAsExpected();
	}

	@Test
	@Ignore
	public void testBadExecType() throws IOException, InterruptedException {
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServConf.IR_SERVER;
		try {
			IRServConf.IR_SERVER = "localhost";
			@SuppressWarnings("serial")
			Map<String,String> params = new HashMap<String,String>() {{
				put("exectype", "foobar");
			}};
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", params, this);
			JobHandle_I jobHandle = resp.getJobHandle();
			while(true) {
				jobHandle = IRLib.getProgress(jobHandle);
				assertEquals(PigRunner.ReturnCode.ILLEGAL_ARGS, jobHandle.getErrorCode());
				return;
			}
		} finally {
			IRServConf.IR_SERVER = origServerURI;
		}
	}

	@Test
	@Ignore
	public void testResultPushing() throws InterruptedException {
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServConf.IR_SERVER;
		try {
			IRServConf.IR_SERVER = "localhost";
			@SuppressWarnings("serial")
			Map<String,String> params = new HashMap<String,String>() {{
				put("exectype", "local");
			}};
			testRespPaket = null;
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", params, this, Disposition.NOTIFY);
			JobHandle_I jobHandle = resp.getJobHandle();
			long startWaitTime = System.currentTimeMillis();
			while(testRespPaket == null) {
				if ((System.currentTimeMillis() - startWaitTime) > IRServConf.STARTUP_TIME_MAX) {
					jobHandle.setStatus(JobStatus.FAILED);
					jobHandle.setMessage(String.format("Job '%s' did not either fail or succeed within timeout of %d seconds", jobHandle.getJobName(),IRServConf.STARTUP_TIME_MAX/1000));
					fail("Push response did not arrive soon enough.");
				}
				Thread.sleep(1000);
			}
		} catch (IOException e) {
			fail("IOException in sendProcessRequest: " + e.getMessage());
		} finally {
			IRServConf.IR_SERVER = origServerURI;
		}
	}
	
	@Test
	@Ignore
	public void testResultQueuing() throws InterruptedException{
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServConf.IR_SERVER;
		try {
			IRServConf.IR_SERVER = "localhost";
			@SuppressWarnings("serial")
			Map<String,String> params = new HashMap<String,String>() {{
				put("exectype", "local");
			}};
			testRespPaket = null;
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", params, Disposition.QUEUE_RESULTS);
			JobHandle_I jobHandle = resp.getJobHandle();
			long startWaitTime = System.currentTimeMillis();
			ConcurrentLinkedQueue<JobHandle_I> responseQueue = pigClient.getResponseQueueByType("GENERIC");
			JobHandle_I respJobHandle = null;
			while(true) {
				if ((System.currentTimeMillis() - startWaitTime) > IRServConf.STARTUP_TIME_MAX) {
					jobHandle.setStatus(JobStatus.FAILED);
					jobHandle.setMessage(String.format("Job '%s' did not either fail or succeed within timeout of %d seconds", jobHandle.getJobName(),IRServConf.STARTUP_TIME_MAX/1000));
					fail("Push response did not arrive soon enough.");
				}
				respJobHandle = responseQueue.poll();
				if (respJobHandle != null) {
					assertEquals(JobStatus.SUCCEEDED, respJobHandle.getStatus());
					return;
				}
				else
					Thread.sleep(1000);
			}
		} catch (IOException e) {
			fail("IOException in sendProcessRequest: " + e.getMessage());
		} finally {
			IRServConf.IR_SERVER = origServerURI;
		}
	}

	@Test
	@Ignore
	public void testResultCustomQueuing() throws InterruptedException{
		FileUtils.deleteQuietly(new File(resultFile));
		String origServerURI = IRServConf.IR_SERVER;
		try {
			IRServConf.IR_SERVER = "localhost";
			@SuppressWarnings("serial")
			Map<String,String> params = new HashMap<String,String>() {{
				put("exectype", "local");
			}};
			testRespPaket = null;
			String myQueueName = "myQueue";
			ServiceResponsePacket resp = pigClient.sendProcessRequest("pigtestStoreResult", params, Disposition.QUEUE_RESULTS, myQueueName);
			JobHandle_I jobHandle = resp.getJobHandle();
			long startWaitTime = System.currentTimeMillis();
			ConcurrentLinkedQueue<JobHandle_I> responseQueue = pigClient.getResponseQueueByType(myQueueName);
			JobHandle_I respJobHandle = null;
			while(true) {
				if ((System.currentTimeMillis() - startWaitTime) > IRServConf.STARTUP_TIME_MAX) {
					jobHandle.setStatus(JobStatus.FAILED);
					jobHandle.setMessage(String.format("Job '%s' did not either fail or succeed within timeout of %d seconds", jobHandle.getJobName(),IRServConf.STARTUP_TIME_MAX/1000));
					fail("Push response did not arrive soon enough.");
				}
				respJobHandle = responseQueue.poll();
				if (respJobHandle != null) {
					assertEquals(JobStatus.SUCCEEDED, respJobHandle.getStatus());
					return;
				}
				else
					Thread.sleep(1000);
			}
		} catch (IOException e) {
			fail("IOException in sendProcessRequest: " + e.getMessage());
		} finally {
			IRServConf.IR_SERVER = origServerURI;
		}
	}
	
	public void testNgramFrequencies() {
		
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
