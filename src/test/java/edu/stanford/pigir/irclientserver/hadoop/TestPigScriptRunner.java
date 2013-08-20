package edu.stanford.pigir.irclientserver.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.IRServConf;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

public class TestPigScriptRunner {
	
	File scriptFileDoStore = new File("src/test/PigScripts/CommandLineUtils/Pig/pigtestStoreResult.pig");
	File scriptFileNoStore = new File("src/test/PigScripts/CommandLineUtils/Pig/pigtestNoStore.pig");
	String resultFile      =          "/tmp/pigtestResult007.txt";
	Map<String,String> params = null;
	
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
	
	
	/**
	 * Test Pig script that does not include a DUMP or STORE call.
	 * For that case client must call store() on the runner.
	 * @throws IOException
	 */
	@Test
	public void testScriptProgrammaticStore() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner(scriptFileNoStore, resultFile, "theCount", params);
		runner.store();
		ensureFileAsExpected();
	}

	@Test
	public void testSimpleScriptIteration() throws IOException {
		PigScriptRunner runner = new PigScriptRunner(scriptFileNoStore, "theCount", params);
		try {
			Iterator<Tuple> resultIt = runner.iterator();
			for (String expectedLine : trueResult) {
				if (! resultIt.hasNext()) {
					fail("Result does not include line '" + expectedLine + "'.");
				}
				String[] wordAndFreq = expectedLine.split("\\s");
				Tuple actualTuple = resultIt.next();
				assertEquals(wordAndFreq[0], actualTuple.get(0));
				assertEquals(wordAndFreq[1], actualTuple.get(1).toString());
			}
		} finally {
			runner.shutDownPigRequest();
		}
		// Success!
		assertTrue(true);
	}
	
	
	@Test
	public void testNonImplementedPigMethod() {
		PigScriptRunner runner = new PigScriptRunner();
		// Non-existent Pig script:
		JobHandle_I jobHandle = runner.asyncPigRequest("foobarbumboodle", null);
		assertEquals(jobHandle.getClass().getName(), "edu.stanford.pigir.irclientserver.ArcspreadException$NotImplementedException");
	}
	
	/**
	 * The following test intentionally calls a non-existing script.
	 * That action makes Pig not even initiate Hadoop activity, so 
	 * PigScriptRunner.getProgress() only learns about the fault by
	 * timing out. The timeout is normally IRServConf.STARTUP_TIME_MAX,
	 * but we set it to 5 secs so that test won't run so long.
	 * @throws InterruptedException
	 */
	@Test
	public void testEarlyDeath() throws InterruptedException {
		PigScriptRunner runner = new PigScriptRunner();
		runner.setScriptRootDir("src/test");
		long startTime = System.currentTimeMillis();		
		JobHandle_I jobHandle = runner.asyncPigRequest("pigtestBadPig", null);

		boolean failureDetected = false;
		long originalTimeout = IRServConf.STARTUP_TIME_MAX;
		try {
			// Set the timeout that's normally large (20sec at time of this writing)
			// to something smaller so test won't take so long:
			IRServConf.STARTUP_TIME_MAX = 5000;
			while (true) {
				long timeNow = System.currentTimeMillis();
				// Have we exceeded the maximum startup time without having gotten
				// a JobStatus.FAILED? The '+ 2000' adds  two seconds to our 
				// fault condition test to give PigScriptRunner.getProgress() a chance
				// to detect the never-started Pig job:
				if ((timeNow - startTime) > IRServConf.STARTUP_TIME_MAX + 2000) {
					fail("PigScriptRunner did not detect launch termination within STARTUP_TIME_MAX msecs");
				}
				jobHandle = runner.getProgress(jobHandle);
				if (jobHandle.getStatus() == JobStatus.FAILED) {
					failureDetected = true;
					break;
				}
				System.out.println(String.format("Awaiting timeout from PigScriptRunner.getProgress() calls %d of %d",
						(timeNow - startTime)/1000, IRServConf.STARTUP_TIME_MAX/1000));
				Thread.sleep(1000);
			}
			assertTrue(failureDetected);
		} finally {
			IRServConf.STARTUP_TIME_MAX = originalTimeout;
		}
	}
	
	@Test
	public void testScriptWithParm() throws IOException, InterruptedException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner();
		params = new HashMap<String,String>();
		params.put("INFILE", "src/test/resources/mary.txt");
		params.put("exectype", "local");
		try {
			runner.setScriptRootDir("src/test");
			JobHandle_I jobHandle = runner.asyncPigRequest("pigtestStoreResultOutfileParm", params);
			while (! new File(resultFile).canRead()) {
				jobHandle = runner.getProgress(jobHandle);
				printPigStatus(jobHandle);
				//***Thread.sleep(3000);
				Thread.sleep(50);
			}
			System.out.println("Output file available; waiting for it to be written.");
			Thread.sleep(2000);
			ensureFileAsExpected();
			jobHandle = runner.getProgress(jobHandle);
			long lastRuntime = jobHandle.getRuntime();
			printPigStatus(jobHandle);
			// Since the job is finished the runtime should no longer
			// change:
			Thread.sleep(1000);
			jobHandle = runner.getProgress(jobHandle);
			assertEquals(lastRuntime, jobHandle.getRuntime());
			assertEquals(100, jobHandle.getProgress());
		} finally {
			runner.shutDownPigRequest();
		}
	}
	
	// ------------------------------------ Utility Methods -----------------------
	
	private void printPigStatus(JobHandle_I jobHandle) {
		System.out.println(String.format("Status: %s; Progress: %d; NumJobsRunning: %d, Runtime: %d, Bytes written: %d; Msg: %s", 
				jobHandle.getStatus(), 
				jobHandle.getProgress(),
				jobHandle.getNumJobsRunning(),
				jobHandle.getRuntime(),
				jobHandle.getBytesWritten(),
				jobHandle.getMessage()));
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
