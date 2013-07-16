package edu.stanford.pigir.irclientserver.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import org.python.modules.time.Time;

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
	}
	
	@Test
	public void testScriptRunFromStore() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner(scriptFileDoStore, resultFile, "theCount", params);
		try {
			runner.setScriptRootDir("src/test");
			runner.store();
			ensureFileAsExpected();
		} finally {
			runner.shutDownPigRequest();
		}
	}
	
	@Test
	public void testScriptRunViaServicePigRequest() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner();
		try {
			runner.setScriptRootDir("src/test");
			runner.asyncPigRequest("pigtestStoreResult", null); // null: no args
			while (! new File(resultFile).canRead()) {
				Time.sleep(3);
			}
			System.out.println("Output file available; waiting for it to be written.");
			Time.sleep(5);
			ensureFileAsExpected();
		} finally {
			runner.shutDownPigRequest();
		}
	}
	
	@Test
	public void testScriptWithParm() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner();
		params = new HashMap<String,String>();
		params.put("INFILE", "src/test/resources/mary.txt");
		try {
			runner.setScriptRootDir("src/test");
			runner.asyncPigRequest("pigtestStoreResultOutfileParm", params); // null: no args
			while (! new File(resultFile).canRead()) {
				Time.sleep(3);
			}
			System.out.println("Output file available; waiting for it to be written.");
			Time.sleep(5);
			ensureFileAsExpected();
		} finally {
			runner.shutDownPigRequest();
		}
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
