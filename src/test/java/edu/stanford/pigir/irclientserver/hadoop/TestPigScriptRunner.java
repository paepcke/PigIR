package edu.stanford.pigir.irclientserver.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;

public class TestPigScriptRunner {
	
	File scriptFileDoStore = new File("src/test/resources/pigtestStoreResult.pig");
	File scriptFileNoStore = new File("src/test/resources/pigtestNoStore.pig");
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
	
	
	@Test
	public void testScriptProgrammaticStore() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner(scriptFileNoStore, resultFile, "theCount");
		runner.store();
		ensureFileAsExpected();
	}

	@Test
	public void testSimpleScriptIteration() throws IOException {
		PigScriptRunner runner = new PigScriptRunner(scriptFileNoStore, "theCount");
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
			runner.shutdown();
		}
	}
	
	@Test
	public void testScriptRun() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		PigScriptRunner runner = new PigScriptRunner(scriptFileDoStore, "theCount");
		try {
			PigScriptRunner.setPackageRootDir("src/test");
			runner.servicePigRequest("pigtestStoreResult", null);
			//runner.run();
			ensureFileAsExpected();
		} finally {
			runner.shutdown();
		}
	}
	
	private void ensureFileAsExpected() throws IOException {
		Iterator<String> resultIt = FileUtils.lineIterator(new File(resultFile + "/part-r-00000"));
		for (String expectedLine : trueResult) {
			if (! resultIt.hasNext()) {
				fail("Result does not include line '" + expectedLine + "'.");
			}
			String actualLine = resultIt.next();
			assertEquals(expectedLine, actualLine);
		}
	}
}
