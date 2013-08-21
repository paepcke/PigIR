package edu.stanford.pigir.irclientserver.irclient;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;

import edu.stanford.pigir.irclientserver.IRServConf.HADOOP_EXECTYPE;
import edu.stanford.pigir.irclientserver.JobHandle_I;

public class TestIRLib {

	String resultFile = "src/test/resources/tinyWarc1_0.warc_ngrams.csv.gz";
	String groundTruthFile = "src/test/resources/tinyWarcTrigramsGroundTruth.gz";
	
	@Before
	public void setUp() throws Exception {
		IRLib.setExectype(HADOOP_EXECTYPE.LOCAL);
	}

	@Test
	public void testWarcNgrams() throws IOException {
		FileUtils.deleteQuietly(new File(resultFile));
		// Make trigrams from a WARC file:
		JobHandle_I jobHandle = IRLib.warcNgrams(new File("src/test/resources/tinyWarc1_0.warc"), 3);
		System.out.println(jobHandle.getJobName());
		jobHandle = IRLib.getProgress(jobHandle);
		//System.out.println(String.format("Jobname: %s (%s): %s", jobHandle.getJobName(), jobHandle.getStatus(), jobHandle.getMessage()));
		ensureFileAsExpected();
	}

	// ------------------------------------ Utility Methods -----------------------

	private void ensureFileAsExpected() throws IOException {
		Iterator<String> resultIt = null;
		Iterator<String> groundTruthIt = null;
		String actualLine = null;
		String groundTruth = null;
		try {
		resultIt = FileUtils.lineIterator(new File(resultFile + "/part-r-00000"));
		groundTruthIt = FileUtils.lineIterator(new File(groundTruthFile));
		} catch (IOException e) {
			fail(e.getMessage());
		}
		while (groundTruthIt.hasNext()) {
			groundTruth = groundTruthIt.next();
			try {
				actualLine = resultIt.next();
			} catch (NoSuchElementException e) {
				fail("Result has fewer lines than ground truth.");
			}
			assertEquals(groundTruth, actualLine);
		}
		// Check for result having lines beyond the ground truth file:
		try {
			actualLine = resultIt.next();
			fail(String.format("Computed file as at least one additional line: '%s'", actualLine));
		} catch (NoSuchElementException e) {
			return;
		}
	}
}
