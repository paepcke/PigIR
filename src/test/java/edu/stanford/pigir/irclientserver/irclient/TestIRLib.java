package edu.stanford.pigir.irclientserver.irclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.IRServConf.HADOOP_EXECTYPE;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

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
		// Hang for at most 30 seconds:
		IRLib.waitForResult(jobHandle, 30000);
		if (jobHandle.getStatus() != JobStatus.SUCCEEDED) {
			fail("WARC ngram test failed: %s" + jobHandle.getMessage());
		}
		ensureFileAsExpected();
	}

	// ------------------------------------ Utility Methods -----------------------

	private void ensureFileAsExpected() throws IOException {
		Iterator<String> resultIt = null;
		Iterator<String> groundTruthIt = null;
		String actualLine = null;
		String groundTruth = null;
		File fullResultFile =  new File(resultFile + "/part-r-00000.gz");
		try {
			resultIt = FileUtils.lineIterator(fullResultFile);
		} catch (FileNotFoundException e) {
			fail(String.format("Result file %s is not yet available.", fullResultFile.getAbsoluteFile()));
		} catch (IOException e) {
			fail(e.getMessage());
		}
		
		try {
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
