package edu.stanford.pigir.fishercollection;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.CanReadFileFilter;
import org.junit.Before;
import org.junit.Test;

public class TestTimestampStripper {

	String testDir = "src/test/resources/FisherCollDir";
	String testOutDir = "/tmp/FisherCol";
	File tmpOutFile = null;
	
	
			
	@Before
	public void setUp() throws Exception {
		File outDir = new File(testOutDir);
		if (outDir.exists()) {
			Iterator<File> oldOutFiles = FileUtils.iterateFiles(outDir, CanReadFileFilter.CAN_READ, null);
			while (oldOutFiles.hasNext()) {
				oldOutFiles.next().delete();
			}
		} else {
			boolean success = (outDir.mkdirs());
			if (!success) {
				throw new IOException("Could not create temp directory '" + testOutDir + "' for test output.");
			}
		}
	}

	@Test
	public void test() throws IOException {
		new TimestampStripper(testDir, testOutDir);
		File outDirFile = new File(testOutDir);
		Iterator<File> newOutFiles = FileUtils.iterateFiles(outDirFile, CanReadFileFilter.CAN_READ, null); 
		while (newOutFiles.hasNext()) {
			File nextResFile = newOutFiles.next();
			System.out.println("Result file " + nextResFile.getAbsolutePath() + ":");
			List<String> resultLines = FileUtils.readLines(nextResFile);
			System.out.println(resultLines);
		}
	}
}
