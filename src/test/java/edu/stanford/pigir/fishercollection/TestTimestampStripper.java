package edu.stanford.pigir.fishercollection;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	ArrayList<String> res1 = new ArrayList<String>();
	ArrayList<String> res2 = new ArrayList<String>();
	ArrayList<List<String>> removalResults = new ArrayList<List<String>>(); 
			
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
	
		res1.add("WARC/1.0, WARC_Type: resource");
		res1.add("WARC-Date: 2013-05-25T18:17Z");
		res1.add("WARC_RECORD_ID: file:///sample1.txt");
		res1.add("Content-Type: application/warc");
		res1.add("Content-Length: 100");
		res1.add("");
		res1.add("hello");
		res1.add("hello");
		res1.add("hi how are you");
		res1.add("good");
		res1.add("where are you calling from");
		res1.add("i'm calling from pittsburgh pennsylvania");
		
		res1.add("WARC/1.0");
		res1.add("WARC_Type: resource");
		res1.add("WARC-Date: 2013-05-25T18:17Z");
		res1.add("WARC_RECORD_ID: file:///sample2.txt");
		res1.add("Content-Type: application/warc");
		res1.add("Content-Length: 72");
		res1.add("");
		res1.add("i think it was  gossiping");
		res1.add("gossiping  (( mm ))  probably smoking, smoking");
		
		removalResults.add(res1);
		removalResults.add(res2);
	}

	@Test
	public void test() throws IOException {
		new TimestampStripper(testDir, testOutDir);
		File outDirFile = new File(testOutDir);
		Iterator<File> newOutFiles = FileUtils.iterateFiles(outDirFile, CanReadFileFilter.CAN_READ, null); 
		for (int i=0; i<2; i++) {
			File nextResFile = newOutFiles.next();
			List<String> resultLines = FileUtils.readLines(nextResFile);
			
			assertTrue(Arrays.equals(resultLines.toArray(),removalResults.get(i).toArray()));
			//System.out.println("Result file " + nextResFile.getAbsolutePath() + ":");
			//System.out.println(resultLines);
			
		}
	}
}
