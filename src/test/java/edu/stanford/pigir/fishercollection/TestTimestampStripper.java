package edu.stanford.pigir.fishercollection;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
	
	String[] res1 = new String[]{
		"WARC/1.0",
		"WARC_Type: resource",
		"WARC-Date: 2013-05-25T18:17Z",
		"WARC_RECORD_ID: file:///sample1.txt",
		"Content-Type: application/warc",
		"Content-Length: 100",
		"",
		"hello",
		"hello",
		"hi how are you",
		"good",
		"where are you calling from",
		"i'm calling from pittsburgh pennsylvania",
	};
	
	String[] res2 = new String[]{
		"WARC/1.0",
		"WARC_Type: resource",
		"WARC-Date: 2013-05-25T18:17Z",
		"WARC_RECORD_ID: file:///sample2.txt",
		"Content-Type: application/warc",
		"Content-Length: 72",
		"",
		"i think it was  gossiping",
		"gossiping  (( mm ))  probably smoking",
		"smoking"
	};
	//ArrayList<String> res2 = new ArrayList<String>();
	ArrayList<String[]> removalResults = new ArrayList<String[]>();
			
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
			String[] expectedStrArr = removalResults.get(i);
			String[] resStrArr = resultLines.toArray(new String[0]);
			// The WARC date field in expected must be adjusted to match
			// the date field of the just-created materials. Else
			// the equality assertion will fail:
			expectedStrArr[2] = resStrArr[2];
			assertArrayEquals(resStrArr,expectedStrArr);
			//System.out.println("Result file " + nextResFile.getAbsolutePath() + ":");
			//System.out.println(resultLines);
			
		}
	}
}
