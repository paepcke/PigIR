package edu.stanford.pigir.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKeepWarcIf {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

	@Test
	public void testFiltering() throws IOException, InterruptedException {
		
		System.out.println("Reading WARC records via Hadoop, then writing a filtered subsets back ...please wait...");
		
		String[] cmd = new String[1];
		// The Pig script is driven by a shell script that we
		// now invoke:
		cmd[0] = "src/test/PigScripts/testWarcFilter";
		Process proc = null;
		try {
            proc = Runtime.getRuntime().exec(cmd);
            proc.waitFor();
        } catch (IOException e) {
            fail("Could not run the shell test script: " + e.getMessage());
        } catch (InterruptedException e) {
        	fail("Interrupt during run of test shell script: " + e.getMessage());
		}

		// Check the result files against what we know they
		// should be:
		long oneRecordRemaining = 10l;
		long oneRecordFilteredOut = 11l;
		long sizeOneRecordRemaining = FileUtils.sizeOf(new File("/tmp/test/.....gz/part-m-00000"));
		long sizeOneRecordFilteredOut = FileUtils.sizeOf(new File("/tmp/test/.....gz/part-m-00000"));
		assertEquals(refSize, newSize);
		
		//long csumOrigFile = FileUtils.checksumCRC32(new File("/tmp/test/mixedContent.warc"));
		//long csumNewFile  = FileUtils.checksumCRC32(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		//assertEquals(csumOrigFile, csumNewFile);
	}
	
	
}
