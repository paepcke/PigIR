package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWarcFilter {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testWarcFilter() throws IOException, InterruptedException {
		
		System.out.println("Reading WARC records via Hadoop, then writing some of them back, and checking result ...please wait...");
		
		String[] cmd = new String[1];
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
		// Compare the original file with the one that was written.
		// The order of the header fields will differ, but the lengths
		// should match:
		
		long refSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent.warc"));
		long newOnlyOneSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent_onlyOne.gz/part-m-00000"));
		long newNoSomethingSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent_notSomething.gz/part-m-00000"));
		assertEquals(660, newOnlyOneSize);
		assertEquals(5244, newNoSomethingSize);
		
		//long csumOrigFile = FileUtils.checksumCRC32(new File("/tmp/test/mixedContent.warc"));
		//long csumNewFile  = FileUtils.checksumCRC32(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		//assertEquals(csumOrigFile, csumNewFile);
	}
	
	
}
