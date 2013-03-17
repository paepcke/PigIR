package edu.stanford.pigir.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.KeepWarcIf;

public class TestKeepWarcIf {

	TupleFactory tfactory = TupleFactory.getInstance();
	Tuple minTuple = null;
	Tuple oneOptionalHeaderFldTuple = null;
	Tuple fullTuple = null;
	
	@Before
	public void setUp() throws Exception {
		// Min record has: WARC_RECORD_ID, CONTENT_LENGTH, WARC_DATE, WARC_TYPE:
		minTuple             = makeStringTuple("rec1", "10", "jan-1-1970", "response");
		
		oneOptionalHeaderFldTuple = makeStringTuple("rec2", "10", "jan-1-1970", "response");
		Tuple targetURIHeader      		= makeStringTuple("warc-target-uri", "http://good.luck");
		oneOptionalHeaderFldTuple.append(makeBag(targetURIHeader));
		
		fullTuple = makeStringTuple("rec3", "10", "jan-1-1970", "response");
		Tuple warcTruncatedFld = makeStringTuple("warc-truncated", "yes");
		Tuple warcFilenameFld  = makeStringTuple("warc-filename", "/foo/bar.warc");
		Tuple warcTargetFld    = makeStringTuple("warc-target-urn", "http://foo.dmoz.org/bar");
		Tuple warcTargetFld1   = makeStringTuple("warc-target-urn1", "http://foo.bar.org/fum.txt");
		fullTuple.append(makeBag(warcTruncatedFld, warcFilenameFld, warcTargetFld, warcTargetFld1));
		fullTuple.append(new DataByteArray("This is the content.".getBytes()));
	}

	private Tuple makeStringTuple(String... vals) {
		ArrayList<String> tupleFlds = new ArrayList<String>();
		for (String str : vals) 
			tupleFlds.add(str);
		return tfactory.newTuple(tupleFlds); 
	}
	
	private Tuple makeCondition (Tuple t, String warcHeaderFldName, String regex) {
		ArrayList<Object> tupleFlds = new ArrayList<Object>();
		for (Object el : t.getAll())
			tupleFlds.add(el);
		tupleFlds.add(warcHeaderFldName);
		tupleFlds.add(regex);
		return tfactory.newTuple(tupleFlds);
	}
	
	private DataBag makeBag(Tuple...tuples) {
		DefaultDataBag bag = new DefaultDataBag();
		for (Tuple t : tuples)
			bag.add(t);
		return bag;
	}
	
	@Test
	public void testMakePigWarcRecord() throws IOException {
		PigWarcRecord rec = new PigWarcRecord(minTuple);
		assertEquals("rec1", rec.get(PigWarcRecord.WARC_RECORD_ID));
		assertEquals("10", rec.get(PigWarcRecord.CONTENT_LENGTH));
		assertEquals("jan-1-1970", rec.get(PigWarcRecord.WARC_DATE));
		assertEquals("response", rec.get(PigWarcRecord.WARC_TYPE));
		
		rec = new PigWarcRecord(oneOptionalHeaderFldTuple);
		assertEquals("rec2", rec.get(PigWarcRecord.WARC_RECORD_ID));
		assertEquals("10", rec.get(PigWarcRecord.CONTENT_LENGTH));
		assertEquals("jan-1-1970", rec.get(PigWarcRecord.WARC_DATE));
		assertEquals("response", rec.get(PigWarcRecord.WARC_TYPE));
		assertEquals("http://good.luck", rec.get(PigWarcRecord.WARC_TARGET_URI));

		rec = new PigWarcRecord(fullTuple);
		assertEquals("rec3", rec.get(PigWarcRecord.WARC_RECORD_ID));
		assertEquals("10", rec.get(PigWarcRecord.CONTENT_LENGTH));
		assertEquals("jan-1-1970", rec.get(PigWarcRecord.WARC_DATE));
		assertEquals("response", rec.get(PigWarcRecord.WARC_TYPE));
		assertEquals( "yes", rec.get(PigWarcRecord.WARC_TRUNCATED));
		assertEquals("/foo/bar.warc", rec.get(PigWarcRecord.WARC_FILENAME));
		assertEquals("This is the content.", rec.get(PigWarcRecord.CONTENT));
		assertNotEquals("This is the content", rec.get(PigWarcRecord.CONTENT));
		assertEquals(null, rec.get(PigWarcRecord.WARC_TARGET_URI));
	}

	@Test
	public void testKeepIf() throws IOException {
		KeepWarcIf filterUDF = new KeepWarcIf();
		
		Tuple cond1 = makeCondition(minTuple, PigWarcRecord.WARC_RECORD_ID, "^rec1");
		assertTrue(filterUDF.exec(cond1));
		
		Tuple cond2 = makeCondition(minTuple, PigWarcRecord.WARC_RECORD_ID, "^rec2");
		assertFalse(filterUDF.exec(cond2));
		
		Tuple cond3 = makeCondition(fullTuple, PigWarcRecord.CONTENT, "[^xyz]*the.*");
		assertTrue(filterUDF.exec(cond3));
		
		Tuple cond4 = makeCondition(fullTuple, "warc-target-urn", "(?s).*dmoz.*");
		assertTrue(filterUDF.exec(cond4));

		Tuple cond5 = makeCondition(fullTuple, "warc-target-urn1", "(?s).*dmoz.*");
		assertFalse(filterUDF.exec(cond5));
	
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
		
		long newOnlyOneSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent.warc_onlyOne.gz/part-m-00000.gz"));
		long newNoSomethingSize = FileUtils.sizeOf(new File("/tmp/test/mixedContent.warc_noSomething.gz/part-m-00000.gz"));
		assertEquals(478, newOnlyOneSize);
		assertEquals(3919, newNoSomethingSize);
		
		//long csumOrigFile = FileUtils.checksumCRC32(new File("/tmp/test/mixedContent.warc"));
		//long csumNewFile  = FileUtils.checksumCRC32(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		//assertEquals(csumOrigFile, csumNewFile);
	}
	
}
