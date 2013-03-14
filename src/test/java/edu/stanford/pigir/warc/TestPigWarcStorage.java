package edu.stanford.pigir.warc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestPigWarcStorage {
	PigWarcStorage storage = new PigWarcStorage();
	Tuple t = null;
	
	@SuppressWarnings("hiding")
	public class TestRecordWriter<Object, Text> extends RecordWriter<Object, Text> {
		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
		}

		@Override
		public void write(Object arg0, Text arg1) throws IOException,
				InterruptedException {
			System.out.print(((Text) arg1).toString());
		}
	}

	@Before
	public void setUp() throws Exception {
		t = TupleFactory.getInstance().newTuple(6);
		/*
		Need:
			WARC_RECORD_ID,
			CONTENT_LENGTH,
			WARC_DATE,
			WARC_TYPE
			[some optional fields to test]
			content
		*/
		t.set(0, "<recID1>");
		t.set(1,19);
		t.set(2,"mar-4-2013");
		t.set(3,"response");
		

		Tuple attrValTuple1 = TupleFactory.getInstance().newTuple(2);
		Tuple attrValTuple2 = TupleFactory.getInstance().newTuple(2);
		attrValTuple1.set(0, "tuple1Key1");
		attrValTuple1.set(1, "tuple1Value1");
		attrValTuple2.set(0, "tuple2Key1");
		attrValTuple2.set(1, "tuple2Value1");
		DefaultDataBag warcOptionalHeaderFields = new DefaultDataBag();
		warcOptionalHeaderFields.add(attrValTuple1);
		warcOptionalHeaderFields.add(attrValTuple2);

		t.set(4,warcOptionalHeaderFields);
		t.set(5, "This is the content");
	}

	@Test
	@Ignore
	public void testGetFieldValue() throws ExecException {
		assertEquals("<recID1>", storage.getFieldValue(t.get(0)));
		assertEquals("19", storage.getFieldValue(t.get(1)));
		assertEquals("mar-4-2013", storage.getFieldValue(t.get(2)));
		assertEquals("response", storage.getFieldValue(t.get(3)));
		
		String expectedOptionalFields = "tuple1Key1: tuple1Value1\ntuple2Key1: tuple2Value1\n"; 
		String actualOptionalFields   = storage.getFieldValue(t.get(4));
		assertEquals(expectedOptionalFields, actualOptionalFields);
		
		assertEquals("This is the content", storage.getFieldValue(t.get(5)));
	}
	
	//Keep ignored
	@Test
	@Ignore
	public void testGetOutputFormat() {
		TextOutputFormat<WritableComparable<?>, Tuple> outFormat = storage.getOutputFormat();
		System.out.println(outFormat);
	}
	
	@Test
	@Ignore
	public void testWriteTuple() throws IOException {
		storage.prepareToWrite(new TestRecordWriter<Object, Text>());
		storage.putNext(t);
	}

	// Ignored because it takes a number of seconds to complete.
	// Do run for thorough testing!

	@Test
	@Ignore
	public void testTrueLoadThenStore() throws IOException, InterruptedException {
		
		System.out.println("Reading WARC records via Hadoop, then writing them back, and checking result ...please wait...");
		
		String[] cmd = new String[1];
		cmd[0] = "src/test/PigScripts/testPigWarcStorage";
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
		long newSize = FileUtils.sizeOf(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		assertEquals(refSize, newSize);
		
		//long csumOrigFile = FileUtils.checksumCRC32(new File("/tmp/test/mixedContent.warc"));
		//long csumNewFile  = FileUtils.checksumCRC32(new File("/tmp/test/testPigWarcStorageResult.warc/part-m-00000"));
		//assertEquals(csumOrigFile, csumNewFile);
	}
}
