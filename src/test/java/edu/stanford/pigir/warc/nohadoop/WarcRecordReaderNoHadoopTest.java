package test.java.edu.stanford.pigir.warc.nohadoop;

import static org.junit.*;

import java.io.File;
import java.io.IOException;

import main.java.edu.stanford.pigir.warc.nohadoop.WarcRecord;
import main.java.edu.stanford.pigir.warc.nohadoop.WarcRecordReader;

public class WarcRecordReaderNoHadoopTest {

	File testWarcFile0_18;
	File testWarcFile0_18GZipped;
	File testWarcFile1_0;
	WarcRecordReader warcReader0_18;
	WarcRecordReader warcReader1_0;
	WarcRecordReader warcReader0_18GZipped;
	
	@Before
	public void setUp() throws Exception {
		
		//---------- WARC/0.18 -------------
		
		testWarcFile0_18 = new File("src/test/resources/tinyWarc0_18.warc");
		if (!testWarcFile0_18.exists()) {
			System.out.println("File " + testWarcFile0_18 + " does not exist.");
		} else {
			//System.out.println("File " + testWarcFile0_18 + " exists.");
			//System.out.println("File length is " + testWarcFile0_18.length());
		}
		
		warcReader0_18 = new WarcRecordReader(testWarcFile0_18);
		
		//---------- WARC/1.0 -------------

		testWarcFile1_0 = new File("src/test/resources/tinyWarc1_0.warc");
		if (!testWarcFile1_0.exists()) {
			System.out.println("File " + testWarcFile1_0 + " does not exist.");
		} else {
			//System.out.println("File " + testWarcFile1_0 + " exists.");
			//System.out.println("File length is " + testWarcFile1_0.length());
		}
		warcReader1_0 = new WarcRecordReader(testWarcFile1_0);
		
		//---------- WARC/0.18GZipped -------------		
		testWarcFile0_18GZipped = new File("src/test/resources/tinyWarc0_18.warc.gz");
		if (!testWarcFile0_18GZipped.exists()) {
			System.out.println("File " + testWarcFile0_18GZipped + " does not exist.");
		} else {
			//System.out.println("File " + testWarcFile0_18GZipped + " exists.");
			//System.out.println("File length is " + testWarcFile0_18GZipped.length());
		}
		warcReader0_18GZipped = new WarcRecordReader(testWarcFile0_18GZipped);
	}

	@Test
	public void testWarc0_18() throws IOException {
		assertTrue(warcReader0_18.nextKeyValue());
		long key = warcReader0_18.getCurrentKey();
		WarcRecord   record = warcReader0_18.getCurrentValue();
		assertEquals(0, key);
		assertEquals("warcinfo", record.get(WarcRecord.WARC_TYPE));
		//assertEquals("WARC/0.18", record.get(WarcRecord.WARC_TYPE));
		
		assertTrue(warcReader0_18.nextKeyValue());
		record = warcReader0_18.getCurrentValue();
		assertEquals("response", record.get(WarcRecord.WARC_TYPE));
		int lenFirstContentRecord = 21064; // Includes HTTP header
		assertEquals(lenFirstContentRecord, Integer.parseInt(record.get(WarcRecord.CONTENT_LENGTH)));
		String content = record.get(WarcRecord.CONTENT);
		assertEquals(lenFirstContentRecord, content.length());
		
		assertTrue(warcReader0_18.nextKeyValue());
		record = warcReader0_18.getCurrentValue();
		assertEquals("response", record.get(WarcRecord.WARC_TYPE));
		int len2ndContentRecord = 21032; // Includes HTTP header
		assertEquals(len2ndContentRecord, Integer.parseInt(record.get(WarcRecord.CONTENT_LENGTH)));
	}

	@Test
	public void testWarc0_18GZipped() throws IOException {
		assertTrue(warcReader0_18GZipped.nextKeyValue());
		long key = warcReader0_18GZipped.getCurrentKey();
		WarcRecord   record = warcReader0_18GZipped.getCurrentValue();
		assertEquals(0, key);
		assertEquals("warcinfo", record.get(WarcRecord.WARC_TYPE));
		//assertEquals("WARC/0.18", record.get(WarcRecord.WARC_TYPE));
		
		assertTrue(warcReader0_18GZipped.nextKeyValue());
		record = warcReader0_18GZipped.getCurrentValue();
		assertEquals("response", record.get(WarcRecord.WARC_TYPE));
		int lenFirstContentRecord = 21064; // Includes HTTP header
		assertEquals(lenFirstContentRecord, Integer.parseInt(record.get(WarcRecord.CONTENT_LENGTH)));
		String content = record.get(WarcRecord.CONTENT);
		assertEquals(lenFirstContentRecord, content.length());
		
		assertTrue(warcReader0_18GZipped.nextKeyValue());
		record = warcReader0_18GZipped.getCurrentValue();
		assertEquals("response", record.get(WarcRecord.WARC_TYPE));
		int len2ndContentRecord = 21032; // Includes HTTP header
		assertEquals(len2ndContentRecord, Integer.parseInt(record.get(WarcRecord.CONTENT_LENGTH)));
	}
	
	@Test
	public void testWarc1_0() throws IOException {
		assertTrue(warcReader1_0.nextKeyValue());
		long key = warcReader1_0.getCurrentKey();
		WarcRecord   record = warcReader1_0.getCurrentValue();
		assertEquals(0, key);
		assertEquals("warcinfo", record.get(WarcRecord.WARC_TYPE));

		assertTrue(warcReader1_0.nextKeyValue());
		record = warcReader1_0.getCurrentValue();
		int lenFirstContentRecord = 62; // Includes HTTP header
		assertEquals(lenFirstContentRecord, Integer.parseInt(record.get(WarcRecord.CONTENT_LENGTH)));
		String content = record.get(WarcRecord.CONTENT);
		assertEquals(lenFirstContentRecord, content.length());
		
	
	}
}
