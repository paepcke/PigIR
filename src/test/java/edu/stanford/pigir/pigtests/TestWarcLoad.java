package edu.stanford.pigir.pigtests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.warc.PigWarcRecord;

public class TestWarcLoad {

	static PigServer pserver;
	static Properties props = new Properties();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
			URI piggybankPath = new File("target/classes/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			String pigirJarPath  = Common.findVersionedFileName("target", "pigir", "jar");
			pserver.registerJar(pigirJarPath);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void test() throws IOException {
		pserver.registerQuery(
				"docs = LOAD 'src/test/resources/ClueWeb09_English_Sample.warc' " +
						"		USING edu.stanford.pigir.warc.WarcLoader" +
						"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
						"           optionalHeaderFlds:bytearray, content:bytearray);"
				);
		//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,date;");
		//pserver.registerQuery("docsCulled = FOREACH docs GENERATE WARC-Record-ID,date;");
		//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,content;");
		
		Iterator<Tuple> resIt = null;
		ArrayList<PigWarcRecord> warcRecs = new ArrayList<PigWarcRecord>();
		resIt = pserver.openIterator("docs");
		while (resIt.hasNext()) {
			Tuple oneTuple = resIt.next();
			try {
				PigWarcRecord warcRec = new PigWarcRecord(oneTuple);
				warcRecs.add(warcRec);
			} catch (IOException e) {
				fail(e.getMessage());
			}
		}
		// Use second and last record to do some checks:
		PigWarcRecord second = warcRecs.get(1);
		PigWarcRecord last   = warcRecs.get(warcRecs.size() - 1);
		
		assertEquals("21064", second.get("content-length"));
		
		assertEquals("http://www.snp-y.org/", last.get("WARC-Target-URI"));
		assertEquals("11258", last.get("Content-Length"));
		assertEquals("clueweb09-en0040-54-00401", last.get("warc-trec-id"));
		
		//System.out.println(warcRecs);
	}
}
