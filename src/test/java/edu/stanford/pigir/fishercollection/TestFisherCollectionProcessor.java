package edu.stanford.pigir.fishercollection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.CanReadFileFilter;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.fishercollection.FisherCollectionProcessor.Topic;

public class TestFisherCollectionProcessor {

	String testDir = "src/test/resources/FisherCollDir";
	String testOutDir = "/tmp/FisherCol";
	File tmpOutFile = null;
	
	String[] res1 = new String[]{
		"WARC/1.0",
		"WARC-Type: resource",
		"WARC-Date: 2013-05-25T18:17Z",
		"WARC-RECORD-ID: file:///fe_03_05858.txt",
		"Content-Type: application/warc",
		"Content-Length: 100",
		"Fisher-topic-name: ENG10",
		"Fisher-topic-short_desc: Hypothetical Situations. An Anonymous Benefactor",
		"Fisher-topic-desc: If an unknown benefactor offered each of you a million dollars - with the only stipulation being that you could never speak to your best friend again - would you take the million dollars? ", 		
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
		"WARC-Type: resource",
		"WARC-Date: 2013-05-25T18:17Z",
		"WARC-RECORD-ID: file:///fe_03_11599.txt",
		"Content-Type: application/warc",
		"Content-Length: 72",
		"Fisher-topic-name: ENG25",
		"Fisher-topic-short_desc: Strikes by Professional Athletes.",
		"Fisher-topic-desc: How do each of you feel about the recent strikes by professional athletes? Do you think that professional athletes deserve the high salaries they currently receive? ", 		
		"",
		"i think it was  gossiping",
		"gossiping  (( mm ))  probably smoking",
		"smoking"
	};
	//ArrayList<String> res2 = new ArrayList<String>();
	HashMap<String,String[]> removalResults = new HashMap<String,String[]>();

	
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
	
		removalResults.put("/tmp/FisherCol/fe_03_05858.warc", res1);
		removalResults.put("/tmp/FisherCol/fe_03_11599.warc", res2);
	}

	@Test
	public void testTimestampStripping() throws IOException {
		new FisherCollectionProcessor(testDir, testOutDir);
		File outDirFile = new File(testOutDir);
		Iterator<File> newOutFiles = FileUtils.iterateFiles(outDirFile, CanReadFileFilter.CAN_READ, null); 
		for (int i=0; i<2; i++) {
			File nextResFile = newOutFiles.next();
			List<String> resultLines = FileUtils.readLines(nextResFile);
			String[] expectedStrArr = removalResults.get(nextResFile.getAbsolutePath());
			String[] resStrArr = resultLines.toArray(new String[0]);
			// The WARC date field in expected must be adjusted to match
			// the date field of the just-created materials. Else
			// the equality assertion will fail:
			expectedStrArr[2] = resStrArr[2];
			assertArrayEquals(expectedStrArr, resStrArr);
			//System.out.println("Result file " + nextResFile.getAbsolutePath() + ":");
			//System.out.println(resultLines);
			
		}
	}
	
	@Test
	public void testFileToTopicMapCreation() throws InvocationTargetException, IOException {
		
		FisherCollectionProcessor fisherProc = new FisherCollectionProcessor();
		HashMap<String,Topic> testRes = fisherProc.buildFileNameToTopicMap();
		Topic topic = testRes.get("00034");
		assertEquals(topic.topicName, "ENG35");
		assertEquals(topic.topicShortDesc, "Illness.");

		topic = testRes.get("11672");
		assertEquals(topic.topicName, "ENG31");
		assertEquals(topic.topicShortDesc, "Corporate Conduct in the US.");
	}
	
	@Test
	public void testGzipping() throws IOException, InterruptedException {
		FisherCollectionProcessor fisherProc = new FisherCollectionProcessor();
		// Method to be tested will cooperate with tests if 'testing' is true;
		fisherProc.testing = true;
		ArrayList<String> fileNames = new ArrayList<String>();
		List<String> commands  = new ArrayList<String>();
		try {
			fisherProc.gzipAllWarcFiles(fileNames);
		} catch (IOException expected) {
			// expected exception.
		}
		
		fileNames.add("f1");
		commands = fisherProc.gzipAllWarcFiles(fileNames);
		assertEquals(1, commands.size());
		assertEquals("gzip f1 ", commands.get(0));
		
		fileNames.add("f2");
		fileNames.add("f3");
		fileNames.add("f4");
		fileNames.add("f5");
		fileNames.add("f6");
		fileNames.add("f7");
		fileNames.add("f8");
		fileNames.add("f9");
		fileNames.add("f10");
		fileNames.add("f11");
		fileNames.add("f12");
		fileNames.add("f13");
		fileNames.add("f14");
		fileNames.add("f15");
		commands = fisherProc.gzipAllWarcFiles(fileNames);
		assertEquals(1, commands.size());
		assertEquals("gzip f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 f14 f15 ", commands.get(0));
		
		fileNames.add("f16");
		commands = fisherProc.gzipAllWarcFiles(fileNames);
		assertEquals(1, commands.size());
		assertEquals("gzip f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 f14 f15 f16 ", commands.get(0));

		fileNames.add("f17");
		commands = fisherProc.gzipAllWarcFiles(fileNames);
		assertEquals(2, commands.size());
		assertEquals("gzip f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 f14 f15 f16 ", commands.get(0));
		assertEquals("gzip f17 ", commands.get(1));
	}
}
