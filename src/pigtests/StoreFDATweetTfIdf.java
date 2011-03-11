package pigtests;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.pigudf.RegexpTokenize;

/* TfIDF of morTweetsSmall.csv final entry (all Andreas fuzz, Menlo Park, 7/26/10 3:58) should be:
   (c72f171d-5a11-47e4-af56-dae50ef99fb7,fuzz,0.2329900014453396)
   (c72f171d-5a11-47e4-af56-dae50ef99fb7,Andreas,0.0)
   (c72f171d-5a11-47e4-af56-dae50ef99fb7,all,-0.013797561719408352)
*/

public class StoreFDATweetTfIdf {
	
	PigServer pserver;
	Properties props = new Properties();
	public StoreFDATweetTfIdf() {
		 try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}
	
	void doTests() {
		try {
			pserver.registerJar("contrib/piggybank.jar");
			pserver.registerJar("contrib/pigUtils.jar");
			// pserver.debugOn();
			pserver.registerQuery(
					//"docs = LOAD 'file://E:/users/paepcke/dldev/EclipseWorkspaces/PigUtils/src/Datasets/morTweetsSmall.csv' USING " + 
					"docs = LOAD 'file://E:/users/paepcke/dldev/EclipseWorkspaces/PigUtils/src/Datasets/FDADataSortedCRLFFixed.csv' USING " + 
					             "org.apache.pig.piggybank.storage.CSVLoader AS " +
					             "(txt:chararray," +
					             "source:chararray," + 
					             "dateTime:chararray);");

			/*			
            Create data with the following schema: 
              {date: chararray,time: chararray,
               wordTfs: {(word: chararray,tf: double)},
               docID: chararray}
           */
			pserver.registerQuery(
				    "docTokens = FOREACH docs {"  +
				        "dateAndTime = pigutils.SplitDateTime(dateTime);" +
				        "docId = (chararray) pigutils.GetUUID();" +
				      "GENERATE " +
				      "  (chararray) dateAndTime.date AS date," +
				      "  (chararray) dateAndTime.time AS time," +
				      "  pigutils.TFs(pigutils.RegexpTokenize(txt," +
				      										  RegexpTokenize.USE_DEFAULT_SPLIT_REGEXP + "," +
				      										  RegexpTokenize.PRESERVE_URLS + "," +
				      										  RegexpTokenize.KILL_STOPWORDS + "))" +
				      										  "," +
				      "  docId AS docId;" +
				      "};"
			);
			// Get ((id1, {(w1,tf1),(w2,tf2)}), (id2, {(w2,tf3), (w3,tf4)}), ...):
			pserver.registerQuery("theWordTfs  = FOREACH docTokens GENERATE docId, wordTfs;");
			// Get (all, {(id1, {(w1,tf1),(w2,tf2)}), (id2, {(w2,tf3), (w3,tf4)}), ...)}:
			pserver.registerQuery("docIdPlusWordTfsBag = GROUP theWordTfs all;");
			// The following works wbRecordReader the cluster, but not here:
			//pserver.registerQuery("tfidfs = FOREACH docIdPlusWordTfsBag GENERATE FLATTEN(pigutils.TfIdf(theWordTfs));");
			// Get {(id1,w1,tfidf),(id2,w2,tfidf), ...}
			pserver.registerQuery("tfidfs = FOREACH docIdPlusWordTfsBag {" +
					"result = pigutils.TfIdf(docIdPlusWordTfsBag.theWordTfs);" +
					"GENERATE FLATTEN(result) AS (docId:chararray, word:chararray, tfidf:double);" +
					"};");
			
			pserver.registerQuery("tweetDateTimes = FOREACH docTokens GENERATE docId,date,time;");
			pserver.registerQuery("tfIdfDateTimesJoin =  JOIN tweetDateTimes BY docId, tfidfs BY docId;");
			pserver.registerQuery("tfIdfDateTimes = FOREACH tfIdfDateTimesJoin GENERATE " +
					                                   "tweetDateTimes::docId," +
					                                   "tweetDateTimes::date," +
					                                   "tweetDateTimes::time," +
					                                   "tfidfs::word," +
					                                   "tfidfs::tfidf;");
			pserver.registerQuery("STORE tfIdfDateTimes INTO 'FDATfIdfs' USING PigStorage;");


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		StoreFDATweetTfIdf tester = new StoreFDATweetTfIdf();
		tester.doTests();
	}
}
