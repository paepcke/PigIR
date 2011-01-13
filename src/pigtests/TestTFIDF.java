package pigtests;


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;
import pigir.RegexpTokenize;

public class TestTFIDF{
	
	PigServer pserver;
	Properties props = new Properties();
	public TestTFIDF() {
		 try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}
	
	void doTests() {
		try {
			Map<String,String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),"contrib/piggybank/java/piggybank.jar").toURI(); 
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			// pserver.debugOn();
			pserver.registerQuery(
					"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/morTweetsSmall.csv' USING " + 
					//"docs = LOAD 'file://E:/users/paepcke/dldev/EclipseWorkspaces/Pigir/src/Datasets/FDADataSortedCRLFFixed.csv' USING " + 
					             "org.apache.pig.piggybank.storage.CSVLoader AS " +
					             "(txt:chararray," +
					             "source:chararray," + 
					             "dateTime:chararray);");

			
			// Get size of the collection:
			pserver.registerQuery("collSizeScalar = FOREACH (GROUP docs all) GENERATE COUNT(docs) AS collSize;");
			
			/*			
            Create data with the following schema: 
              {date: chararray,
               time: chararray,
               wordTfs: {(word: chararray,tf: double)},
               tweetID: chararray}
           */

			pserver.registerQuery(
				    "docTokens = FOREACH docs {"  +
				        "dateAndTime = pigir.SplitDateTime(dateTime);" +
				      "GENERATE " +
				      "  (chararray) dateAndTime.date AS date," +
				      "  (chararray) dateAndTime.time AS time," +
				      "  pigir.TFs(pigir.RegexpTokenize(txt," +
				      										  RegexpTokenize.USE_DEFAULT_SPLIT_REGEXP + "," +
				      										  RegexpTokenize.PRESERVE_URLS + "," +
				      										  RegexpTokenize.KILL_STOPWORDS + ")) " +
				      										  		"AS tokenTfs," +
				      "  (chararray) pigir.GetUUID() AS tweetID;" +
				      "};"
			);
			
			// Get ({(w1,tf1),(w2,tf2)}, {(w2,tf3), (w3,tf4)}, ...):
			pserver.registerQuery("theTokenTfs  = FOREACH docTokens GENERATE tweetID, FLATTEN(tokenTfs) AS (token,tf);");

			/* Get relation with tuples of the form: (docId,token,termFrequency). Like:
				   (bd0b091f-d13f-459e-a88b-8b3dfad4be21,Halts,0.07142857142857142)
				   (bd0b091f-d13f-459e-a88b-8b3dfad4be21,eu,0.07142857142857142)
				   (bd0b091f-d13f-459e-a88b-8b3dfad4be21,url4,0.07142857142857142)
				   (bd0b091f-d13f-459e-a88b-8b3dfad4be21,Safety,0.07142857142857142)
			*/
		    pserver.registerQuery("theTokenGroups = GROUP theTokenTfs BY token;");  
		    
		    
		    /*
		     Get tuples of the form (token1, {(docId1,token1,tf1,1) (docId2,token1,tf1,2),...})
		        (COCOA,{(d26a3b36-905c-4b75-bd90-aed03813afde,COCOA,0.04)})
				(Halts,{(4b533115-21d8-4cfb-97ec-aa88b085fe62,Halts,0.0625),(6918ca5e-3e1b-4792-aa42-9a2f1d1c3e3e,Halts,0.06666666666666667),(da7cd067-9c78-41a1-b38a-9566980fe997,Halts,0.07142857142857142),(91b274cb-787d-4228-a3e3-5e1c69920e7a,Halts,0.06666666666666667)})
				(Trial,{(6918ca5e-3e1b-4792-aa42-9a2f1d1c3e3e,Trial,0.06666666666666667),(91b274cb-787d-4228-a3e3-5e1c69920e7a,Trial,0.06666666666666667),(4b533115-21d8-4cfb-97ec-aa88b085fe62,Trial,0.0625),(da7cd067-9c78-41a1-b38a-9566980fe997,Trial,0.07142857142857142)})
				(about,{(d26a3b36-905c-4b75-bd90-aed03813afde,about,0.04)})
		     */
		    pserver.registerQuery("theTokenTfAndDfs = FOREACH theTokenGroups {" +
		    		"									  df = COUNT(theTokenTfs);" +
		    		"									  GENERATE " +
		    		"											FLATTEN(theTokenTfs) AS (docId:chararray,token:chararray,tf:double)," +
		    		"											df AS df;" +
		    		"								   };");
				/*
				 Get tuples of the form (docID,token,tfidf), like:
				    (0f378800-1e87-436e-a49e-8804c4c2286e,FDA,0.0625,7)
					(f4cae923-e748-403d-8e97-72413b389d2d,FDA,0.06666666666666667,7)
					(076167c2-db72-4c1a-8d75-4e84aaed7f53,Fda,0.0625,1)
					(ee12dcc4-ee67-46ed-8c6b-ef9cbc0b23da,May,0.08333333333333333,1)
					(fdb73531-a2d1-43f1-942e-6fae157b6cfe,New,0.07692307692307693,2)
				 */
		    pserver.registerQuery("theTokenTfAndIdfs = FOREACH theTokenTfAndDfs {" +
		    		"										GENERATE docId,token, pigir.TFIDF(collSizeScalar.collSize,tf,df) AS tfidf;" +
		    		"										};");
		    
		    pserver.registerQuery("store theTokenTfAndIdfs INTO 'Datasets/morTweetTfIDF' USING PigStorage(',');");
		    //Common.print(pserver, "collSizeScalar");
		    //Common.print(pserver, "docTokens");
			//Common.print(pserver, "theTokenTfs");
			//Common.print(pserver, "theTokenGroups");
			//Common.print(pserver, "theTokenTfAndDfs");
			//Common.print(pserver, "theTokenTfAndIdfs");
			
			//pserver.dumpSchema("collSizeScalar");
			//pserver.dumpSchema("theTokenTfs");
			//pserver.dumpSchema("theTokenGroups");
			//pserver.dumpSchema("theTokenTfAndDfs");
		    //pserver.dumpSchema("docIdGroups");
			//pserver.dumpSchema("theTokenTfAndIdfs");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		TestTFIDF tester = new TestTFIDF();
		tester.doTests();
	}
}
