package pigtests;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import pigir.Common;

public class BagReturnTester {

	PigServer pserver;
	Properties props = new Properties();
	public BagReturnTester() {
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
			//pserver.debugOn();
			pserver.registerQuery(
					  //"docs = LOAD 'file://E:/users/paepcke/dldev/EclipseWorkspaces/PigUtils/src/Datasets/morTweetsSmall.csv' USING " + 
					  "docs = LOAD 'resources/Datasets/morTweetsSmall.csv' USING " + 
					             "org.apache.pig.piggybank.storage.CSVLoader AS " +
					             "(txt:chararray," +
					             "source:chararray," + 
					             "dateTime:chararray);");

			pserver.registerQuery("collOfTuples = FOREACH docs GENERATE pigutils.BagTest(), dateTime;");
			Common.print(pserver, "collOfTuples");
			pserver.registerQuery("myColl = FOREACH collOfTuples GENERATE collection;");
			
			Common.print(pserver, "myColl");
			pserver.dumpSchema("collOfTuples");
			pserver.dumpSchema("myColl");
			
			Iterator<Tuple> bagsIt = pserver.openIterator("myColl");
			while (bagsIt.hasNext()) {
				Common.print(pserver, bagsIt.next());
			}
					
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
					
	public static void main(String[] args) {
		new BagReturnTester().doTests();

	}

}
