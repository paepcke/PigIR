package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

public class TestCSVWriter {

	PigServer pserver;
	Properties props = new Properties();

	public TestCSVWriter() {
		
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	void doTests() {
		
		try {
			
			Map<String, String> env = System.getenv();
			// URI piggybankPath = new File(env.get("PIG_HOME"),
			URI piggybankPath = new File("E:/Users/Paepcke/Software/Hadoop/pig-0.8.0/contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
/*			
			// Matrix of integers: 1,2,3
			//                     4,5,6
			pserver.registerQuery("A = LOAD 'E:/Users/Paepcke/dldev/TestData/integers.csv' " +
								   "USING org.apache.pig.piggybank.storage.CSVLoader;");
			Common.print(pserver, "A");
			pserver.registerQuery("STORE A INTO 'E:/Users/Paepcke/dldev/TestData/integersBackOut.csv' USING pigir.pigudf.CSVExcelWriter();");
*/
			// Matrix of hairy stuff:
  			//        Andreas, Paepcke, 10
			//		  Inez,Griesbaum,20
			//		  Johnny "the knive", Conley, 40
			//		  Conrad, "blue, gray man", 50
			
			pserver.registerQuery("B = LOAD 'E:/Users/Paepcke/dldev/TestData/strings.csv' " +
								   "USING org.apache.pig.piggybank.storage.CSVLoader;");
			Common.print(pserver, "B");
			pserver.registerQuery("STORE B INTO 'E:/Users/Paepcke/dldev/TestData/stringsBackOut.csv' USING pigir.pigudf.CSVExcelWriter();");
			
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		new TestCSVWriter().doTests();
	}
}
