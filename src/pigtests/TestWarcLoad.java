package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

class TestWarcLoad {

	PigServer pserver;
	Properties props = new Properties();

	public TestWarcLoad() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}

	void doTests() {
		try {
			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			pserver.registerQuery(
					"docs = LOAD 'resources/Datasets/ClueWeb09_English_Sample.warc' " +
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					//"docs = LOAD 'file://" +
					//env.get("HOME") +
					//"C:/Users/Paepcke" +
					//"/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					//"/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					"		USING pigir.warc.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray," +
					"           optionalHeaderFlds:bytearray, content:chararray);"
			);

			/*
			pserver.registerQuery(
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					"docs = LOAD 'file://" +
					//env.get("HOME") +
					"C:/Users/Paepcke" +
					"/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					//"/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					"		USING pigir.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int);"
			);
			*/
			/*
			pserver.registerQuery(
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					//"docs = LOAD 'file://E:/users/paepcke/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					"docs = LOAD 'file://" +
					//env.get("HOME") +
					"C:/Users/Paepcke" +
					"/dldev/Datasets/ClueWeb09_English_SampleCompressed.warc.gz' " +
					//"/dldev/Datasets/ClueWeb09_English_Sample.warc' " +
					"		USING pigir.WarcLoader" +
					"       AS (warcRecordId:chararray, contentLength:int, date:chararray);"
			);
			*/
			
			pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,date;");
			//pserver.registerQuery("docsCulled = FOREACH docs GENERATE contentLength,content;");
			//Common.print(pserver, "docs");
			Common.print(pserver, "docsCulled");
			pserver.dumpSchema("docs");
			//pserver.dumpSchema("docsCulled");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new TestWarcLoad().doTests();
	}
}