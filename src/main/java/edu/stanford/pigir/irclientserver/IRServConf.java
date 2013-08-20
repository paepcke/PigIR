package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class IRServConf {

	// ---------------  Start Common Configuration Options
	
	public static String IR_SERVER = "mono.stanford.edu";
	//public static String IR_SERVER_STR = "ilhead2.stanford.edu";
	public static int IR_SERVICE_REQUEST_PORT = 4040;
	public static int IR_SERVICE_RESPONSE_PORT = 4041;
	
	public static HADOOP_EXECTYPE hadoopExecType = HADOOP_EXECTYPE.LOCAL;
	public static enum HADOOP_EXECTYPE {
		LOCAL,
		MAPREDUCE
	}
	
	// ---------------  End Common Configuration Options
	
	public static String IR_RESPONSE_CONTEXT = "response";
	public static URI IR_RESPONSE_RECIPIENT_URI = null; 

	public static URI IR_SERVICE_URI = null;
	
	// Max time in msecs of Pig inactivity before concluding
	// that a Pig script has failed without even interacting
	// with Hadoop (e.g. parsing error). Used in PigScriptRunner:
	public static long STARTUP_TIME_MAX = 20000;

	
static {
	try {
		IR_SERVICE_URI = new URI("http",
						 null, // no user into
						 IRServConf.IR_SERVER,
						 IRServConf.IR_SERVICE_REQUEST_PORT,
						 null, // no context
						 null, // no query
						 null); // no fragment

		IR_RESPONSE_RECIPIENT_URI = Utils.getSelfURI(IRServConf.IR_SERVICE_RESPONSE_PORT, 
													 IRServConf.IR_RESPONSE_CONTEXT);
	} catch (URISyntaxException e) {
		throw new RuntimeException("Bad URI syntax: " + e.getMessage());
	} catch (UnknownHostException e) {
		throw new RuntimeException("Bad URI host: " + e.getMessage());
	}
	}
}
