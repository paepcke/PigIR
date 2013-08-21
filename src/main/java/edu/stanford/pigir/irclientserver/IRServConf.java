package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class IRServConf {

	// ---------------  Start Common User Level Configuration Options
	
	public static String IR_SERVER = "mono.stanford.edu";
	//public static String IR_SERVER_STR = "ilhead2.stanford.edu";
	public static int IR_SERVICE_REQUEST_PORT = 4040;
	public static int IR_SERVICE_RESPONSE_PORT = 4041;
		
	public static HADOOP_EXECTYPE hadoopExecType = HADOOP_EXECTYPE.LOCAL;
	public static enum HADOOP_EXECTYPE {
		LOCAL,
		MAPREDUCE
	}
	
	// ---------------  End Common User Level Configuration Options
	
	public static String IR_RESPONSE_CONTEXT = "response";
	public static URI IR_RESPONSE_RECIPIENT_URI = null; 

	public static URI IR_SERVICE_URI = null;
	
	// Max time in msecs of Pig inactivity before concluding
	// that a Pig script has failed without even interacting
	// with Hadoop (e.g. parsing error). Used in PigScriptRunner:
	public static long STARTUP_TIME_MAX = 20000;
	
	// Pig scripts are stored in DEFAULT_SCRIPT_ROOT_DIR/PigScripts/CommandLineUtils/Pig
	// DEFAULT_SCRIPT_ROOT_DIR in turn is relative to the project root.
	// For testing, this constant is set to "src/test":
	public static String DEFAULT_SCRIPT_ROOT_DIR = "src/main/";
	
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
