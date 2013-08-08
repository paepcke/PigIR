package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class IRServiceConfiguration {

	// ---------------  Start Common Configuration Options
	
	public static String IR_SERVER = "mono.stanford.edu";
	//public static String IR_SERVER_STR = "ilhead2.stanford.edu";
	public static int IR_SERVICE_REQUEST_PORT = 4040;
	public static int IR_SERVICE_RESPONSE_PORT = 4041;
	
	// ---------------  End Common Configuration Options
	
	public static String IR_RESPONSE_CONTEXT = "response";
	public static URI IR_RESPONSE_RECIPIENT_URI = null; 

	public static URI IR_SERVICE_URI = null;

	
static {
	try {
		IR_SERVICE_URI = new URI("http",
						 null, // no user into
						 IRServiceConfiguration.IR_SERVER,
						 IRServiceConfiguration.IR_SERVICE_REQUEST_PORT,
						 null, // no context
						 null, // no query
						 null); // no fragment

		IR_RESPONSE_RECIPIENT_URI = Utils.getSelfURI(IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT, 
													 IRServiceConfiguration.IR_RESPONSE_CONTEXT);
	} catch (URISyntaxException e) {
		throw new RuntimeException("Bad URI syntax: " + e.getMessage());
	} catch (UnknownHostException e) {
		throw new RuntimeException("Bad URI host: " + e.getMessage());
	}
	}
}
