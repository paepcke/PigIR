package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;

public class IRServiceConfiguration {

	public static URI IR_SERVICE_URI = null;
	public static String IR_SERVER_STR = "http://ilhead2.stanford.edu";
	public static int IR_SERVICE_REQUEST_PORT = 8081;
	public static int IR_SERVICE_RESPONSE_PORT = 8082;	
	public static String IR_RESPONSE_CONTEXT = "response";

	public IRServiceConfiguration() {
		try {
			IR_SERVICE_URI = new URI(IR_SERVER_STR);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Bad URI syntax: " + e.getMessage());
		}
	}
}
