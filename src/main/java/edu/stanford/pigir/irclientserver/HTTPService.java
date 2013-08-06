package edu.stanford.pigir.irclientserver;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class HTTPService {
	
	public static Server serverForRequests = null;
	public static Server serverForResponses = null;
	
	public HTTPService() {
		
		try {
			if (serverForRequests == null) { 
				serverForRequests = new Server(IRServiceConfiguration.IR_SERVICE_REQUEST_PORT);
				serverForRequests.start();
			}
			if (serverForResponses == null) {
				serverForResponses = new Server(IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT);
				serverForResponses.start();
			}

			// Wait for both servers to be finished:
			serverForRequests.join();
			serverForResponses.join();

		} catch (Exception e) {
			throw new RuntimeException("Could not start the HTTP (i.e. Jetty) server: " + e.getMessage());
		}
	}
	
	public void registerRequestHandler(AbstractHandler handler) {
		serverForRequests.setHandler(handler);
	}
	
	public void registerResponseHandler(AbstractHandler handler) {
		serverForResponses.setHandler(handler);
	}
	
}
