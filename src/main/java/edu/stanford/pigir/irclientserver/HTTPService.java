package edu.stanford.pigir.irclientserver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class HTTPService {

	private static Server jettyServer = null;
	
	public HTTPService(int listeningPort, AbstractHandler requestHandler) {

		if (jettyServer == null) {
			try {
				jettyServer = new Server(listeningPort);
				registerMessageHandler(requestHandler);
				jettyServer.start();
			} catch (Exception e) {
				throw new RuntimeException("Could not start the HTTP (i.e. Jetty) server: " + e.getMessage());
			}
		}
	}
	
	public void registerMessageHandler(AbstractHandler handler) {
		jettyServer.setHandler(handler);
	}
	
	public int sendPacket(String jsonStr, URI targetURI ) throws IOException { 

		HttpClient httpclient = new DefaultHttpClient();
		HttpPost httppost = new HttpPost(targetURI);

		try {
			httppost.setEntity(new StringEntity(jsonStr));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Cannot place JSON message string into HTTP POST request: " + e.getMessage());
		}

		//Execute and get the response.
		HttpResponse response;
		try {
			response = httpclient.execute(httppost);
		} catch (ClientProtocolException e) {
			throw new RuntimeException("Cannot send HTTP POST request (ClientProtocolException): " + e.getMessage());
		}
		StatusLine status = response.getStatusLine();
		int statusCode = status.getStatusCode();
		return statusCode;
	}
}
