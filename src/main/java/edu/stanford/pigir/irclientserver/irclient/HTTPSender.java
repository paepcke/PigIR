package edu.stanford.pigir.irclientserver.irclient;

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

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;

public class HTTPSender {

	public static int sendPacket(ServiceRequestPacket reqPack, URI targetURI ) throws IOException { 

		HttpClient httpclient = new DefaultHttpClient();
		HttpPost httppost = new HttpPost(targetURI);
		String jsonStr = reqPack.toJSON();

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
