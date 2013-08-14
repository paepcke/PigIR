package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;

public class HTTPSender {

	public static ServiceResponsePacket sendPacket(ServiceRequestPacket reqPack, URI targetURI ) throws IOException { 

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
		} catch (HttpHostConnectException e) {
			throw new IOException(String.format("Could not contact IR Server at %s: %s", targetURI, e.getMessage()));
		}
		// Get body of HTTP response, which should be a JSONized 
		// ServiceResponsePacket:
		HttpEntity responseEntity = response.getEntity();
		ServiceResponsePacket respPack = null;
		if(responseEntity!=null) {
			String jsonResponse = EntityUtils.toString(responseEntity);
			try {
				respPack = ServiceResponsePacket.fromJSON(jsonResponse);
			} catch (JSONException e) {
				respPack = makeBadResponseErrorPacket(reqPack);
			}
		} 
		//StatusLine status = response.getStatusLine();
		//int statusCode = status.getStatusCode();
		return respPack;
	}
	
	private static ServiceResponsePacket makeBadResponseErrorPacket(ServiceRequestPacket reqPack) {
		ClientSideReqID_I clientReqID = reqPack.getClientSideReqId();
		JobHandle_I fakeJobHandle = new PigServiceHandle("<unknown>", JobStatus.FAILED, "HTTP response body did not contain parsable JSON for request " + reqPack.toString());
		return new ServiceResponsePacket(clientReqID, fakeJobHandle);
	}
}
