package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.HttpResponse;
//import org.apache.http.client.ClientProtocolException;

import org.json.JSONException;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;

public class HTTPSender {

	/**
	 * Send an IR request packet to IRServer. Return the ServiceResonsePacket
	 * that is returned immediately for asyn requests, or very quickly for
	 * administrative requests.
	 * @param reqPack
	 * @param targetURI
	 * @return
	 * @throws IOException
	 */
	public static ServiceResponsePacket sendPacket(ServiceRequestPacket reqPack, URI targetURI ) throws IOException {
		String jsonStr = reqPack.toJSON();
		ServiceResponsePacket respPack = null;
		try {
			return sendPacket(jsonStr, targetURI);
		} catch (JSONException e) {
			respPack = makeBadResponseErrorPacket(reqPack, e);
		}
		return respPack;
	}
	
	/**
	 * Send a push-response packet that notifies an IRClient (and thereby one of its applications)
	 * that a particular launch has finished all its jobs. Returns the reponse packet that the
	 * client side HTTPD returned.
	 * @param respPack
	 * @param targetURI
	 * @return
	 * @throws IOException
	 */
	public static ServiceResponsePacket sendPacket(ServiceResponsePacket respPack, URI targetURI ) throws IOException {
		String jsonStr = respPack.toJSON();
		ServiceResponsePacket respToRespPack = null;
		try {
			return sendPacket(jsonStr, targetURI);
		} catch (JSONException e) {
			respToRespPack = makeBadResponseErrorPacket(respPack, e);
		}
		return respToRespPack;
	}
	
	/**
	 * Workhorse for sentPacket() methods above. Contacts the given URI, expecting an HTTPD there.
	 * Sends the given JSON string in the body of an HTTP POST message. Returns
	 * the HTTP response payload part reified into a ServiceResponsePacket.
	 * @param jsonPayload
	 * @param targetURI
	 * @return
	 * @throws IOException
	 * @throws JSONException
	 */
	private static ServiceResponsePacket sendPacket(String jsonPayload, URI targetURI ) throws IOException, JSONException { 

		HttpClient httpclient = new DefaultHttpClient();
		HttpPost httppost = new HttpPost(targetURI);

		try {
			httppost.setEntity(new StringEntity(jsonPayload));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Cannot place JSON message string into HTTP POST request: " + e.getMessage());
		}

		//Execute and get the response.
		HttpResponse response;
		try {
			response = (HttpResponse) httpclient.execute(httppost);
		} catch (HttpHostConnectException e) {
			throw new IOException(String.format("Could not contact IR Server at %s: %s", targetURI, e.getMessage()));
		} catch (HttpException e) {
			throw new RuntimeException("Cannot send HTTP POST response (HttpException): " + e.getMessage());
		}
		// Get body of HTTP response, which should be a JSONized 
		// ServiceResponsePacket:
		HttpEntity responseEntity = response.getEntity();
		ServiceResponsePacket respPack = null;
		if(responseEntity!=null) {
			String jsonResponse = EntityUtils.toString(responseEntity);
			// The following will throw a JSONException if response entity was mangled.
			// Caller must catch that exception:
			respPack = ServiceResponsePacket.fromJSON(jsonResponse);
		} 
		//StatusLine status = response.getStatusLine();
		//int statusCode = status.getStatusCode();
		return respPack;
	}
	
	private static ServiceResponsePacket makeBadResponseErrorPacket(ServiceResponsePacket initialRespPack, Exception e) {
		ClientSideReqID_I clientReqID = initialRespPack.getClientSideReqId();
		String jobName = initialRespPack.getJobHandle().getJobName();
		JobHandle_I fakeJobHandle = new PigServiceHandle(jobName, 
														 JobStatus.FAILED, 
														 String.format("HTTP body in response to push-response has no parsable JSON for request by clientID %s (job: %s): %s",
																 	   clientReqID.getID(),
																 	   jobName,
																 	   e.getMessage()));
		return new ServiceResponsePacket(clientReqID, fakeJobHandle);
	}
	
	private static ServiceResponsePacket makeBadResponseErrorPacket(ServiceRequestPacket reqPack, Exception e) {
		ClientSideReqID_I clientReqID = reqPack.getClientSideReqId();
		JobHandle_I fakeJobHandle = new PigServiceHandle("<unknown>", 
														 JobStatus.FAILED, 
														 String.format("HTTP response body has no parsable JSON for request by clientID %s (operator: %s): %s",
																 	   clientReqID.getID(),
																 	   reqPack.getOperator(),
																 	   e.getMessage()));
		return new ServiceResponsePacket(clientReqID, fakeJobHandle);
	}
}
