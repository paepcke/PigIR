package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.HTTPService;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.PigClient_I;
import edu.stanford.pigir.irclientserver.Utils;

public class IRClient extends AbstractHandler {
	
	private static Map<String, ConcurrentLinkedQueue<ServiceResponsePacket>> resultQueues =
			new HashMap<String,ConcurrentLinkedQueue<ServiceResponsePacket>>();
	private static Map<String, PigClient_I> resultListeners = new HashMap<String, PigClient_I>();
	HTTPService httpService = null;
	
	public IRClient() {
		httpService = new HTTPService(IRServiceConfiguration.IR_SERVICE_REQUEST_PORT);
		httpService.registerResponseHandler(this);
	}

	public void sendProcessRequest(String operator, Map<String,String> params) {
		// Create default client side request. They go into the GENERIC response queue:
		sendProcessRequestWorker(operator, params, new ClientSideReqID());
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, PigClient_I resultCallbackObj) {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		try {
			reqID.setResultRecipientURI(Utils.getURI(IRServiceConfiguration.IR_RESPONSE_CONTEXT + "/" + timestamp));
		} catch (UnknownHostException | URISyntaxException e) {
			throw new RuntimeException("Trouble creating a response URI: " + e.getMessage());			
		}
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, Disposition disposition) {
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC",
													"<null>", // no callback id
													disposition);
		if (disposition != Disposition.DISCARD_RESULTS) {
			try {
				reqID.setResultRecipientURI(Utils.getURI(IRServiceConfiguration.IR_RESPONSE_CONTEXT));
			} catch (UnknownHostException | URISyntaxException e) {
				throw new RuntimeException("Trouble creating a response URI: " + e.getMessage());
			}
		}
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, PigClient_I resultCallbackObj, Disposition disposition) {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp, disposition); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		if (disposition != Disposition.DISCARD_RESULTS) {
			try {
				reqID.setResultRecipientURI(Utils.getURI(IRServiceConfiguration.IR_RESPONSE_CONTEXT + "/" + timestamp));
			} catch (UnknownHostException | URISyntaxException e) {
				throw new RuntimeException("Trouble creating a response URI: " + e.getMessage());			
			}
		}
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	private void sendProcessRequestWorker(String operator, Map<String,String> params, ClientSideReqID_I reqID) {	
		Log.info("[Client] Sending process request: " + operator);
		
		// Which result queue should the response go to?
		String reqClass = reqID.getRequestClass();
		// Ensure that a response queue of this type exists:
		if (!resultQueues.containsKey(reqClass))
			resultQueues.put(reqClass,  new ConcurrentLinkedQueue<ServiceResponsePacket>());
		
		// Ship the request to the server:
		netListener.sendPacket(operator, params, reqID);
	}
	
	public void setScriptRootDir(String dir) {
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		sendProcessRequest("setPigScriptRoot", params);
	}
	
	public void newPigResponse(ServiceResponsePacket resp) {
		ClientSideReqID_I clientReqId = resp.clientSideReqId;
		Disposition disposition = clientReqId.getDisposition();
		Log.info(String.format("[Client] received response: %s; Disposition: %s, ReqClass: %s, ReqID: %s.",
				resp.resultHandle.getStatus(), disposition, clientReqId.getRequestClass(), clientReqId.getID()));
		switch (disposition) {
		case DISCARD_RESULTS:
			return;
		case QUEUE_RESULTS:
			String reqClass = clientReqId.getRequestClass();
			Queue<ServiceResponsePacket> appropriateResultQueue = resultQueues.get(reqClass);
			// There should be a queue, b/c we create queues when necessary
			// in sendProcessRequest. But who knows...:
			if (appropriateResultQueue == null)
				throw new AssertionError(String.format("No response queue exists for request class %s (req ID was %s)", reqClass, clientReqId.getID()));
			// Queue the result...
			appropriateResultQueue.add(resp);
			// ... and if a result callback recipient was included
			// in the clientRequest then notify it:
			String requestID = clientReqId.getID();
			if ( (requestID != null) && (resultListerners. resultListeners)
				clientReqId.getResultRecipientURI().resultAvailable(resp);
			return;
		case NOTIFY:
			if (clientReqId.getResultRecipientURI() != null)
				clientReqId.getResultRecipientURI().resultAvailable(resp);
		}
	}

	@Override
	public void handle(String target, 
					   Request baseRequest, 
					   HttpServletRequest servletRequest,
					   HttpServletResponse servletResponse) throws IOException, ServletException {
		servletResponse.setContentType("text/html;charset=utf-8");
		servletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		servletResponse.getWriter().println("<h1>Got response</h1>");
	}
	
}
