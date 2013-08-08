package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.HTTPService;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.ResultRecipient_I;
import edu.stanford.pigir.irclientserver.Utils;

public class IRClient extends AbstractHandler {
	
	private static Map<String, ConcurrentLinkedQueue<ServiceResponsePacket>> resultQueues =
			new HashMap<String,ConcurrentLinkedQueue<ServiceResponsePacket>>();
	private static Map<String, ResultRecipient_I> resultListeners = new HashMap<String, ResultRecipient_I>();
	private static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irclient.IRClient");
	
	
	HTTPService httpService = null;
	
	public IRClient() {
		IRClient.log.setLevel(Level.DEBUG);
		BasicConfigurator.configure();
		
		httpService = new HTTPService(IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT, this);
		log.info("IR client response service running at " + IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT);		
	}

	public void sendProcessRequest(String operator, Map<String,String> params) throws IOException {
		// Create default client side request. They go into the GENERIC response queue:
		sendProcessRequestWorker(operator, params, new ClientSideReqID());
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj) throws IOException {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		reqID.setResultRecipientURI(getResponseURI(timestamp));
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, Disposition disposition) throws IOException {
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC",
													"<null>", // no callback id
													disposition);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(java.lang.String.valueOf(Common.getTimestamp())));
		}
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition) throws IOException {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp, disposition); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(timestamp));
		}
		sendProcessRequestWorker(operator, params, reqID);
	}
	
	private URI getResponseURI(String responseID) {
		URI result = null;
		try {
			Utils.getSelfURI(IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT,
					IRServiceConfiguration.IR_RESPONSE_CONTEXT + "/" + responseID);
		} catch (UnknownHostException | URISyntaxException e) {
			throw new RuntimeException("Trouble creating a response URI: " + e.getMessage());			
		}
		return result;
	}
	
	private void sendProcessRequestWorker(String operator, Map<String,String> params, ClientSideReqID_I reqID) throws IOException {	
		log.info("[Client] Sending process request: " + operator);
		
		// Which result queue should the response go to?
		String reqClass = reqID.getRequestClass();
		// Ensure that a response queue of this type exists:
		if (!resultQueues.containsKey(reqClass))
			resultQueues.put(reqClass,  new ConcurrentLinkedQueue<ServiceResponsePacket>());
		
		ServiceRequestPacket reqPaket = new ServiceRequestPacket(operator, params, reqID);
		
		// Ship the request to the server:
		httpService.sendPacket(reqPaket.toJSON().toString(), IRServiceConfiguration.IR_SERVICE_URI);
	}
	
	public void setScriptRootDir(String dir) throws IOException {
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		sendProcessRequest("setPigScriptRoot", params);
	}
	
	public void newPigResponse(ServiceResponsePacket resp) {
		ClientSideReqID_I clientReqId = resp.clientSideReqId;
		Disposition disposition = clientReqId.getDisposition();
		log.info(String.format("[Client] received response: %s; Disposition: %s, ReqClass: %s, ReqID: %s.",
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
			// in the clientRequest then notify it in addition to queueing:
			notifyListener(clientReqId, resp);
			return;
		case NOTIFY:
			notifyListener(clientReqId, resp);
			return;
		}
	}

	private void notifyListener(ClientSideReqID_I reqID, ServiceResponsePacket resp) {
		String requestID = reqID.getID();
		ResultRecipient_I resultRecipient = IRClient.resultListeners.get(requestID);
		if ( (requestID != null) && (resultRecipient != null) )
			// Notify the result listener:
			resultRecipient.resultAvailable(resp);
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
