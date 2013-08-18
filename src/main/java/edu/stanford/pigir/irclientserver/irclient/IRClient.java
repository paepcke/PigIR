package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.irclientserver.ArcspreadException;
import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigService_I;
import edu.stanford.pigir.irclientserver.ResultRecipient_I;
import edu.stanford.pigir.irclientserver.Utils;
import edu.stanford.pigir.irclientserver.irserver.HTTPD;

/**
 * @author paepcke
 *
 *	Class that interacts with IRServer. Sends hadoop processing requests
 *  to IRServer via HTTP POST requests, receives HTTP responses, and
 *  notifies its clients in the next layer up of incoming asynchronous
 *  results. Two types of HTTP responses are received: the HTTP responses
 *  to the HTTP POST requests sent, and the HTTP responses pushed to the client
 *  from IRServer with results of asynchronous requests. At the lowest level,
 *  HTTPD.java receives those push responses, and HTTP uses callbacks newPigServiceResponse()
 *  for processing and handing up to the application.
 *  
 *  This class is a singleton. Use getInstance() to obtain that instance.
 */
public class IRClient extends Thread implements PigService_I {
	
	private static IRClient singletonInstance = null;
	
	private static Map<String, ConcurrentLinkedQueue<JobHandle_I>> resultQueues =
			new HashMap<String,ConcurrentLinkedQueue<JobHandle_I>>();
	private static Map<String, ResultRecipient_I> resultListeners = new HashMap<String, ResultRecipient_I>();
	private static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irclient.IRClient");
	@SuppressWarnings("unused")
	private HTTPD httpd = null;
	
	
	//-------------------------  P U B L I C  ---------------------------
	
	public static IRClient getInstance() {
		if (singletonInstance != null)
			return singletonInstance;
		singletonInstance = new IRClient();
		return singletonInstance;
	}
	
	private IRClient() {
		BasicConfigurator.configure();
		start();
		log.info("IR client response service running at " + IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT);
	}
	
	
	
	/**
	 * When clients place requests whose responses are to 
	 * be queued, the client needs access to the queue on which
	 * he is to hang himself. This method returns the always
	 * available "GENERIC" queue. 
	 * @return the queue named "GENERIC"
	 */
	public ConcurrentLinkedQueue<JobHandle_I> getResponseQueueByType() {
		return resultQueues.get("GENERIC");
	}
	
	/**
	 * When clients place requests whose responses are to 
	 * be queued, the client needs access to the queue on which
	 * he is to hang himself. This method returns the queue 
	 * of the given name. Queues with new names are created by
	 * passing queue names to the appropriate sendProcessRequest() 
	 * methods below.
	 * @return the queue on which responses are delivered.
	 */
	public ConcurrentLinkedQueue<JobHandle_I> getResponseQueueByType(String reqClass) {
		return resultQueues.get(reqClass);
	}
	
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params) throws IOException {
		// Create default client side request. They go into the GENERIC response queue:
		return sendProcessRequestWorker(operator, params, new ClientSideReqID());
	}
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj) throws IOException {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		reqID.setResultRecipientURI(getResponseURI(timestamp));
		return sendProcessRequestWorker(operator, params, reqID);
	}
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, Disposition disposition) throws IOException {
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC",
													"<null>", // no callback id
													disposition);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(java.lang.String.valueOf(Common.getTimestamp())));
		}
		return sendProcessRequestWorker(operator, params, reqID);
	}
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition) throws IOException {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID("GENERIC", timestamp, disposition); // ID is the timestamp of this request
		resultListeners.put(timestamp, resultCallbackObj);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(timestamp));
		}
		return sendProcessRequestWorker(operator, params, reqID);
	}
	

	//-------------------------  C A L L B A C K S  F O R   H T T P D  ---------------------------

	/**
	 * Called by HTTPD when a response packet arrives
	 * for an earlier request. We build a response indicating
	 * that we properly received the response from the server.
	 * @param resp
	 */
	public ServiceResponsePacket newPigServiceResponse(ServiceResponsePacket resp) {
		ClientSideReqID_I clientReqId = resp.clientSideReqId;
		JobHandle_I respJobHandle = resp.getJobHandle();
		respJobHandle.setStatus(JobStatus.SUCCEEDED);
		ServiceResponsePacket respToRespPack = new ServiceResponsePacket(clientReqId, respJobHandle);

		Disposition disposition = clientReqId.getDisposition();
		log.info(String.format("[Client] received response: %s; Disposition: %s, ReqClass: %s, ReqID: %s.",
				resp.resultHandle.getStatus(), disposition, clientReqId.getRequestClass(), clientReqId.getID()));
		switch (disposition) {
		case DISCARD_RESULTS:
			return respToRespPack;
		case QUEUE_RESULTS:
			String reqClass = clientReqId.getRequestClass();
			Queue<JobHandle_I> appropriateResultQueue = resultQueues.get(reqClass);
			// There should be a queue, b/c we create queues when necessary
			// in sendProcessRequest. But who knows...:
			if (appropriateResultQueue == null)
				throw new AssertionError(String.format("No response queue exists for request class %s (req ID was %s)", reqClass, clientReqId.getID()));
			// Queue the result...
			appropriateResultQueue.add(resp.resultHandle);
			// ... and if a result callback recipient was included
			// in the clientRequest then notify it in addition to queueing:
			notifyListener(clientReqId, resp);
			return respToRespPack;
		case NOTIFY:
			notifyListener(clientReqId, resp);
			return respToRespPack;
		}
		return respToRespPack;
	}
	

	public ServiceResponsePacket newPigServiceRequest(ServiceRequestPacket req) {
		ServiceResponsePacket res = new ServiceResponsePacket(req.getClientSideReqId(),
								    new ArcspreadException.NotImplementedException("IR clients do not expect request packets, only responses to its requests to server."));
		return res;
	}
	
	//-------------------------  P R I V A T E  ---------------------------
	
	/**
	 * Workhorse for sending requests to IRServer instances
	 * 
	 * @param operator name of the script to run at the server
	 * @param params name/value pairs for all required arguments.
	 * @param reqID a client-side request ID that the server will include in responses. 
	 *           Used upon receipt of that response to route it to the appropriate application.
	 * @return a response packet object that includes the client-side ID, and a JobHandle_I
	 *           object for subsequent progress calls to the server.
	 * @throws IOException
	 */
	private ServiceResponsePacket sendProcessRequestWorker(String operator, Map<String,String> params, ClientSideReqID_I reqID) throws IOException {	
		log.info("[Client] Sending process request: " + operator);
		
		// Which result queue should the response go to?
		String reqClass = reqID.getRequestClass();
		// Ensure that a response queue of this type exists:
		if (!resultQueues.containsKey(reqClass))
			resultQueues.put(reqClass,  new ConcurrentLinkedQueue<JobHandle_I>());
		
		ServiceRequestPacket reqPaket = new ServiceRequestPacket(operator, params, reqID);
		
		// Ship the request to the server:
		ServiceResponsePacket resp = HTTPSender.sendPacket(reqPaket, IRServiceConfiguration.IR_SERVICE_URI);
		return resp;
	}

	/**
	 * Using settings in IRServiceConfiguration, create a 
	 * URI to which the IRService can send response packets.
	 * @param responseID
	 * @return
	 */
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
	

	private void notifyListener(ClientSideReqID_I reqID, ServiceResponsePacket resp) {
		String requestID = reqID.getID();
		ResultRecipient_I resultRecipient = IRClient.resultListeners.get(requestID);
		if ( (requestID != null) && (resultRecipient != null) )
			// Notify the result listener:
			resultRecipient.resultAvailable(resp);
	}
	
	public void run() {
		super.run();
		// Start the http demon. This call doesn't return until the demon
		// quits, which is why IRClient is a thread:
		httpd = new HTTPD(IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT, this);
	}
}
