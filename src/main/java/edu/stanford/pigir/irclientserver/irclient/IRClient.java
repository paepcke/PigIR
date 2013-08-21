package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
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
import edu.stanford.pigir.irclientserver.IRServConf;
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
 *  results.
 *  
 *  It is recommended that applications use methods in IRLib.java for
 *  issuing processing requests, rather than directly calling the methods
 *  in this class. 
 *  
 *  Two types of HTTP responses are received: the single-connection HTTP 
 *  returns to the HTTP POST requests that this IRClient sends to theIRServer, 
 *  and HTTP responses that the IRServer pushed to this client when the server
 *  has finished processing an asynchronous processing request.
 *  
 *  At the lowest level, HTTPD.java receives those push responses, and HTTPD
 *  uses callbacks to newPigServiceResponse() for processing and handing the received
 *  results up to the application.
 *  
 *  The bulk of the methods in this class are polymorphisms of sendProcessRequest().
 *  The signature variations mostly allow callers to specify what happens with results.
 *  
 *  This class is a singleton. Use getInstance() to obtain that instance.
 */
public class IRClient extends Thread implements PigService_I {
	
	private static IRClient singletonInstance = null;
	
	// Mapping of result queue names to queues of JobHandle_Is. 
	// Must be wrapped in synchronizedMap, to make sendProcessRequesWorker()
	// and its feeders (sendProcessRequest()) thread safe:
	private static Map<String, ConcurrentLinkedQueue<JobHandle_I>> resultQueues =
			Collections.synchronizedMap(new HashMap<String,ConcurrentLinkedQueue<JobHandle_I>>());
	// Mapping of result queue names to callers of sendProcessRequest() methods.
	// Made thread safe by wrapping in synchronizedMap:
	private static Map<String, ResultRecipient_I> resultListeners = 
			Collections.synchronizedMap(new HashMap<String, ResultRecipient_I>());
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
		log.info("IR client response service running at " + IRServConf.IR_SERVICE_RESPONSE_PORT);
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
	
	/*---------------------------
	* sendProcessRequest(operator, parameters)
	*-------------------*/
	/**
	 * Send an IR request. Since no ResultRecipeint_I is provided in this 
	 * method flavor, no asynchronous response should be expected. The return
	 * value will provide the result of the request across the wire to the Server,
	 * including a JobHandle that can be used in subsequent calls to getProgress().
	 * The default for the result Disposition is Disposition.DISCARD_RESULTS. That
	 * is asynchronous results delivered from IRServer are not queued for the caller
	 * to be picked up at some point. This method is optimal when no response is
	 * expected from the server, such as for admin operators that merely configure
	 * the server.
	 * @param operator IR script name to run at server
	 * @param params name/value pairs for arguments expected by the script
	 * @return a response packet object with status and a JobHandle_I for follow-on queries. 
	 * @throws IOException
	 */
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params) throws IOException {
		// Create default client side request. They go into the GENERIC response queue:
		return sendProcessRequestWorker(operator, params, new ClientSideReqID());
	}
	
	/*---------------------------
	* sendProcessRequest(operator, parameters, resultQueueName)
	*-------------------*/
	/**
	 * Send an IR request. Since no ResultRecipeint_I is provided in this 
	 * method flavor, no asynchronous response should be expected. The return
	 * value will provide the result of the request across the wire to the Server,
	 * including a JobHandle that can be used in subsequent calls to getProgress().
	 * The default for the result Disposition is Disposition.DISCARD_RESULTS. That
	 * is asynchronous results delivered from IRServer are not queued for the caller
	 * to be picked up at some point. This method is optimal when no response is
	 * expected from the server, such as for admin operators that merely configure
	 * the server.
	 * @param operator IR script name to run at server
	 * @param params name/value pairs for arguments expected by the script
	 * @param resultQueueName arbitrary name for referring to result queue. 
	 * @return a response packet object with status and a JobHandle_I for follow-on queries. 
	 * @throws IOException
	 */
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params, String resultQueueName) throws IOException {
		// Create default client side request. They go into the GENERIC response queue:
		return sendProcessRequestWorker(operator, params, new ClientSideReqID());
	}

	/*---------------------------
	* sendProcessRequest(operator, params, resultCallbackObj)
	*-------------------*/
	
	/**
	 * Send an IR request. A response disposition of Disposition.NOTIFY is 
	 * implied. That is the passed-in ResultRecipient_I's resultAvailable()
	 * method will be called when the IRServer signals that a script has 
	 * finished.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param resultCallbackObj an object that is notified when a script has finished 
	 * @return a response packet object with status and a JobHandle_I for follow-on queries.
	 * @throws IOException
	 */
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj) throws IOException {
		return sendProcessRequest(operator, params, resultCallbackObj, "GENERIC");
	}
	
	/*---------------------------
	* sendProcessRequest(operator, params, resultCallbackObj, resultQueueName)
	*-------------------*/
	
	/**
	 * Send an IR request. A response disposition of Disposition.NOTIFY is 
	 * implied. That is the passed-in ResultRecipient_I's resultAvailable()
	 * method will be called when the IRServer signals that a script has 
	 * finished.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param resultCallbackObj an object that is notified when a script has finished
	 * @param resultQueueName arbitrary name for referring to result queue. 
	 * @return a response packet object with status and a JobHandle_I for follow-on queries.
	 * @throws IOException
	 */
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, String resultQueueName) throws IOException {
		String clientID = createClientID();
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID(resultQueueName, clientID, Disposition.NOTIFY, getResponseURI(clientID));
		resultListeners.put(clientID, resultCallbackObj);
		reqID.setResultRecipientURI(getResponseURI(clientID));
		return sendProcessRequestWorker(operator, params, reqID);
	}
	
	/*---------------------------
	* sendProcessRequest(operator, params, disposition)
	*-------------------*/
	/**
	 * Send an IR request. Since no callback object is provided in 
	 * this method, the Disposition must not be Disposition.NOTIFY.
	 * It is still fine to use Disposition.QUEUE_RESULTS and Disposition.DISCARD_RESULTS.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param disposition instruction on what to do once a result is available from the script
	 * @return a response packet object with status and a JobHandle_I for follow-on queries.
	 * @throws IOException
	 */
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, Disposition disposition) throws IOException {
		return sendProcessRequest(operator, params, disposition, "GENERIC");
	}

	/*---------------------------
	* sendProcessRequest(operator, params, disposition, resultQueueName)
	*-------------------*/
	/**
	 * Send an IR request. Since no callback object is provided in 
	 * this method, the Disposition must not be Disposition.NOTIFY.
	 * It is still fine to use Disposition.QUEUE_RESULTS and Disposition.DISCARD_RESULTS.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param disposition instruction on what to do once a result is available from the script
	 * @param resultQueueName arbitrary name for referring to result queue. 
	 * @return a response packet object with status and a JobHandle_I for follow-on queries.
	 * @throws IOException
	 */
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, Disposition disposition, String resultQueueName) throws IOException {
		if (disposition == Disposition.NOTIFY) {
			throw new IllegalArgumentException("If result disposition is to be NOTIFY, must use sendProcessRequest() flavor that includes recipient of the notifications.");
		}
		String clientID = createClientID();
		// Event queue for results: "generic". ID: timestamp 
		ClientSideReqID reqID = new ClientSideReqID(resultQueueName,
													clientID,
													disposition);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(java.lang.String.valueOf(Common.getTimestamp())));
		}
		return sendProcessRequestWorker(operator, params, reqID);
	}
	
	/*---------------------------
	* sendProcessRequest(operator, params, resultCallObj, disposition)
	*-------------------*/
	
	/**
	 * Send an IR request. The value of Disposition is not constrained in
	 * this flavor of the sendProcessRequest() method.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param resultCallbackObj resultCallbackObj an object that is notified when a script has finished 
	 * @param disposition instruction on what to do once a result is available from the script
	 * @return
	 * @throws IOException
	 */
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition) throws IOException {
		return sendProcessRequest(operator, params, resultCallbackObj, disposition, "GENERIC");
	}
	
	/*---------------------------
	* sendProcessRequest(operator, params, resultCallObj, disposition, resultQueueName)
	*-------------------*/
	
	/**
	 * Send an IR request. The value of Disposition is not constrained in
	 * this flavor of the sendProcessRequest() method.
	 * @param operator IR script name to run at server
	 * @param params params name/value pairs for arguments expected by the script
	 * @param resultCallbackObj resultCallbackObj an object that is notified when a script has finished 
	 * @param disposition instruction on what to do once a result is available from the script
	 * @param resultQueueName arbitrary name for referring to result queue. 
	 * @return
	 * @throws IOException
	 */
	
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition, String resultQueueName) throws IOException {
		String clientID = createClientID();
		URI responseURI = getResponseURI(clientID);
		ClientSideReqID reqID = new ClientSideReqID(resultQueueName, clientID, disposition, responseURI);
		resultListeners.put(clientID, resultCallbackObj);
		if (disposition != Disposition.DISCARD_RESULTS) {
			reqID.setResultRecipientURI(getResponseURI(clientID));
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
	public void pushResultNotification(ServiceResponsePacket resp) {
		ClientSideReqID_I clientReqId = resp.clientSideReqId;
		JobHandle_I respJobHandle = resp.getJobHandle();
		respJobHandle.setStatus(JobStatus.SUCCEEDED);

		Disposition disposition = clientReqId.getDisposition();
		log.info(String.format("[Client] received response: %s; Disposition: %s, ReqClass: %s, ReqID: %s.",
				resp.resultHandle.getStatus(), disposition, clientReqId.getRequestClass(), clientReqId.getID()));
		switch (disposition) {
		case DISCARD_RESULTS:
			return;
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
		case NOTIFY:
			notifyListener(clientReqId, resp);
		}
	}
	
	public void pushResultNotification(JobHandle_I jobHandle) {
		log.error("IRClient does not expect push notifications with JobHandle_I argument");
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
		ServiceResponsePacket resp = HTTPSender.sendPacket(reqPaket, IRServConf.IR_SERVICE_URI);
		return resp;
	}

	private String createClientID() {
		String timestamp = java.lang.String.valueOf(Common.getTimestamp());
		return "IRClient_" + timestamp;
	}
	
	/**
	 * Using settings in IRServConf, create a 
	 * URI to which the IRService can send response packets.
	 * @param responseID will be appended to this machine's URI as the URI context.
	 * @return fully formed URI to which the IRServer can send ServiceResponsPackets.
	 */
	private URI getResponseURI(String responseID) {
		URI result = null;
		try {
			result = Utils.getSelfURI(IRServConf.IR_SERVICE_RESPONSE_PORT,
					IRServConf.IR_RESPONSE_CONTEXT + "/" + responseID);
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
		httpd = new HTTPD(IRServConf.IR_SERVICE_RESPONSE_PORT, this);
	}

}
