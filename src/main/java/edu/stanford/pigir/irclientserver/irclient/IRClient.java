package edu.stanford.pigir.irclientserver.irclient;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.irserver.IRServer;

public class IRClient {
	
	private static Map<String, ConcurrentLinkedQueue<ServiceResponsePacket>> resultQueues =
			new HashMap<String,ConcurrentLinkedQueue<ServiceResponsePacket>>();
	
	private static int CONNECT_ATTEMPT_TIMEOUT = 5000; // sec
	private Inet4Address IRSERVER_IP = null;
	private NetworkListenerClient netListener = null;
	
	public Client kryoClient;
	
	public IRClient() throws UnknownHostException, RemoteException {
		
		IRSERVER_IP = (Inet4Address) InetAddress.getByName("localhost");
		
		kryoClient = new Client();
		IRPacket.registerClasses(kryoClient);
		netListener = new NetworkListenerClient(this);
		netListener.init(kryoClient);
		kryoClient.addListener(netListener);
		kryoClient.start();
		try {
			kryoClient.connect(CONNECT_ATTEMPT_TIMEOUT, IRSERVER_IP, IRServer.IRSERVICE_PORT);
		} catch (Exception e) {
			Log.info("Failed to connect to IRServer: " + e.getMessage());
			kryoClient.stop();
			throw new RemoteException("Failed to connect to server: " + e.getMessage());
		}
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params, ClientSideReqID_I clientReqID) {
		Log.info("[Client] Sending process request: " + operator);
		// Which result queue should the response go to?
		String reqClass = clientReqID.getRequestClass();
		// Ensure that a response queue of this type exists:
		if (!resultQueues.containsKey(reqClass))
			resultQueues.put(reqClass,  new ConcurrentLinkedQueue<ServiceResponsePacket>());
		
		// Ship the request to the server:
		netListener.sendPacket(operator, params, new ClientSideReqID());
	}
	
	public void sendProcessRequest(String operator, Map<String,String> params) {
		// Create default client side request. They go into the BUILT_IN response queue:
		sendProcessRequest(operator, params, new ClientSideReqID());
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
			if (clientReqId.getResultRecipient() != null)
				clientReqId.getResultRecipient().resultAvailable(resp);
			return;
		case NOTIFY:
			if (clientReqId.getResultRecipient() != null)
				clientReqId.getResultRecipient().resultAvailable(resp);
		}
	}
}
