package edu.stanford.pigir.irclientserver.irclient;

import java.util.Map;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

public class NetworkListenerClient extends Listener {
	
	private Client kryoClient;
	private IRClient resultRecipient = null;
	
	public NetworkListenerClient(IRClient irClient) {
		super();
		resultRecipient = irClient;
	}	
	
	public void init(Client client) {
		this.kryoClient = client;
	}
	
	public void connected(Connection conn) {
		Log.info("[Client] Connection to server successful.");
	}

	public void sendPacket(String operator, Map<String,String> params, ClientSideReqID_I theClientSideId) {
		ServiceRequestPacket req = new ServiceRequestPacket();
		req.operator = operator;
		req.params   = params;
		req.clientSideReqId = theClientSideId;
		kryoClient.sendTCP(req);
	}
	
	public void disconnected(Connection conn) {
		Log.info("[Client] Connection to IRServer was broken.");		
	}
	
	public void received(Connection conn, Object content) {
		ServiceResponsePacket resp = null;
		try {
			resp = (ServiceResponsePacket) content;
		} catch (Exception e) {
			// Ignore Kryo framework messages, such as keep-alives.
			return;
		}
		Log.info("[Client] " + resp.resultHandle);
		resultRecipient.newPigResponse(resp);
	}
}
