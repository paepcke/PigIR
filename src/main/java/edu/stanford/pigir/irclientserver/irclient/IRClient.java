package edu.stanford.pigir.irclientserver.irclient;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.irserver.IRServer;

public class IRClient {
	
	private static int CONNECT_ATTEMPT_TIMEOUT = 5000; // sec
	private Inet4Address IRSERVER_IP = null;
	private NetworkListenerClient netListener = null;
	
	public Client kryoClient;
	
	public IRClient() throws UnknownHostException, RemoteException {
		
		IRSERVER_IP = (Inet4Address) InetAddress.getByName("localhost");
		
		kryoClient = new Client();
		IRPacket.registerClasses(kryoClient);
		netListener = new NetworkListenerClient();
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
	
	public void sendProcessRequest(String operator, Map<String,String> params) {
		Log.info("[Client] Sending process request: " + operator);
		netListener.sendPacket(operator, params);
	}
	
	public void setScriptRootDir(String dir) {
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		sendProcessRequest("setPigScriptRoot", params);
	}
}
