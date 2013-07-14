package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.python.modules.time.Time;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.irserver.IRServer;

public class IRClient {
	
	private static int CONNECT_ATTEMPT_TIMEOUT = 5000; // sec
	private Inet4Address IRSERVER_IP = null;
	
	public Client kryoClient;
	
	public IRClient() throws UnknownHostException {
		
		IRSERVER_IP = (Inet4Address) InetAddress.getByName("localhost");
		
		kryoClient = new Client();
		IRPacket.registerClasses(kryoClient);
		NetworkListenerClient netListener = new NetworkListenerClient();
		netListener.init(kryoClient);
		kryoClient.addListener(netListener);
		kryoClient.start();
		try {
			kryoClient.connect(CONNECT_ATTEMPT_TIMEOUT, IRSERVER_IP, IRServer.IRSERVICE_PORT);
		} catch (IOException e) {
			Log.info("Failed to connect to IRServer.");
			kryoClient.stop();
		}
		
		
	}
	
	public static void main(String[] args) throws UnknownHostException {
		Log.set(Log.LEVEL_DEBUG);
		new IRClient();
		while (true) {
			Time.sleep(5);
		}
	}
}
