package edu.stanford.pigir.irclientserver.irserver;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.PigService;
import edu.stanford.pigir.irclientserver.PigService.PigServiceID;

public class IRServer {
	
		
	public static int IRSERVICE_PORT = 4040;
	
    // IRServer is a singleton:
	private static IRServer theInstance = null;

	// Map from a PigScriptRunner instance ID to the PigScriptRunner
	// instance itself (needed e.g. for progress requests):
	private Map<PigServiceID,PigService> pigServices = new HashMap<PigServiceID,PigService>();
	
	private Server kryoServer;
	
	/**
	 * Get the singleton instance of the IRServer, creating
	 * one if one exists. As a side effect, this call refreshes
	 * the server's knowledge of which Pig scripts are available.
	 * So, if a new Pig script (a.k.a. operator) is added, a 
	 * call to getInstance() will make that new script known even
	 * to a running server.
	 * @return The singleton IRServer
	 * @throws IOException
	 */
	public static IRServer getInstance() throws IOException {
		if (theInstance != null) {
			 return theInstance;
		}
		IRServer.theInstance = new IRServer();
		return IRServer.theInstance;
	}
	
	private IRServer() throws IOException {
		kryoServer = new Server();
		IRPacket.registerClasses(kryoServer);
		// Get a network port listener:  
		kryoServer.addListener(new NetworkListenerServer(this));
		// Bind server to TCP (only, not to UDP as well):
		kryoServer.bind(IRSERVICE_PORT); 
		kryoServer.start();
	}
	
	public void serviceHadoopRequest(Connection kryoConn, ServiceRequestPacket requestPacket) {
		PigService pigService = pigServices.get(requestPacket.operator);
		if (pigService == null) {
			String errMsg = String.format("Pig request operator '%s' unknown in request from %s",
										  requestPacket.operator, kryoConn.toString());
			Log.error(errMsg);
			return;
		}
		pigService.servicePigRequest(requestPacket.operator, requestPacket.params);
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Log.set(Log.LEVEL_DEBUG);
		try {
			new IRServer();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.info("IR Server running at " + IRSERVICE_PORT);
	}

}
