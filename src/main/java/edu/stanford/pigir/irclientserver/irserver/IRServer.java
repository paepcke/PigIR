package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.IRPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.JobHandle;
import edu.stanford.pigir.irclientserver.PigService;
import edu.stanford.pigir.irclientserver.PigServiceImpl;
import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;

public class IRServer implements PigService {
	
		
	public static int IRSERVICE_PORT = 4040;
	
    // IRServer is a singleton:
	private static IRServer theInstance = null;
	private Server kryoServer;
	
	@SuppressWarnings("serial")
	private static Set<String> adminOps = new HashSet<String>() {{
		add("setPigScriptRoot");
		add("getJobStatus");
	}};
	
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
	
	public JobHandle newPigServiceRequest(ServiceRequestPacket req) {
		PigServiceImpl pigServiceImpl = new PigScriptRunner();
		if (IRServer.adminOps.contains(req.operator))
			return processAdminOp(pigServiceImpl, req.operator, req);
		else
			return pigServiceImpl.asyncPigRequest(req.operator, req.params);
	}
			
	public String getJobName() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getErrorCode() {
		// TODO Auto-generated method stub
		return 0;
	}
			
	private JobHandle processAdminOp(PigServiceImpl pigService, String operator, ServiceRequestPacket req) {
		switch (operator) {
		case "setPigScriptRoot":
			pigService.setScriptRootDir(req.params.get("scriptRoot"));
			return null;
		case "getJobStatus":
			return null;
		}
		return null;
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
