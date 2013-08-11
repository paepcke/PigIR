package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.stanford.pigir.irclientserver.ArcspreadException;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;
import edu.stanford.pigir.irclientserver.PigServiceImpl_I;
import edu.stanford.pigir.irclientserver.PigService_I;
import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;

public class IRServer implements PigService_I {


	@SuppressWarnings("unused")
	private static HTTPD httpService = null;
	private static IRServer theInstance = null;
	private static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irserver.IRServer");

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
	
	// IRServer is a singleton (so declared private):
	private IRServer() {
		// Create an HTTPD and set the incoming-packet
		// handler to be this instance:
		httpService = new HTTPD(IRServiceConfiguration.IR_SERVICE_REQUEST_PORT, this);
	}

	
	public ServiceResponsePacket newPigServiceRequest(ServiceRequestPacket req) {
		
		PigServiceImpl_I pigServiceImpl_I = new PigScriptRunner();
		JobHandle_I res = null;
		
		if (IRServer.adminOps.contains(req.operator))
			res = processAdminOp(pigServiceImpl_I, req.operator, req);
		else
			res = pigServiceImpl_I.asyncPigRequest(req.operator, req.params);
		
		ServiceResponsePacket resp = new ServiceResponsePacket(req.clientSideReqId, res);
		
		log.info("[Server] responding " + resp.resultHandle.toString());
		
		return resp;
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

	private JobHandle_I processAdminOp(PigServiceImpl_I pigService, String operator, ServiceRequestPacket req) {
		JobHandle_I resultHandle = new PigServiceHandle("pigServiceAdmin", JobStatus.SUCCEEDED);
		switch (operator) {
		case "setPigScriptRoot":
			try {
				pigService.setScriptRootDir(req.params.get("scriptRoot"));
			} catch (Exception e) {
				resultHandle.setMessage(e.getMessage());
				resultHandle.setStatus(JobStatus.FAILED);
			}
			break;
		case "getJobStatus":
			// TODO: parameters in req packet should be single attr/val: "jobName" : <name>"
			ArcspreadException.NotImplementedException exc = new ArcspreadException.NotImplementedException();
			resultHandle = exc;
		}
		return resultHandle;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		IRServer.log.setLevel(Level.DEBUG);
		BasicConfigurator.configure();
		try {
			// The following call will hang till
			// the server is terminated:
			IRServer.getInstance();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
