package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServConf;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;
import edu.stanford.pigir.irclientserver.PigServiceImpl_I;
import edu.stanford.pigir.irclientserver.PigService_I;
import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;
import edu.stanford.pigir.irclientserver.irclient.HTTPSender;

public class IRServer implements PigService_I {


	@SuppressWarnings("unused")
	private static HTTPD httpService = null;
	private static IRServer theInstance = null;
	public static Logger log = Logger.getLogger("edu.stanford.pigir.irclientserver.irserver.IRServer");
	private static Map<String,ClientSideReqID_I> jobToClient = new HashMap<String,ClientSideReqID_I>();

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
		httpService = new HTTPD(IRServConf.IR_SERVICE_REQUEST_PORT, this);
	}

	
	public ServiceResponsePacket newPigServiceRequest(ServiceRequestPacket req) {
		
		PigServiceImpl_I pigServiceImpl = new PigScriptRunner();
		JobHandle_I resJobHandle = null;
		
		if (IRServer.adminOps.contains(req.operator))
			resJobHandle = processAdminOp(pigServiceImpl, req.operator, req);
		else {
			// Get the job started (returns quickly); passing this IRServer as destination
			// for launch-done callback to pushResultNotification():
			resJobHandle = pigServiceImpl.asyncPigRequest(req.operator, req.params, this);
			// If client wants anything other than discarding the result,
			// remember which jobName is associated with the client that
			// made this request:
			ClientSideReqID_I clientInfo = req.getClientSideReqId(); 
			if ((clientInfo != null) && (clientInfo.getDisposition() != Disposition.DISCARD_RESULTS)) {
				jobToClient.put(resJobHandle.getJobName(), req.getClientSideReqId());
			}
		}
		
		ServiceResponsePacket resp = new ServiceResponsePacket(req.clientSideReqId, resJobHandle);
		log.info("[Server] responding " + resp.resultHandle.toString());
		return resp;
	}
	
	/* (non-Javadoc)
	 * @see edu.stanford.pigir.irclientserver.PigService_I#pushResultNotification(edu.stanford.pigir.irclientserver.JobHandle_I)
	 */
	public void pushResultNotification(JobHandle_I jobHandle) {
		// Get the client contact information for this job:
		ClientSideReqID_I clientInfo = jobToClient.get(jobHandle.getJobName());
		if (clientInfo == null) {
			log.error(String.format("Could not push result for job '%s' to client: no record of this job in IRServer", jobHandle.getJobName()));
			return;
		}
		if (clientInfo.getDisposition() == Disposition.DISCARD_RESULTS)
			return;
		ServiceResponsePacket resp = new ServiceResponsePacket(clientInfo, jobHandle);
		try {
			HTTPSender.sendPacket(resp, clientInfo.getResultRecipientURI());
		} catch (IOException e) {
			log.error(String.format("Encountered IO exception when attempting push result notification for job %s: %s.", jobHandle.getJobName(), e.getMessage()));
			return;
		}
	}
	
	public void pushResultNotification(ServiceResponsePacket resp) {
		ClientSideReqID_I clientInfo = resp.getClientSideReqId();
		log.info("IR service does not expect response packets from clients, but got one from: " + clientInfo.getID());
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
			resultHandle = pigService.getProgress(req.params.get("jobName"));
			break;
		}
		return resultHandle;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.INFO);
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
