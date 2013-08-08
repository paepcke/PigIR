package edu.stanford.pigir.irclientserver.irserver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.esotericsoftware.minlog.Log;

import edu.stanford.pigir.irclientserver.ArcspreadException;
import edu.stanford.pigir.irclientserver.HTTPService;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;
import edu.stanford.pigir.irclientserver.PigServiceImpl;
import edu.stanford.pigir.irclientserver.PigService_I;
import edu.stanford.pigir.irclientserver.hadoop.PigScriptRunner;

public class IRServer extends AbstractHandler implements PigService_I {


	private static HTTPService httpService = null;
	private static IRServer theInstance = null;

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
	
	// IRServer is a singleton:
	private IRServer() {
		// Create a Jetty service, and set the incoming-packet
		// handler to be this instance:
		httpService = new HTTPService(IRServiceConfiguration.IR_SERVICE_REQUEST_PORT, this);
	}

	@Override
	public void handle(String target, 
			Request baseRequest, 
			HttpServletRequest servletRequest,
			HttpServletResponse servletResponse) throws IOException, ServletException {
		servletResponse.setContentType("text/html;charset=utf-8");
		servletResponse.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);

		ServiceRequestPacket req = null;
		//******************
		System.out.println(baseRequest);
		//******************		
		//******req.logMsg();
		JobHandle_I callResult = newPigServiceRequest(req);

		ServiceResponsePacket resp = new ServiceResponsePacket();
		resp.resultHandle = callResult;
		resp.clientSideReqId = req.clientSideReqId;
		Log.info("[Server] responding " + resp.resultHandle.toString());
		servletResponse.getWriter().println("<h1>Got response</h1>");
		newPigServiceRequest(req);
	}	
	
	public JobHandle_I newPigServiceRequest(ServiceRequestPacket req) {
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

	private JobHandle_I processAdminOp(PigServiceImpl pigService, String operator, ServiceRequestPacket req) {
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
		Log.set(Log.LEVEL_DEBUG);
		try {
			IRServer.getInstance();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.info("IR Server running at " + IRServiceConfiguration.IR_SERVICE_REQUEST_PORT);
	}
}
