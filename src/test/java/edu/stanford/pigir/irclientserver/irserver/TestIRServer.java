package edu.stanford.pigir.irclientserver.irserver;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

public class TestIRServer {
	
	static IRServer irServer = null;
	Map<String,String> params = null;

	@BeforeClass
	public static void setUpOnce() throws Exception {
		irServer = IRServer.getInstance();
	}
	
	@Before
	public void setUp() throws Exception {
		params = null;
	}

	@SuppressWarnings("serial")
	@Test
	public void testCallTest() {
		ClientSideReqID_I reqID = new ClientSideReqID();
		// Set parameters to null, though parm "msgToEcho" is expected:
		ServiceRequestPacket req = new ServiceRequestPacket("testCall", null, reqID);		
		JobHandle_I jobHandle = irServer.newPigServiceRequest(req);
		assertEquals(3, jobHandle.getErrorCode());
		
		// Correct parameter:
		params = new HashMap<String,String>() {{
			put("msgToEcho", "My test message");
		}};
		req.setParameters(params);
		jobHandle = irServer.newPigServiceRequest(req);
		assertEquals(JobStatus.SUCCEEDED, jobHandle.getStatus());
		assertEquals("FakeTestJobName", jobHandle.getJobName());
		//System.out.println(jobHandle);
	}
}
