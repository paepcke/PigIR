package edu.stanford.pigir.irclientserver.irserver;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;

public class TestIRServer {
	
	static IRServer irServer = null;

	@BeforeClass
	public static void setUpOnce() throws Exception {
		irServer = IRServer.getInstance();
	}

	@Test
	public void test() {
		//public JobHandle_I newPigServiceRequest(ServiceRequestPacket req)
		//String theOperator, Map<String,String> theParams, ClientSideReqID_I theReqID)
		ClientSideReqID_I reqID = new ClientSideReqID();
		ServiceRequestPacket req = new ServiceRequestPacket("testCall", null, reqID);		
		System.out.println(req);
		JobHandle_I jobHandle = irServer.newPigServiceRequest(req);
		System.out.println(jobHandle);
	}

}
