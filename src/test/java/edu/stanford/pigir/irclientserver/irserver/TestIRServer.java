package edu.stanford.pigir.irclientserver.irserver;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONStringer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.stanford.pigir.irclientserver.ClientSideReqID;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I;
import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;
import edu.stanford.pigir.irclientserver.PigServiceHandle;

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
		ServiceResponsePacket resp = irServer.newPigServiceRequest(req);
		assertEquals(3, resp.getJobHandle().getErrorCode());
		
		// Correct parameter:
		params = new HashMap<String,String>() {{
			put("msgToEcho", "My test message");
		}};
		req.setParameters(params);
		resp = irServer.newPigServiceRequest(req);
		assertEquals(JobStatus.SUCCEEDED, resp.getJobHandle().getStatus());
		assertEquals("FakeTestJobName", resp.getJobHandle().getJobName());
		//System.out.println(jobHandle);
	}
	
	@Test
	public void testClientSideReqIDJSONIZation() throws URISyntaxException, JSONException {
		ClientSideReqID_I reqID = new ClientSideReqID("spreadSheetCell", "A4", Disposition.DISCARD_RESULTS, new URI("mono.stanford.edu/foo"));
		JSONStringer stringer = new JSONStringer();
		stringer.object();
		stringer = reqID.toJSON(stringer);
		stringer.endObject();
		assertEquals("{\"resultRecipientURI\":\"mono.stanford.edu\\/foo\",\"id\":\"A4\",\"requestClass\":\"spreadSheetCell\",\"responseDisposition\":\"DISCARD_RESULTS\"}", stringer.toString());
		// System.out.println(stringer.toString());
		// Get it back:
		ClientSideReqID recoveredID = ClientSideReqID.fromJSON(stringer.toString());
		assertEquals("spreadSheetCell", recoveredID.getRequestClass());
		assertEquals("A4", recoveredID.getID());
		assertEquals(Disposition.DISCARD_RESULTS, recoveredID.getDisposition());
		assertEquals(new URI("mono.stanford.edu/foo"), recoveredID.getResultRecipientURI());
		//System.out.println(recoveredID);
	}
	
	@Test
	public void testServiceReqPacketJSONIzation() throws JSONException, URISyntaxException {
		ClientSideReqID_I reqID = new ClientSideReqID("spreadSheetCell", "A4", Disposition.DISCARD_RESULTS, new URI("mono.stanford.edu/foo"));
		Map<String,String> inParams = new HashMap<String,String>();
		inParams.put("parm1", "foo");
		inParams.put("parm2", "bar");
		ServiceRequestPacket reqPacket = new ServiceRequestPacket("myOperator", inParams, reqID);
		
		// Convert to JSON:
		String jsonReqPacketStr = reqPacket.toJSON();
		assertEquals("{\"request\":{\"resultRecipientURI\":\"mono.stanford.edu\\/foo\",\"id\":\"A4\",\"requestClass\":\"spreadSheetCell\",\"responseDisposition\":\"DISCARD_RESULTS\"},\"operator\":\"myOperator\",\"params\":{\"parm2\":\"bar\",\"parm1\":\"foo\"}}",
				      jsonReqPacketStr);
		//System.out.println(stringer.toString());
		
		// Recover the packet from the generated JSON:
		ServiceRequestPacket recoveredPack = ServiceRequestPacket.fromJSON(jsonReqPacketStr);
		
		// Check operator:
		assertEquals("myOperator", recoveredPack.getOperator());
		
		// Check integrity of clientside request id:
		ClientSideReqID_I recoveredID = recoveredPack.getClientSideReqId();
		assertEquals("spreadSheetCell", recoveredID.getRequestClass());
		assertEquals("A4", recoveredID.getID());
		assertEquals(Disposition.DISCARD_RESULTS, recoveredID.getDisposition());
		assertEquals(new URI("mono.stanford.edu/foo"), recoveredID.getResultRecipientURI());
		
		/// Check parameters:
		assertEquals("foo", recoveredPack.getParams().get("parm1"));
		assertEquals("bar", recoveredPack.getParams().get("parm2"));
		
	}
	
	@Test
	public void testServiceResponsePacketJSONIzation() throws URISyntaxException, JSONException {
		ClientSideReqID_I reqID = new ClientSideReqID("spreadSheetCell", "A4", Disposition.DISCARD_RESULTS, new URI("mono.stanford.edu/foo"));
		JobHandle_I jobHandle = new PigServiceHandle("myJob", JobStatus.RUNNING, "All good");
		ServiceResponsePacket respPack = new ServiceResponsePacket(reqID, jobHandle);
		
		// Turn to JSON:
		String jsonRespPacketStr = respPack.toJSON();
		assertEquals("{\"request\":{\"resultRecipientURI\":\"mono.stanford.edu\\/foo\",\"id\":\"A4\",\"requestClass\":\"spreadSheetCell\",\"responseDisposition\":\"DISCARD_RESULTS\"},\"serviceHandle\":{\"jobName\":\"myJob\",\"status\":\"RUNNING\",\"errorCode\":-1,\"message\":\"All good\"}}", 
				     jsonRespPacketStr);
		//System.out.println(stringer.toString());

		// Recover the packet from the generated JSON:
		ServiceResponsePacket recoveredPack = ServiceResponsePacket.fromJSON(jsonRespPacketStr);
		
		// Check integrity of clientside request id:
		ClientSideReqID_I recoveredID = recoveredPack.getClientSideReqId();
		assertEquals("spreadSheetCell", recoveredID.getRequestClass());
		assertEquals("A4", recoveredID.getID());
		assertEquals(Disposition.DISCARD_RESULTS, recoveredID.getDisposition());
		assertEquals(new URI("mono.stanford.edu/foo"), recoveredID.getResultRecipientURI());
		
		// Check result handle:
		assertEquals("myJob", recoveredPack.getJobHandle().getJobName());
		assertEquals(JobStatus.RUNNING, recoveredPack.getJobHandle().getStatus());
		assertEquals("All good", recoveredPack.getJobHandle().getMessage());
	
	}
}
