/**
 * 
 */
package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;

/**
 * @author paepcke
 *
 */
public class IRLib {

	private static IRClient irclient = IRClient.getInstance();

	@SuppressWarnings("serial")
	public static JobHandle_I getProgress(JobHandle_I jobHandle) throws IOException {
		final String jobName = jobHandle.getJobName();
		ServiceResponsePacket resp = irclient.sendProcessRequest("getJobStatus", 
																 new HashMap<String,String>(){{
																	 	put("jobName", jobName);
																	 	}});
		return resp.getJobHandle();
	}
	
	public static JobHandle_I setScriptRootDir(String dir) throws IOException {
		Map<String,String> params = new HashMap<String,String>();
		params.put("scriptRoot", dir);
		return irclient.sendProcessRequest("setPigScriptRoot", params).getJobHandle();
	}
	
	// --------------------------------  P R I V A T E -------------------

}
