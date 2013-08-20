/**
 * 
 */
package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.IRServiceConfiguration;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.JobHandle_I.JobStatus;

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
	
	
	/**
	 * Busy-wait for a job to either succeed, fail, or be declared dead.
	 * NOTE: consider using getResultQueue().**** instead
	 * @param jobHandle
	 * @return
	 * @throws IOException 
	 */
	public JobHandle_I awaitResultPolling(JobHandle_I jobHandle) throws IOException {
		return awaitResultPolling(jobHandle, IRServiceConfiguration.STARTUP_TIME_MAX);
	}
	
	/**
	 * Busy-wait for a job to either succeed, fail, or be declared dead.
	 * NOTE: consider using getResultQueue().**** instead
	 * @param jobHandle
	 * @param timeout
	 * @return
	 * @throws IOException 
	 */
	public JobHandle_I awaitResultPolling(JobHandle_I jobHandle, long timeout) throws IOException {
		long startWaitTime = System.currentTimeMillis();
		while (true) {
			if ((System.currentTimeMillis() - startWaitTime) > timeout) {
				jobHandle.setStatus(JobStatus.FAILED);
				jobHandle.setMessage(String.format("Job '%s' did not either fail or succeed within timeout of %d seconds", jobHandle.getJobName(),timeout/1000));
				return jobHandle;
			}
			jobHandle = getProgress(jobHandle);
			if (jobHandle.getStatus() == JobStatus.SUCCEEDED)
				return jobHandle;
			if (jobHandle.getStatus() == JobStatus.FAILED) {
				String errMsg = String.format("Job '%s' returned failure (error code %d): %s", jobHandle.getJobName(), jobHandle.getErrorCode(), jobHandle.getMessage());
				jobHandle.setMessage(errMsg);
				return jobHandle;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				continue;
			}
		}
	}
	
	// --------------------------------  P R I V A T E -------------------

}
