/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import org.json.JSONException;
import org.json.JSONStringer;

/**
 * @author paepcke
 *
 */
public interface JobHandle_I {
	
	public enum JobStatus {
		FAILED,
		KILLED,
		PREP,
		RUNNING,
		SUCCEEDED,
		UNKNOWN;
		
		public String toJSONValue() {
			return this.name();
		}
		
		public static JobStatus fromJSONValue(String jsonValue) {
			for (JobStatus anEnumValue : JobStatus.values()) {
				if (anEnumValue.toJSONValue().equals(jsonValue))
					return anEnumValue;
			}
			return null;
		}
	}
	
	
	public String getJobName();
	public JobStatus getStatus();
	public int getProgress();
	public int getNumJobsRunning();
	public long getRuntime();
	public long getBytesWritten();
	public int getErrorCode();
	public String getMessage();
	
	public void setProgress(int progress);
	public void setNumJobsRunning(int jobsStillRunning);
	public void setRuntime(long runtime);
	public void setBytesWritten(long bytesWritten);

	public void setStatus(JobStatus newStatus);
	public void setErrorCode(int newErrorCode);
	public void setMessage(String newMessage);
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException;
}

