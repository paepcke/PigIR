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
	public int getErrorCode();
	public String getMessage();
	public void setStatus(JobStatus newStatus);
	public void setErrorCode(int newErrorCode);
	public void setMessage(String newMessage);
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException;
}

