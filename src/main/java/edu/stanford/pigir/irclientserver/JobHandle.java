/**
 * 
 */
package edu.stanford.pigir.irclientserver;

/**
 * @author paepcke
 *
 */
public interface JobHandle {
	
	public enum JobStatus {
		FAILED,
		KILLED,
		PREP,
		RUNNING,
		SUCCEEDED
	}
	
	
	public String getJobName();
	public JobStatus getStatus();
	public int getErrorCode();
	public String getMessage();
	public void setStatus(JobStatus newStatus);
	public void setErrorCode(int newErrorCode);
	public void setMessage(String newMessage);
}
