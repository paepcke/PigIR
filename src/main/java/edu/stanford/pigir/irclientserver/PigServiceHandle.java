package edu.stanford.pigir.irclientserver;

public class PigServiceHandle implements JobHandle {

	JobStatus status 	= null;
	String	  jobName 	= null;
	int       errorCode = -1;
	String    message   = "";
	
	public PigServiceHandle(String theJobName, JobStatus theStatus) {
		status = theStatus;
		jobName = theJobName;
	}
	
	public PigServiceHandle(String theJobName, JobStatus theStatus, String errorMessage) {
		this(theJobName, theStatus);
		message = errorMessage;
	}
	
	@Override
	public JobStatus getStatus() {
		return status;
	}
	
	@Override
	public String getJobName() {
		return jobName;
	}

	@Override
	public int getErrorCode() {
		return errorCode;
	}

	@Override
	public String getMessage() {
		return message;
	}

	public String toString() {
		if (getJobName() != null)
			return String.format("<PigServiceHandle:%s (status: %s)>", getJobName(), getStatus());
		else
			return String.format("<PigServiceHandle:unnamed (status: %s)>", getStatus());
	}
}
