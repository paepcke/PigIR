package edu.stanford.pigir.irclientserver;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

public class PigServiceHandle implements JobHandle_I {

	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	
	String	  jobName 	= "null";
	JobStatus status 	= JobStatus.UNKNOWN;
	int       errorCode = -1;
	String    message   = "";
	
	public PigServiceHandle() {
		// Kryonet deserialization needs a no-args constructor. 
	}
	
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

	@Override
	public void setStatus(JobStatus newStatus) {
		status = newStatus;
	}

	@Override
	public void setErrorCode(int newErrorCode) {
		errorCode = newErrorCode;
	}

	@Override
	public void setMessage(String newMessage) {
		message = newMessage;
	}
	
	public String toString() {
		if (getJobName() != null)
			return String.format("<PigServiceHandle:%s (status: %s)>", getJobName(), getStatus());
		else
			return String.format("<PigServiceHandle:unnamed (status: %s)>", getStatus());
	}
	
	public static String getPigTimestamp() {
		return PigServiceHandle.timestampFormat.format(new Date()); 
	}
	
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException {
		stringer.key("jobName");
		stringer.value(getJobName());
		stringer.key("status");
		stringer.value(getStatus());
		stringer.key("errorCode");
		stringer.value(getErrorCode());
		stringer.key("message");
		stringer.value(getMessage());
		
		return stringer;
	}
}
