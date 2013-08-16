package edu.stanford.pigir.irclientserver;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

public class PigServiceHandle implements JobHandle_I {

	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		
	String	  jobName 	= "null";
	JobStatus status 	= JobStatus.UNKNOWN;
	int       errorCode = -1;
	String    message   = "";
	
	private enum ServiceHandleJSONDecodeState {
		GET_JOBNAME,
		GET_ERROR_CODE,
		GET_MESSAGE,
		GET_JOB_STATUS
	}
	
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
		stringer.value(getStatus().toJSONValue());
		stringer.key("errorCode");
		stringer.value(getErrorCode());
		stringer.key("message");
		stringer.value(getMessage());
		
		return stringer;
	}
	
	public static PigServiceHandle fromJSON(String jsonStr) throws JSONException {
		String jobName = null;
		JobStatus status = null;
		int errorCode = -1;
		String msg = null;
		PigServiceHandle res = null;
		ServiceHandleJSONDecodeState decodeState = ServiceHandleJSONDecodeState.GET_JOBNAME; 

		try {
			JSONObject jObj = new JSONObject(jsonStr);
			jobName = jObj.getString("jobName");
			decodeState = ServiceHandleJSONDecodeState.GET_ERROR_CODE;
			errorCode = jObj.getInt("errorCode");
			decodeState = ServiceHandleJSONDecodeState.GET_MESSAGE;
			msg = jObj.getString("message");
			decodeState = ServiceHandleJSONDecodeState.GET_JOB_STATUS;
			status = JobStatus.fromJSONValue(jObj.getString("status"));			
			
			res = new PigServiceHandle(jobName, status, msg);
			res.setErrorCode(errorCode);
			return res;
		} catch (JSONException e) {
			String errMsg = null;
			switch (decodeState) {
			case GET_JOBNAME:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the job name from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_ERROR_CODE:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the error code from '%s' (%s)", jsonStr, e.getMessage());
			case GET_MESSAGE:
				errMsg = String.format("Cannot decode JSON object job handle: failed while fetching the message from '%s' (%s)", jsonStr,  e.getMessage());
			case GET_JOB_STATUS:
				errMsg = String.format("Cannot decode JSON object job handle: failed while extracting the job status from '%s' (%s)", jsonStr,  e.getMessage());
			}
			throw new JSONException(errMsg);
		}
	}
}
