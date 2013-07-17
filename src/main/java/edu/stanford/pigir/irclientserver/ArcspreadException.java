package edu.stanford.pigir.irclientserver;

import edu.stanford.pigir.irclientserver.JobHandle.JobStatus;

/**
 * @author paepcke
 *
 *  Exceptions passed around across the client server boundary.
 *  All these exceptions inherit from ArcspreadException, which
 *  extends Java exceptions. The exceptions are classes nested
 *  inside ArcspreadException. 
 *  
 *  What's special is that these exceptions implement JobHandle,
 *  and can therefore be passed wherever computation results are
 *  passed across the client/server boundary. The methods required
 *  by this interface are inherited from AcrspreadException.
 */

@SuppressWarnings("serial")
public class ArcspreadException extends Exception {

	private static String DEFAULT_JOBNAME = "<unnamed>";

	
	public ArcspreadException() {
		super();
	}

	public ArcspreadException(String msg) {
		super(msg);
	}

	// -----------------------------  Each Exception is a Subclass ----------------------------------
	
	public static class NotImplementedException extends ArcspreadException implements JobHandle {
		protected static final int errorCode = 1;
		@SuppressWarnings("unused")
		protected static String jobName = ArcspreadException.DEFAULT_JOBNAME;

		public NotImplementedException(String msg) {
			super(msg);
		}
		
		public NotImplementedException(String theJobName, String msg) {
			super(msg);
			jobName = theJobName;
		}
		
		public NotImplementedException() {
			super();
		}
	}

	// ----------------------------  Methods Inherited By all Excpetion Subclasses Above ---------
	public int getErrorCode() {
		int res = -1;
		try {
			res = (int) this.getClass().getDeclaredField("errorCode").get(this);
		} catch (NoSuchFieldException | SecurityException e) {
			// We guarantee that the field is defined (in each exception subclass)
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public JobStatus getStatus() {
		return JobHandle.JobStatus.FAILED;
	}

	public String getJobName() {
		String jobName = null;
		try {
			jobName = (String) this.getClass().getDeclaredField("jobName").get(this);
		} catch (NoSuchFieldException | SecurityException e) {
			// We guarantee that the field is defined (in each exception subclass)
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return jobName;
	}
	
	public void setStatus(JobStatus newStatus) {}

	public void setErrorCode(int newErrorCode) {}

	public void setMessage(String newMessage) {}
	
	
	
	
	
	public static void main(String[] args) {
		NotImplementedException exc = new NotImplementedException("Test msg");
		System.out.println(String.format("Msg: '%s'. Errcode: %d", exc.getMessage(), NotImplementedException.errorCode));
		
		exc = new NotImplementedException("Job test msg", "myJob");
		System.out.println(String.format("Msg: '%s'. Errcode: %d. Job: %s", exc.getMessage(), exc.getErrorCode(), exc.getJobName()));
		
		exc = new NotImplementedException();
		System.out.println(String.format("Msg: '%s'. Errcode: %d. Job: %s", exc.getMessage(), exc.getErrorCode(), exc.getJobName()));
		
	}
}

