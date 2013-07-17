package edu.stanford.pigir.irclientserver;

public class ClientSideReqID implements ClientSideReqID_I {
	
	private String id = "<null>";
	private String requestClass = "BUILT_IN";
	private Disposition responseDisposition = Disposition.DISCARD_RESULTS;
	private ResultRecipient_I resultRecipient = null;
	
	public ClientSideReqID() {
	}
	
	public ClientSideReqID(String theId) {
		id = theId;
	}

	public ClientSideReqID(String theRequestClass, String theId) {
		this(theId);
		requestClass = theRequestClass;
	}
	
	public ClientSideReqID(String theRequestClass, String theId, Disposition theDisposition) {
		this(theRequestClass, theId);
		if (theDisposition == Disposition.NOTIFY) {
			throw new IllegalArgumentException("If result disposition is to be NOTIFY, must use constructor that includes recipient of the notifications.");
		}
		// Not a NOTIFY disposition: don't need a callback recipient:
		responseDisposition = theDisposition;
	}
	
	public ClientSideReqID(String theRequestClass, String theId, Disposition theDisposition, ResultRecipient_I contactCallback) {
		this(theRequestClass, theId);
		responseDisposition = theDisposition;
		resultRecipient = contactCallback;
	}
	
	public String getID() {
		return id;
	}

	public String getRequestClass() {
		return requestClass;
	}
	
	public Disposition getDisposition() {
		return responseDisposition;
	}
	
	public ResultRecipient_I getResultRecipient() {
		return resultRecipient;
	}
}
