package edu.stanford.pigir.irclientserver;

import java.net.URI;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

public class ClientSideReqID implements ClientSideReqID_I {
	
	private URI resultRecipientURI = null;
	private String id = "<null>";
	private String requestClass = "BUILT_IN";
	private Disposition responseDisposition = Disposition.DISCARD_RESULTS;
	
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
	
	public ClientSideReqID(String theRequestClass, String theId, Disposition theDisposition, URI contactCallback) {
		this(theRequestClass, theId);
		responseDisposition = theDisposition;
		resultRecipientURI = contactCallback;
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
	
	public URI getResultRecipientURI() {
		return resultRecipientURI;
	}
	
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException {
		stringer.key("resultRecipientURI");
		stringer.value(resultRecipientURI.toASCIIString());
		stringer.key("id");
		stringer.value(id);
		stringer.key("requestClass");
		stringer.key(requestClass);
		stringer.key("responseDisposition");
		stringer.value(responseDisposition);

		return stringer;
	}
}
