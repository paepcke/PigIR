package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONStringer;

public class ClientSideReqID implements ClientSideReqID_I {
	
	private URI resultRecipientURI = null;
	private String id = "<null>";
	private String requestClass = "GENERIC";
	private Disposition responseDisposition = Disposition.DISCARD_RESULTS;
	
	public ClientSideReqID() {
		resultRecipientURI = IRServiceConfiguration.IR_RESPONSE_RECIPIENT_URI;
	}
	
	public ClientSideReqID(String theId) {
		this();
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
		// Check whether the callback URI has a port:
		resultRecipientURI = ensurePortPresent(contactCallback);
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
	
	public void setResultRecipientURI(URI theURI) {
		resultRecipientURI = theURI;
	}
	
	public JSONStringer toJSON(JSONStringer stringer) throws JSONException {
		stringer.key("resultRecipientURI");
		if (resultRecipientURI != null)
			stringer.value(resultRecipientURI.toASCIIString());
		else
			stringer.value("<null>");
		stringer.key("id");
		stringer.value(id);
		stringer.key("requestClass");
		stringer.value(requestClass);
		stringer.key("responseDisposition");
		stringer.value(responseDisposition);

		return stringer;
	}
	
	private URI ensurePortPresent(URI uri) {
		if (uri.getPath() != null)
			return uri;

		// Add the configured request port:
		try {
			uri = new URI(uri.getScheme(),
					null, // no "userInfo"
					uri.getHost(),
					IRServiceConfiguration.IR_SERVICE_RESPONSE_PORT,
					null, // no path
					null, // no query
					null); // no fragment;
		return uri;
		} catch (URISyntaxException e) {
			throw new RuntimeException("URI for a IR service call was syntactically incorrect: " + e.getMessage());
		}
	}
}
