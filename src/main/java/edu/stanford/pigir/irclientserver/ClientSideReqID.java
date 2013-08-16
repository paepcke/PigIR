package edu.stanford.pigir.irclientserver;

import java.net.URI;
import java.net.URISyntaxException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

public class ClientSideReqID implements ClientSideReqID_I {
	
	private URI resultRecipientURI = null;
	private String id = "<null>";
	private String requestClass = "GENERIC";
	private Disposition responseDisposition = Disposition.DISCARD_RESULTS;
	
	// For generating good debug messages when JSON decoding goes wrong:
	private enum JSONDecodingStates {
			GET_ID,
			GET_REQ_CLASS,
			GET_DISPOSITION,
			GET_URI_STR,
			CREATE_URI
	}
	
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
		stringer.value(responseDisposition.toJSONValue());

		return stringer;
	}
	
	public static ClientSideReqID fromJSON(String jsonStr) throws JSONException {
		JSONDecodingStates decodeState = JSONDecodingStates.GET_ID;
		String id = null;
		String reqClass = null;
		Disposition disposition = null;
		String uriStr = null;
		
		URI resultRecipientURI = null;
		try {
			JSONObject jObj = new JSONObject(jsonStr);
			id = jObj.getString("id");
			decodeState = JSONDecodingStates.GET_REQ_CLASS;
			reqClass = jObj.getString("requestClass");
			decodeState = JSONDecodingStates.GET_DISPOSITION;
			String dispositionJSONStr = jObj.getString("responseDisposition");
			disposition = Disposition.fromJSONValue(dispositionJSONStr);
			decodeState = JSONDecodingStates.GET_URI_STR;
			uriStr = jObj.getString("resultRecipientURI");
			if (uriStr.equals("<null>"))
				resultRecipientURI = null;
			else {
				decodeState = JSONDecodingStates.CREATE_URI;
				resultRecipientURI = new URI(uriStr);
			}
		} catch (JSONException e) {
			String errMsg = null;
			switch (decodeState) {
			case GET_ID:
				errMsg = String.format("Cannot decode JSON object ClientSideReqID: failed while fetching the ID from '%s' (%s)", jsonStr, e.getMessage());
				break;
			case GET_REQ_CLASS:
				errMsg = String.format("Cannot decode JSON object ClientSideReqID: failed while fetching the ClassID from '%s' (%s)", jsonStr, e.getMessage());
			case GET_DISPOSITION:
				errMsg = String.format("Cannot decode JSON object ClientSideReqID: failed while fetching the Disposition from '%s' (%s)", jsonStr,  e.getMessage());
			case GET_URI_STR:
				errMsg = String.format("Cannot decode JSON object ClientSideReqID: failed while fetching the result recipient URI string from '%s' (%s)", jsonStr, e.getMessage()); 
			}
			throw new JSONException(errMsg);
		} catch (URISyntaxException e) {
			throw new JSONException(String.format("Cannot decode JSON object ClientSideReqID: failed while creating URI from '%s' in JSON string '%s' (%s)", uriStr, jsonStr, e.getMessage())); 
		}
		return new ClientSideReqID(reqClass, id, disposition, resultRecipientURI);
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
