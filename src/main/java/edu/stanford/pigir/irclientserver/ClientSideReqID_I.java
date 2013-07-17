/**
 * 
 */
package edu.stanford.pigir.irclientserver;


/**
 * @author paepcke
 * 
 * Used to identify the originator or origination purpose
 * of a client side request source. Instances are passed
 * to IRClient.sendProcessRequest(), and are then automatically
 * associated with a returned-responses queue. ClientSideReqID_I
 * with the same string in the 'id' field all share an output queue.
 * Example: all requests originating from one spreadsheet cell C4
 * might use "C4" as the id. Then all request responses from that
 * cell will accumulate in the "C4" queue.
 *
 */
public interface ClientSideReqID_I {
	
	
	public enum Disposition {
		QUEUE_RESULTS,
		DISCARD_RESULTS,
		NOTIFY
	}
	
	public String getID();
	public String getRequestClass();
	public Disposition getDisposition();
	public ResultRecipient_I getResultRecipient();
}
