/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

/**
 * @author paepcke
 *
 */
public interface PigService_I {
	// 
	// 
	/**
	 * Called from server-side HTTPD when a request for IR service
	 * arrives. Must return quickly! Returns JobHandle to use for 
	 * subsequent status requests. 
	 * @param req the ServiceRequestPacket that contains details about the request.
	 * @return the ServiceReponsePacket that represents the resulting
	 *            HTTP response to the client.
	 */
	public ServiceResponsePacket newPigServiceRequest(ServiceRequestPacket req);
	
	/**
	 * Relevant for IRClients: the client-side HTTPD has received 
	 * a ServiceResponsePacket that reports on the finishing status
	 * of an earlier asynchronous call. If appropriate, the IRClient is expected to
	 * notify the application  that originated the IR request of the received response:
	 * @param resp the ServiceResponsePacket contained in the HTTP result notification packet
	 */
	public void pushResultNotification(ServiceResponsePacket resp);
	
	/**
	 * On IRServer side: method for script runner to report termination of an
	 * entire launch.
	 * @param jobHandle structure that contains job name and outcome.
	 */
	public void pushResultNotification(JobHandle_I jobHandle);
}
