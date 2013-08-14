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
	// Must return quickly! Called from network service. Returns
	// handle to use for subsequent status requests.
	public ServiceResponsePacket newPigServiceRequest(ServiceRequestPacket req);
	public ServiceResponsePacket newPigServiceResponse(ServiceResponsePacket resp);
}
