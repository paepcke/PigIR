/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceRequestPacket;

/**
 * @author paepcke
 *
 */
public interface PigService {
	// Must return quickly! Called from network service. Returns
	// handle to use for subsequent status requests.
	public JobHandle newPigServiceRequest(ServiceRequestPacket req);
}
