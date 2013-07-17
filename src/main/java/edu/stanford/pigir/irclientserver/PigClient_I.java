/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

/**
 * @author paepcke
 *
 */
public interface PigClient_I {
	public void newPigResponse(ServiceResponsePacket resp);
}
