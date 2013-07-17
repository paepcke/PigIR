/**
 * 
 */
package edu.stanford.pigir.irclientserver;

import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;

/**
 * @author paepcke
 * 
 * Implementing classes accept callbacks triggered 
 * by processing results returned from the server.
 * 
 */
public interface ResultRecipient_I {
	public void resultAvailable(ServiceResponsePacket resp);

}
