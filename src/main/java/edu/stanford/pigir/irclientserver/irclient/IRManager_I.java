/**
 * 
 */
package edu.stanford.pigir.irclientserver.irclient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.stanford.pigir.irclientserver.ClientSideReqID_I.Disposition;
import edu.stanford.pigir.irclientserver.IRPacket.ServiceResponsePacket;
import edu.stanford.pigir.irclientserver.JobHandle_I;
import edu.stanford.pigir.irclientserver.ResultRecipient_I;

/**
 * All functionality expected from client-side IR service
 * by end user applications. IRLib.java is a heavy user.
 * 
 * @author paepcke
 *
 */
public interface IRManager_I {
	
	public IRClient getInstance();
	public ConcurrentLinkedQueue<JobHandle_I> getResponseQueueByType();
	public ConcurrentLinkedQueue<JobHandle_I> getResponseQueueByType(String reqClass);
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params) throws IOException;
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params, String resultQueueName) throws IOException;
	public ServiceResponsePacket sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj) throws IOException;
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, String resultQueueName) throws IOException;
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, Disposition disposition) throws IOException;
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, Disposition disposition, String resultQueueName) throws IOException;
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition) throws IOException;
	public ServiceResponsePacket  sendProcessRequest(String operator, Map<String,String> params, ResultRecipient_I resultCallbackObj, Disposition disposition, String resultQueueName) throws IOException;
	
}
