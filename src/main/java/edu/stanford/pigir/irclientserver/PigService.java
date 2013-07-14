package edu.stanford.pigir.irclientserver;

import java.util.Map;

public interface PigService {
	
	public interface PigServiceID {
		public String id = null;
	}
	
	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests: 
	public PigServiceID servicePigRequest(String operator, Map<String,String> params);
	public String getProgress(PigServiceID service);
}
