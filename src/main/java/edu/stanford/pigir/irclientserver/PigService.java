package edu.stanford.pigir.irclientserver;


public interface PigService {
	
	public interface PigServiceID {
		public String id = null;
	}
	
	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests. Cannot have
	// this method in this interface, b/c Java does not allow static
	// methods in an interface. But I want this method to be static.c
	// public static PigServiceID servicePigRequest(String operator, Map<String,String> params);
	public String getProgress(PigServiceID service);
}
