package edu.stanford.pigir.irclientserver;

import java.util.Map;

import org.apache.pig.impl.PigContext;

public interface PigService {

	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests. Cannot have
	// this method in this interface, b/c Java does not allow static
	// methods in an interface. But I want this method to be static.c
	PigContext servicePigRequest(String operator, Map<String, String> params);
	public String getProgress(PigContext service);
}
