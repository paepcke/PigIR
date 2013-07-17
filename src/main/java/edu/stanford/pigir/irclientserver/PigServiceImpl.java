package edu.stanford.pigir.irclientserver;

import java.util.Map;

public interface PigServiceImpl {

	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests.
	PigServiceHandle asyncPigRequest(String operator, Map<String, String> params);
	public PigServiceHandle getProgress(PigServiceHandle service);
	public void setScriptRootDir(String scriptRoot);
}
