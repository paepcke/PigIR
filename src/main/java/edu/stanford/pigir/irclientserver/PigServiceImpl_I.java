package edu.stanford.pigir.irclientserver;

import java.util.Map;

public interface PigServiceImpl_I {

	// Must return quickly! Called from network service. Returns
	// identifier to use for subsequent status requests.
	JobHandle_I asyncPigRequest(String operator, Map<String, String> params);
	public JobHandle_I getProgress(JobHandle_I service);
	public void setScriptRootDir(String scriptRoot);
	public JobHandle_I testCall(String strToEcho);
}
