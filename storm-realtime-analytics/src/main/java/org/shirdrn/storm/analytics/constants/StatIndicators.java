package org.shirdrn.storm.analytics.constants;

public interface StatIndicators {

	////// Basic indicators
	
	int USER_DEVICE_INFO = 01;
	int USER_DYNAMIC_INFO = 02;
	
	
	////// Statistical indicators
	
	int NU = 11; // New Users
	int PLAY_NU = 12; // Play New Users
	
	int AU = 21; // Active Users
	int PLAY_AU = 22; // Play Active Users
	
	int LAUNCH_TIMES = 31;
	int PLAY_TIMES = 32;
	
	int PLAY_NU_DURATION = 41;
	int PLAY_AU_DURATION = 42;
	
	
}
