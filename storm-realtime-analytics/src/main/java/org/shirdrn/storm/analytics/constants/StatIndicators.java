package org.shirdrn.storm.analytics.constants;

public interface StatIndicators {

	////// Basic indicators
	
	int USER_DEVICE_INFO = 01;
	int USER_DYNAMIC_INFO = 02;
	
	
	////// Statistical indicators
	
	int NU = 11; // New Users
	int AU = 12; // Active Users
	int LAUNCH_TIMES = 13;
	
	int PLAY_NU = 21; // Play New Users
	int PLAY_AU = 22; // Play Active Users
	int PLAY_TIMES = 23;
	
	int PLAY_NU_DURATION = 31;
	int PLAY_AU_DURATION = 32;
	
	
}
