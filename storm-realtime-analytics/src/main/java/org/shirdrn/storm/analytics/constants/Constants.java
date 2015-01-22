package org.shirdrn.storm.analytics.constants;

public interface Constants {

	long DEFAULT_INCREMENT_VALUE = 1L;
	String REDIS_KEY_NS_SEPARATOR = "::";
	
	String CACHE_ITEM_KEYD_VALUE = "Y";
	String NS_STAT_HKEY = "S";
	String NS_PLAY_NU_DURATION_USER = "NU";
	String NS_PLAY_AU_DURATION_USER = "AU";
	int CACHE_ITEM_EXPIRE_TIME = 1 * 60 * 60; // 1 hour
	
	String USER_INFO_KEY = "us"; // user static information
	String USER_BEHAVIOR_KEY = "ud"; // user dynamic information
	
	String FIRST_OPEN_DATE = "fod";
	String FIRST_PLAY_DATE = "fpd";
	
	String DT_EVENT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	String DT_HOUR_PATTERN = "yyyyMMddHH";
	String DT_DATE_PATTERN = "yyyy-MM-dd";
	String USER_INFO_KEY_PREFIX = Constants.USER_INFO_KEY + Constants.REDIS_KEY_NS_SEPARATOR;
	String USER_DYNAMIC_INFO_KEY_PREFIX = Constants.USER_BEHAVIOR_KEY + Constants.REDIS_KEY_NS_SEPARATOR;
}
