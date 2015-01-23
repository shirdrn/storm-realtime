package org.shirdrn.storm.analytics.mydis.constants;

public interface Constants {

	//// Configuration keys
	String SYNC_SCHEDULER_THREAD_COUNT = "sync.scheduler.thread.count";
	String SYNC_SCHEDULER_PERIOD = "sync.scheduler.period";
	String SYNC_LATEST_HOURS = "sync.worker.latest.hours";
	
	//// Required constants
	String REDIS_KEY_NS_SEPARATOR = "::";
	String NS_STAT_HKEY = "S";
	String NS_PLAY_NU_DURATION_USER = "NU";
	String NS_PLAY_AU_DURATION_USER = "AU";
	
	String DT_HOUR_FORMAT = "yyyyMMddHH";
	String DT_MINUTE_FORMAT = "yyyy-MM-dd HH:00";
}
