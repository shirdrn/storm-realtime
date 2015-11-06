package org.shirdrn.storm.live.constants;

public interface Constants {
	//// Field names
	String FIELD_LIVE_ROOM_ID = "room-id";
	String FIELD_EVENT_CODE = "event-code";
	String FIELD_EVENT_PKT = "event-pkt";
	
	
	//// Constants
	long DEFAULT_INCREMENT_VALUE = 1L;
	long DEFAULT_DECREMENT_VALUE = -1L;
	String ZERO_VALUE = "0";
	
	String DT_EVENT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	//// Configuration keys
	String LIVE_ROOM_USER_COUNTER_REFRESH_INTERVAL = "live.room.user.counter.refresh.interval";
	String LIVE_HEARTBEAT_MAX_EXPIRE_INTERVAL = "live.heartbeat.max.expire.interval";
	String LIVE_ROOM_CACHE_CAPACITY = "live.room.cache.capacity";
	String LIVE_ROOM_CACHE_EXPIRE_TIME = "live.room.cache.expire.time";
	String LIVE_ROOM_CACHE_PURGE_INTERVAL = "live.room.cache.purge.interval";
	String LIVE_ROOM_FRAGMENT_PAIRCACHE_CAPACITY = "live.room.fragment.paircache.capacity";
	
	//// User Status conStants
	String DEFAULT_ITEM_EXISTED_VALUE = "Y";
	
	//// Room/User status key
	String KEY_REDIS_NS_SEPARATOR = "::";
	String KEY_LIVE_ROOM_USER_PLAY_COUNT = 
			"l" + KEY_REDIS_NS_SEPARATOR + 
			"play" + KEY_REDIS_NS_SEPARATOR + 
			"u" + KEY_REDIS_NS_SEPARATOR + 
			"cnt";
	String KEY_LIVE_MAX_USER_COUNT_PREFIX = 
			"l" + KEY_REDIS_NS_SEPARATOR + 
			"max" + KEY_REDIS_NS_SEPARATOR + 
			"cnt";

	String KEY_LIVE_USER_STATUS = "l" + KEY_REDIS_NS_SEPARATOR + "u" + KEY_REDIS_NS_SEPARATOR + "status";
	String KEY_LIVE_HEART_BEAT = "lhb";
	
}
