package org.shirdrn.storm.live.common;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.live.constants.Constants;

@SuppressWarnings("rawtypes")
public class LiveConfiguration implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(LiveConfiguration.class);
	
	private final Map stormConf;
	private int maxHeartbeatExpireInterval = 10; // 10 secs
	private int roomedUserCounterRefreshInterval = 3 * 1000; // 3 secs
	private int roomCachePurgeInterval = 1800 * 1000; // 30 minutes
	private int roomCacheCapacity = 500;
	private int roomAudioPairCacheCapacity = 1000;
	private int roomCacheExpireTime = 5 * 60 * 60 * 1000; // 5 hours (18000000 milliseconds)
	
	public LiveConfiguration(Map stormConf) {
		super();
		this.stormConf = stormConf;
		configureMaxHeartbeatExpireInterval();
		configureHeartbeatCheckInterval();
		configureRoomCacheCapacity();
		configureRoomCacheExpireTime();
		configureRoomAudioPairCacheCapacity();
		configureRoomCachePurgeInterval();
	}
	
	private void configureRoomCachePurgeInterval() {
		String key = Constants.LIVE_ROOM_CACHE_PURGE_INTERVAL;
		try {
			roomCachePurgeInterval = 1000 * Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, roomCachePurgeInterval);
	}

	private void configureRoomAudioPairCacheCapacity() {
		String key = Constants.LIVE_ROOM_FRAGMENT_PAIRCACHE_CAPACITY;
		try {
			roomAudioPairCacheCapacity = Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, roomAudioPairCacheCapacity);	
	}

	private void configureHeartbeatCheckInterval() {
		String key = Constants.LIVE_ROOM_USER_COUNTER_REFRESH_INTERVAL;
		try {
			roomedUserCounterRefreshInterval = 1000 * Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, roomedUserCounterRefreshInterval);
	}
	

	private void configureMaxHeartbeatExpireInterval() {
		String key = Constants.LIVE_HEARTBEAT_MAX_EXPIRE_INTERVAL;
		try {
			maxHeartbeatExpireInterval = Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, maxHeartbeatExpireInterval);
	}
	
	private void configureRoomCacheCapacity() {
		String key = Constants.LIVE_ROOM_CACHE_CAPACITY;
		try {
			roomCacheCapacity = Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, roomCacheCapacity);
	}
	
	private void configureRoomCacheExpireTime() {
		String key = Constants.LIVE_ROOM_CACHE_EXPIRE_TIME;
		try {
			roomCacheExpireTime = 1000 * Integer.parseInt(stormConf.get(key).toString());
		} catch (Exception e) { }
		logConfigKV(key, roomCacheExpireTime);
	}
	
	private void logConfigKV(String key, Object value) {
		LOG.info("Configure: key=" + key + ", value=" + value);	
	}
	
	public int getMaxHeartbeatExpireInterval() {
		return maxHeartbeatExpireInterval;
	}
	public int getRoomedUserCounterRefreshInterval() {
		return roomedUserCounterRefreshInterval;
	}
	public int getRoomCacheCapacity() {
		return roomCacheCapacity;
	}
	public int getRoomFragmentPairCacheCapacity() {
		return roomAudioPairCacheCapacity;
	}
	public int getRoomCachePurgeInterval() {
		return roomCachePurgeInterval;
	}
	public int getRoomCacheExpireTime() {
		return roomCacheExpireTime;
	}
	
	
}
