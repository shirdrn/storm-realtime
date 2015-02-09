package org.shirdrn.storm.analytics.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.TimeoutCache;

import redis.clients.jedis.Jedis;

public class RedisTimeoutCache implements TimeoutCache<Jedis, String, String> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(RedisTimeoutCache.class);
	
	@Override
	public void put(Jedis client, String key, String value, int seconds) {
		client.set(key, value);
		RealtimeUtils.printRedisCmd(LOG, "SET " + key + " " + value);
		
		client.expire(key, seconds);
		RealtimeUtils.printRedisCmd(LOG, "EXPIRE " + key + " " + seconds);
	}

	@Override
	public String get(Jedis client, String key) {
		return client.get(key);
	}


	

}
