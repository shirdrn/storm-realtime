package org.shirdrn.storm.analytics.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.utils.RedisCmdUtils;

import redis.clients.jedis.Jedis;

public class RedisTimeoutCache implements TimeoutCache<Jedis, String, String> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(RedisTimeoutCache.class);
	
	@Override
	public void put(Jedis client, String key, String value, int seconds) {
		client.set(key, value);
		RedisCmdUtils.printCmd(LOG, "SET " + key + " " + value);
		
		client.expire(key, seconds);
		RedisCmdUtils.printCmd(LOG, "EXPIRE " + key + " " + seconds);
	}

	@Override
	public String get(Jedis client, String key) {
		return client.get(key);
	}


	

}
