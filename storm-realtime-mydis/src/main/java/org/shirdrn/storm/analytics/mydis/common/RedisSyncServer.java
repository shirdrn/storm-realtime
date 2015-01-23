package org.shirdrn.storm.analytics.mydis.common;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSyncServer extends SyncServer {

	private static final Log LOG = LogFactory.getLog(RedisSyncServer.class);
	private final JedisPool jedisPool;
	
	public RedisSyncServer(Configuration conf) {
		super(conf);
		// Redis pool
		jedisPool = context.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + jedisPool);
	}
	
	public Jedis getJedis() {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
			throw Throwables.propagate(e);
		}
		return jedis;
	}
	
	public void returnResource(Jedis jedis) {
		try {
			jedisPool.returnResource(jedis);
		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
		}
	}


}
