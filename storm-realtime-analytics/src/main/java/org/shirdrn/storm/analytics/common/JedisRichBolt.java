package org.shirdrn.storm.analytics.common;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

import com.google.common.base.Throwables;

public abstract class JedisRichBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JedisRichBolt.class);
	static final String ctxId = "realtime";
	static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	private transient ApplicationContext springCtx;
	private transient JedisPool jedisPool;
	protected OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// Spring context
		springCtx = SpringFactory.getContextFactory(ctxId, SPTING_CONFIGS).getContext(ctxId);
		LOG.info("Spring context initialized: " + springCtx);
		
		jedisPool = springCtx.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + jedisPool);
		
		this.collector = collector;
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
	
	@Override
	public void cleanup() {
		super.cleanup();
		jedisPool.destroy();
	}

}
