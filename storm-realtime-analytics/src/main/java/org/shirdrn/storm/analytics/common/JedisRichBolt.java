package org.shirdrn.storm.analytics.common;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
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
	static final String contextID = "realtime";
	static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	private transient ApplicationContext applicationContext;
	private transient JedisPool connectionPool;
	protected OutputCollector collector;
	protected Level redisCmdLogLevel = Level.DEBUG;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// Spring context
		applicationContext = SpringFactory.getContextFactory(contextID, SPTING_CONFIGS).getContext(contextID);
		LOG.info("Spring context initialized: " + applicationContext);
		
		connectionPool = applicationContext.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + connectionPool);
		
		this.collector = collector;
		
		// set print Redis cmd log level
		Object level = stormConf.get(Constants.REALTIME_REDIS_CMD_LOG_LEVEL);
		if(level != null) {
			redisCmdLogLevel = RealtimeUtils.parseLevel((String) level);
		}
	}
	
	public Jedis getConnection() {
		Jedis connection = null;
		try {
			connection = connectionPool.getResource();
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
			throw Throwables.propagate(e);
		}
		return connection;
	}
	
	public void releaseConnection(Jedis connection) {
		try {
			connectionPool.returnResource(connection);
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
		}
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
		connectionPool.destroy();
	}

	public Level getRedisCmdLogLevel() {
		return redisCmdLogLevel;
	}

}
