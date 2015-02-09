package org.shirdrn.storm.analytics.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.google.common.base.Throwables;

/**
 * Jedis connection manager, who manages Redis connections.
 * 
 * @author yanjun
 */
public class JedisConnectionManager implements ConnectionManager<Jedis> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JedisConnectionManager.class);
	private static final String contextID = "realtime";
	private static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	private transient final ApplicationContext applicationContext;
	private transient final JedisPool connectionPool;
	private Level redisCmdLogLevel = Level.DEBUG;
	
	public JedisConnectionManager(Configuration conf) {
		// Spring context
		applicationContext = SpringFactory.getContextFactory(contextID, SPTING_CONFIGS).getContext(contextID);
		LOG.info("Spring context initialized: " + applicationContext);
		
		connectionPool = applicationContext.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + connectionPool);
		
		// set print Redis cmd log level
		Object level = conf.getProperty(Constants.REALTIME_REDIS_CMD_LOG_LEVEL);
		if(level != null) {
			redisCmdLogLevel = RealtimeUtils.parseLevel((String) level);
		}
	}
	
	@Override
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

	@Override
	public void releaseConnection(Jedis connection) {
		try {
			connectionPool.returnResource(connection);
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
		}		
	}
	
	@Override
	public Level getCmdLogLevel() {
		return redisCmdLogLevel;
	}

}