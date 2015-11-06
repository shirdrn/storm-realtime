package org.shirdrn.storm.live.utils;

import java.util.Arrays;
import java.util.Map;

import joptsimple.internal.Strings;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.commons.constants.Keys;

import redis.clients.jedis.Jedis;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public class LiveRealtimeUtils {

	private static final Log LOG = LogFactory.getLog(LiveRealtimeUtils.class);
	
	public static Configuration getDefaultConfiguration() {
		try {
			return new PropertiesConfiguration("config.properties");
		} catch (ConfigurationException e) {
			throw Throwables.propagate(e);
		}
	}
	
	public static Configuration getConfiguration(String name) {
		try {
			return new PropertiesConfiguration(name);
		} catch (ConfigurationException e) {
			throw Throwables.propagate(e);
		}
	}
	
	
	public static BaseRichSpout newKafkaSpout(String topic, Configuration config) {
		String[] zks = config.getStringArray(Keys.KAFKA_ZK_SERVERS);
		String zkServers = Strings.join(zks, ",");
		String zkRoot = config.getString(Keys.STORM_ZK_ROOT); 
		boolean forceFromStart = config.getBoolean(Keys.STORM_KAFKA_FORCE_FROM_START, false);
		String clientId = config.getString(Keys.KAFKA_CLIENT_ID);
		Preconditions.checkArgument(clientId != null, "Kafka client ID MUST NOT be null!");
		
		// Configure Kafka
		BrokerHosts brokerHosts = new ZkHosts(zkServers);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, clientId);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = forceFromStart;
		
		// Configure Storm
		String[] zkHosts = config.getStringArray(Keys.STORM_ZK_HOSTS);
		int zkPort = config.getInt(Keys.STORM_ZK_PORT, 2181);
		spoutConf.zkServers = Arrays.asList(zkHosts);
		spoutConf.zkPort = zkPort;
		return new KafkaSpout(spoutConf);
	}
	
	
	private static final String CMD_PREFIX = "COMMAMD => ";
	private static final Map<Level, Integer> LOG_LEVEL_MAP = Maps.newHashMap();
	
	static {
		LOG_LEVEL_MAP.put(Level.DEBUG, Level.DEBUG_INT);
		LOG_LEVEL_MAP.put(Level.INFO, Level.INFO_INT);
		LOG_LEVEL_MAP.put(Level.WARN, Level.WARN_INT);
		LOG_LEVEL_MAP.put(Level.ERROR, Level.ERROR_INT);
		LOG_LEVEL_MAP.put(Level.FATAL, Level.FATAL_INT);
	}
	
	public static Level parseLevel(String level) {
		if(!Strings.isNullOrEmpty(level)) {
			level.toUpperCase();
		}
		return Level.toLevel(level);
	}
	
	public static void printRedisCmd(Log log, String cmd) {
		printRedisCmd(log, Level.INFO, cmd);
	}
	
	public static void printRedisCmd(String cmd) {
		printRedisCmd(LOG, cmd);
	}
	
	public static void printRedisCmd(Log log, Level level, String cmd) {
		int levelCode = LOG_LEVEL_MAP.get(level);
		String message = CMD_PREFIX + cmd;
		
		switch(levelCode) {
			case Level.DEBUG_INT:
				log.debug(message);
				break;
				
			case Level.INFO_INT:
				log.info(message);
				break;
				
			case Level.WARN_INT:
				log.warn(message);
				break;
				
			case Level.ERROR_INT:
				log.error(message);
				break;
				
			case Level.FATAL_INT:
				log.fatal(message);
				break;
				
			default:
				log.debug(message);
		}
	}
	
	/**
	 * Close Jeids connection quietly.
	 * @param connection
	 */
	public static void closeQuietly(Jedis connection) {
		try {
			if(connection != null) {
				connection.close();
			}
		} catch (Exception e) {}
	}

}
