package org.shirdrn.storm.analytics.utils;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class RealtimeUtils {

	private static final Log LOG = LogFactory.getLog(RealtimeUtils.class);
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
		switch(levelCode) {
			case Level.DEBUG_INT:
				log.debug(CMD_PREFIX + cmd);
				break;
			case Level.INFO_INT:
				log.info(CMD_PREFIX + cmd);
				break;
			case Level.WARN_INT:
				log.warn(CMD_PREFIX + cmd);
				break;
			case Level.ERROR_INT:
				log.error(CMD_PREFIX + cmd);
				break;
			case Level.FATAL_INT:
				log.fatal(CMD_PREFIX + cmd);
				break;
		}
	}
	
	public static int getDefaultExpireTime() {
		return Constants.CACHE_ITEM_EXPIRE_TIME;
	}
	
	public static int getExpireTime() {
		return DateTimeUtils.getDiffToNearestHour();
	}
	
	public static int getExpireTime(long timestamp) {
		return DateTimeUtils.getDiffToNearestHour(timestamp);
	}
}
