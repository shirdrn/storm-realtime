package org.shirdrn.storm.analytics.utils;

import org.apache.commons.logging.Log;

public class RedisCmdUtils {

	private static final String CMD_PREFIX = "COMMAMD => ";
	
	public static void printCmd(Log log, String cmd) {
		log.info(CMD_PREFIX + cmd);
	}
}
