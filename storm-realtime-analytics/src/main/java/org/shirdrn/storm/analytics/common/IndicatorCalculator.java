package org.shirdrn.storm.analytics.common;

import java.io.Serializable;

import net.sf.json.JSONObject;

import org.apache.log4j.Level;

import redis.clients.jedis.Jedis;

public interface IndicatorCalculator<R> extends Serializable {

	int getIndicator();
	R calculate(final Jedis jedis, JSONObject event);
	void setPrintRedisCmdLogLevel(Level logLevel);
}
