package org.shirdrn.storm.analytics.common;

import java.io.Serializable;

import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;

public interface IndicatorCalculator<R> extends Serializable {

	R caculate(Jedis jedis, JSONObject e, int indicator);
}
