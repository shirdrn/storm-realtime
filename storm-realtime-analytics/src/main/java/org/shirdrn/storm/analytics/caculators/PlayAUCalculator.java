package org.shirdrn.storm.analytics.caculators;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.common.StatResult;

import redis.clients.jedis.Jedis;

public class PlayAUCalculator extends AUCalculator {

	private static final long serialVersionUID = 1L;

	@Override
	public StatResult caculate(final Jedis jedis, JSONObject event, int indicator) {
		StatResult statResult = super.caculate(jedis, event, indicator);
		if(statResult != null) {
			statResult.setIndicator(indicator);
		}
		return statResult;
	}
	
}
