package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.common.StatResult;

import redis.clients.jedis.Jedis;

public class PlayAUCalculator extends OpenAUCalculator {

	private static final long serialVersionUID = 1L;

	@Override
	public StatResult calculate(final Jedis jedis, JSONObject event, int indicator) {
		StatResult statResult = super.calculate(jedis, event, indicator);
		if(statResult != null) {
			statResult.setIndicator(indicator);
		}
		return statResult;
	}
	
}
