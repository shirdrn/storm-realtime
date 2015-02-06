package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.commons.constants.StatIndicators;

import redis.clients.jedis.Jedis;

public class PlayNUCalculator extends OpenNUCalculator {
	
	private static final long serialVersionUID = 1L;
	
	public PlayNUCalculator() {
		super(StatIndicators.PLAY_NU);
	}

	@Override
	public StatResult calculate(final Jedis jedis, JSONObject event) {
		StatResult statResult = super.calculate(jedis, event);
		if(statResult != null) {
			statResult.setIndicator(indicator);
		}
		return statResult;
	}
	
}
