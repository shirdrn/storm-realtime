package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.common.MappedEventHandler;
import org.shirdrn.storm.analytics.constants.StatIndicators;
import org.shirdrn.storm.analytics.utils.IndicatorCalculatorUtils;

import com.google.common.collect.Sets;

public class PlayEndEventHandler extends MappedEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	
	public PlayEndEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// indicator -> calculator
		mapTo(StatIndicators.NEW_PLAY_DURATION, IndicatorCalculatorUtils.getPlayNUDurationCalculator());
		mapTo(StatIndicators.ACTIVE_PLAY_DURATION, IndicatorCalculatorUtils.getPlayAUDurationCalculator());
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : indicators) {
			switch(indicator) {
				case StatIndicators.NEW_PLAY_DURATION:
					// compute new user play duration
					super.compute(results, indicator, event);
					break;
				case StatIndicators.ACTIVE_PLAY_DURATION:
					// compute active user play duration
					super.compute(results, indicator, event);
					break;
			}
		}
		return results;
	}

}
