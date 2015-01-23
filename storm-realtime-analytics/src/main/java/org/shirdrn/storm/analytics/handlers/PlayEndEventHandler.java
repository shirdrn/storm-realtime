package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.constants.StatIndicators;
import org.shirdrn.storm.analytics.utils.IndicatorCalculatorFactory;

import com.google.common.collect.Sets;

public class PlayEndEventHandler extends JedisEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayEndEventHandler.class);
	
	public PlayEndEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// indicator -> calculator
		mapTo(StatIndicators.PLAY_NU_DURATION, IndicatorCalculatorFactory.getPlayNUDurationCalculator());
		mapTo(StatIndicators.PLAY_AU_DURATION, IndicatorCalculatorFactory.getPlayAUDurationCalculator());
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		LOG.info(this.getClass().getSimpleName() + ": indicators=" + indicators);
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : indicators) {
			switch(indicator) {
				case StatIndicators.PLAY_NU_DURATION:
					// compute new user play duration
					super.compute(results, indicator, event);
					break;
				case StatIndicators.PLAY_AU_DURATION:
					// compute active user play duration
					super.compute(results, indicator, event);
					break;
			}
		}
		LOG.info(this.getClass().getSimpleName() + ": results=" + results);
		return results;
	}

}
