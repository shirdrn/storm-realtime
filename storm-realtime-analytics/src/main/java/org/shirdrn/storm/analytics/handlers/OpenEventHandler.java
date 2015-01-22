package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.common.MappedEventHandler;
import org.shirdrn.storm.analytics.constants.StatIndicators;
import org.shirdrn.storm.analytics.utils.IndicatorCalculatorUtils;

import com.google.common.collect.Sets;

public class OpenEventHandler extends MappedEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(OpenEventHandler.class);
	
	public OpenEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// indicator -> calculator
		mapTo(StatIndicators.USER_DYNAMIC_INFO, IndicatorCalculatorUtils.getUserDynamicInfoCalculator());
		mapTo(StatIndicators.NU, IndicatorCalculatorUtils.getNUCalculator());
		mapTo(StatIndicators.AU, IndicatorCalculatorUtils.getAUCalculator());
		mapTo(StatIndicators.LAUNCH_TIMES, IndicatorCalculatorUtils.getOPenTimesCalculator());
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		LOG.info(this.getClass().getSimpleName() + ": indicators=" + indicators);
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : indicators) {
			switch(indicator) {
				case StatIndicators.NU:
					super.compute(results, indicator, event);
					break;
					
				case StatIndicators.USER_DYNAMIC_INFO:
					// compute user first open date
					super.compute(results, indicator, event);
					break;
					
				case StatIndicators.LAUNCH_TIMES:
					// compute active users
					super.compute(results, indicator, event);
					break;
			}
		}
		LOG.info(this.getClass().getSimpleName() + ": results=" + results);
		return results;
	}

}
