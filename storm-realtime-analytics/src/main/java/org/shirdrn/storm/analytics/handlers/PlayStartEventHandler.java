package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.commons.constants.StatIndicators;

import com.google.common.collect.Sets;

public class PlayStartEventHandler extends JedisEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayStartEventHandler.class);
	
	public PlayStartEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// register indicators
		registerIndicator(StatIndicators.USER_DYNAMIC_INFO); 
		registerIndicator(StatIndicators.PLAY_NU);
		registerIndicator(StatIndicators.PLAY_AU);
		registerIndicator(StatIndicators.PLAY_TIMES);
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		LOG.info(this.getClass().getSimpleName() + ": indicators=" + indicators);
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : indicators) {
			switch(indicator) {
				case StatIndicators.USER_DYNAMIC_INFO:
					// compute user first play date
					super.compute(results, indicator, event);
					break;
					
				case StatIndicators.PLAY_NU:
					// compute play new users
					super.compute(results, indicator, event);
					break;
					
				case StatIndicators.PLAY_AU:
					// compute play active users
					super.compute(results, indicator, event);
					break;
					
				case StatIndicators.PLAY_TIMES:
					// compute play times
					super.compute(results, indicator, event);
					break;
			}
		}
		LOG.info(this.getClass().getSimpleName() + ": results=" + results);
		return results;
	}
	
}
