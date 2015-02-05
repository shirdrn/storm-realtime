package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.AbstractJedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class PlayEndEventHandler extends AbstractJedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public PlayEndEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.PLAY_NU_DURATION);
		registerIndicatorInternal(StatIndicators.PLAY_AU_DURATION);
	}

}
