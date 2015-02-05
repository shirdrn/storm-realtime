package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.AbstractJedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class InstallEventHandler extends AbstractJedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public InstallEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.USER_DEVICE_INFO);		
	}

}
