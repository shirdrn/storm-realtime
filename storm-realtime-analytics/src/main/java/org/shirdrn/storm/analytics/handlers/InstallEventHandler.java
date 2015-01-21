package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.common.KeyedObject;
import org.shirdrn.storm.analytics.common.MappedEventHandler;
import org.shirdrn.storm.analytics.constants.StatIndicators;
import org.shirdrn.storm.analytics.utils.IndicatorCalculatorUtils;

import com.google.common.collect.Sets;

public class InstallEventHandler extends MappedEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(InstallEventHandler.class);
	
	public InstallEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// indicator -> calculator
		mapTo(StatIndicators.USER_DEVICE_INFO, IndicatorCalculatorUtils.getUserDeviceInfoCalculator());
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		TreeSet<AbstractResult> userInfos = Sets.newTreeSet();
		for(int indicator : indicators) {
			KeyedObject<JSONObject> userInfo = null;
			switch(indicator) {
				case StatIndicators.USER_DEVICE_INFO:
					// compute and cache user device information
					super.compute(userInfos, indicator, event);
					LOG.debug("userInfo: " + userInfo);
					break;
			}
		}
		return userInfos;
	}

}
