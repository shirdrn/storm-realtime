package org.shirdrn.storm.analytics.handlers;

import java.util.Collection;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.commons.constants.StatIndicators;

import com.google.common.collect.Sets;

public class InstallEventHandler extends JedisEventHandler<TreeSet<AbstractResult>, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(InstallEventHandler.class);
	
	public InstallEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
		// register indicators
		registerIndicator(StatIndicators.USER_DEVICE_INFO);
	}

	@Override
	public TreeSet<AbstractResult> handle(JSONObject event, Collection<Integer> indicators) throws Exception {
		LOG.info(this.getClass().getSimpleName() + ": indicators=" + indicators);
		TreeSet<AbstractResult> results = Sets.newTreeSet();
		for(int indicator : indicators) {
			KeyedResult<JSONObject> userInfo = null;
			switch(indicator) {
				case StatIndicators.USER_DEVICE_INFO:
					// compute and cache user device information
					super.compute(results, indicator, event);
					LOG.debug("userInfo: " + userInfo);
					break;
			}
		}
		LOG.info(this.getClass().getSimpleName() + ": results=" + results);
		return results;
	}

}
