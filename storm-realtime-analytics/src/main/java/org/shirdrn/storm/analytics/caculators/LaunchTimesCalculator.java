package org.shirdrn.storm.analytics.caculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.analytics.common.LazyCallback;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.analytics.utils.IndicatorCalculatorUtils;
import org.shirdrn.storm.analytics.utils.RedisCmdUtils;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class LaunchTimesCalculator implements IndicatorCalculator<StatResult> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(LaunchTimesCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public StatResult caculate(Jedis jedis, JSONObject event, int indicator) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		String strHour = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_HOUR_PATTERN);
		// get user device information
		JSONObject user =  IndicatorCalculatorUtils.getUserInfo(jedis, udid);
		if(user != null) {
			String channel = user.getString(UserInfoKeys.CHANNEL);
			String version = user.getString(UserInfoKeys.VERSION);
			int osType = user.getInt(UserInfoKeys.DEVICE_TYPE);
			// shared field values
			statResult = new StatResult();
			statResult.setIndicator(indicator);
			statResult.setOsType(osType);
			statResult.setVersion(version);
			statResult.setChannel(channel);
			statResult.setStrHour(strHour);
			
			// set callback handler
			final StatResult result = statResult;
			statResult.setCallback(new LazyCallback<Jedis>() {

				@Override
				public void call(Jedis client) throws Exception {
					String key = result.getStrHour();
					String field = result.toField();
					long count = Constants.DEFAULT_INCREMENT_VALUE;
					client.hincrBy(key, field, count);
					RedisCmdUtils.printCmd(LOG, "HINCRBY " + key + " " + field + " " + count);
				}
				
			});
		}
		return statResult;
	}
	
}
