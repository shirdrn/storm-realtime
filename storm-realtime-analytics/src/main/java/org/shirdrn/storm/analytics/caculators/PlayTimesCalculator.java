package org.shirdrn.storm.analytics.caculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractIndicatorCalculator;
import org.shirdrn.storm.analytics.common.CallbackHandler;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.analytics.utils.EventUtils;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class PlayTimesCalculator extends AbstractIndicatorCalculator<StatResult> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayTimesCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public StatResult caculate(final Jedis jedis, JSONObject event, int indicator) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		String strHour = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_HOUR_PATTERN);
		// get user device information
		JSONObject user =  EventUtils.getUserInfo(jedis, udid);
		if(user != null) {
			String channel = user.getString(UserInfoKeys.CHANNEL);
			String version = user.getString(UserInfoKeys.VERSION);
			int osType = user.getInt(UserInfoKeys.OS_TYPE);
			// create StatResult
			statResult = new StatResult();
			statResult.setOsType(osType);
			statResult.setVersion(version);
			statResult.setChannel(channel);
			statResult.setStrHour(strHour);
			statResult.setIndicator(indicator);
			
			// set callback handler
			final StatResult result = statResult;
			statResult.setCallbackHandler(new CallbackHandler<Jedis>() {

				@Override
				public void call(final Jedis client) throws Exception {
					String key = result.createKey(CommonConstants.NS_STAT_HKEY);
					String field = result.toField();
					long count = Constants.DEFAULT_INCREMENT_VALUE;
					client.hincrBy(key, field, count);
					logRedisCmd(LOG, "HINCRBY " + key + " " + field + " " + count);
				}
				
			});
		}
		return statResult;
	}
	
}
