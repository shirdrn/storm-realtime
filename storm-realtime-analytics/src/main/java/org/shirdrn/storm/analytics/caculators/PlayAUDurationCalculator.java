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
import org.shirdrn.storm.analytics.utils.EventUtils;
import org.shirdrn.storm.analytics.utils.RedisCmdUtils;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class PlayAUDurationCalculator implements IndicatorCalculator<StatResult> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayAUDurationCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public StatResult caculate(Jedis jedis, JSONObject event, int indicator) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		final int duration = event.getInt(EventFields.PLAY_DURATION);
		if(duration > 0) {
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
				statResult.setCallback(new LazyCallback<Jedis>() {

					@Override
					public void call(final Jedis client) throws Exception {
						String key = result.createKey(Constants.NS_STAT_HKEY);
						String userKey = result.createKey(Constants.NS_PLAY_AU_DURATION_USER);
						String field = result.toField();
						// save new users for play AU
						// like: <key, field, value>
						// <2311010202::32::AU, 0::A-360::3.1.2::AAAAAAAAAADDDDDDDDD, Y>
						String userField = field + Constants.REDIS_KEY_NS_SEPARATOR  + udid;
						String userValue = client.hget(key, userField);
						if(userValue == null) {
							userValue = Constants.CACHE_ITEM_KEYD_VALUE;
							Transaction tx = client.multi();
							tx.hset(userKey, userField, userValue);
							tx.hincrBy(key, field, duration);
							tx.exec();
							RedisCmdUtils.printCmd(LOG, "HSET " + userKey + " " + userField + " " + userValue);
							RedisCmdUtils.printCmd(LOG, "HINCRBY " + key + " " + field + " " + duration);
						} else {
							client.hincrBy(key, field, duration);
							RedisCmdUtils.printCmd(LOG, "HINCRBY " + key + " " + field + " " + duration);
						}
					}
					
				});
			}
		}
		return statResult;
	}
	
}
