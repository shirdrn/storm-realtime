package org.shirdrn.storm.analytics.caculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.common.LazyCallback;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.StatIndicators;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.analytics.utils.RedisCmdUtils;

import redis.clients.jedis.Jedis;

public class UserDeviceInfoCalculator implements IndicatorCalculator<KeyedResult<JSONObject>> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDeviceInfoCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public KeyedResult<JSONObject> caculate(final Jedis jedis, JSONObject event, int indicator) {
		// install event
		String udid = event.getString(EventFields.UDID);
		
		JSONObject user = new JSONObject();
		String appId = event.getString(EventFields.APP_KEY);
		user.put(UserInfoKeys.APP_ID, appId);
		String channel = event.getString(EventFields.CHANNEL);
		user.put(UserInfoKeys.CHANNEL, channel);
		String version = event.getString(EventFields.VERSION);
		user.put(UserInfoKeys.VERSION, version);
		int deviceType = event.getInt(EventFields.DEVICE_TYPE);
		user.put(UserInfoKeys.DEVICE_TYPE, deviceType);
		
		String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		
		KeyedResult<JSONObject> keyedObj = new KeyedResult<JSONObject>();
		keyedObj.setIndicator(StatIndicators.USER_DEVICE_INFO);
		keyedObj.setKey(userKey);
		keyedObj.setData(user);
		
		// set callback handler for lazy computation
		final KeyedResult<JSONObject> result =  keyedObj;
		keyedObj.setCallback(new LazyCallback<Jedis>() {

			@Override
			public void call(final Jedis client) throws Exception {
				String key = result.getKey();
				String value = result.getData().toString();
				client.set(key, value);
				RedisCmdUtils.printCmd(LOG, "SET " + key + " " + value);
			}
			
		});
		
		return keyedObj;
	}
	
}
