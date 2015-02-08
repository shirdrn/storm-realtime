package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.GenericIndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

import redis.clients.jedis.Jedis;

public class UserDeviceInfoCalculator extends GenericIndicatorCalculator<KeyedResult<JSONObject>, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDeviceInfoCalculator.class);
	
	public UserDeviceInfoCalculator() {
		super(StatIndicators.USER_DEVICE_INFO);
	}
	
	@SuppressWarnings("serial")
	@Override
	public KeyedResult<JSONObject> calculate(final Jedis connection, JSONObject event) {
		// install event
		String udid = event.getString(EventFields.UDID);
		
		JSONObject user = new JSONObject();
		String appId = event.getString(EventFields.APP_KEY);
		user.put(UserInfoKeys.APP_ID, appId);
		String channel = event.getString(EventFields.CHANNEL);
		user.put(UserInfoKeys.CHANNEL, channel);
		String version = event.getString(EventFields.VERSION);
		user.put(UserInfoKeys.VERSION, version);
		int osType = event.getInt(EventFields.OS_TYPE);
		user.put(UserInfoKeys.OS_TYPE, osType);
		
		String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		
		KeyedResult<JSONObject> keyedObj = new KeyedResult<JSONObject>();
		keyedObj.setIndicator(indicator);
		keyedObj.setKey(userKey);
		keyedObj.setData(user);
		
		// set callback handler for lazy computation
		final KeyedResult<JSONObject> result =  keyedObj;
		keyedObj.setCallbackHandler(new CallbackHandler<Jedis>() {

			@Override
			public void callback(final Jedis client) throws Exception {
				String key = result.getKey();
				String value = result.getData().toString();
				client.set(key, value);
				logRedisCmd(LOG, "SET " + key + " " + value);
			}
			
		});
		
		return keyedObj;
	}
	
}
