package org.shirdrn.storm.analytics.caculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedObject;
import org.shirdrn.storm.analytics.common.LazyCallback;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.utils.RedisCmdUtils;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class UserDynamicInfoCalculator implements IndicatorCalculator<KeyedObject<JSONObject>> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDynamicInfoCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public KeyedObject<JSONObject> caculate(Jedis jedis, JSONObject event, int indicator) {
		final String eventCode = event.getString(EventFields.EVENT_CODE);
		String udid = event.getString(EventFields.UDID);
		final String key = Constants.USER_BEHAVIOR_KEY + Constants.REDIS_KEY_NS_SEPARATOR + udid;
		String time = event.getString(EventFields.EVENT_TIME);
		final String strDate = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
		KeyedObject<JSONObject> keyedObj = new KeyedObject<JSONObject>();
		keyedObj.setKey(key);
		keyedObj.setIndicator(indicator);
		
		// set callback handler for lazy computation
		final KeyedObject<JSONObject> result = keyedObj;
		keyedObj.setCallback(new LazyCallback<Jedis>() {

			@Override
			public void call(Jedis client) throws Exception {
				JSONObject info = null;
				String field = null;
				// first open date
				if(eventCode.equals(EventCode.LAUNCH)) {
					field = Constants.FIRST_OPEN_DATE; 
					String firstOpenDate = client.hget(key, Constants.FIRST_OPEN_DATE);
					if(firstOpenDate == null) {
						info = new JSONObject();
						info.put(Constants.FIRST_OPEN_DATE, strDate);
					}
				}
				// first play date
				if(eventCode.equals(EventCode.PLAY_START)) {
					field = Constants.FIRST_PLAY_DATE; 
					String firstOpenDate = client.hget(key, Constants.FIRST_PLAY_DATE);
					if(firstOpenDate == null) {
						info = new JSONObject();
						info.put(Constants.FIRST_PLAY_DATE, strDate);
					}
				}
				
				if(info != null) {
					result.setObject(info);
					String value = info.getString(field);
					client.hset(key, field, value);
					RedisCmdUtils.printCmd(LOG, "HSET " + key + " " + field + " " + value);
				}
			}
			
		});
		
		return keyedObj;
	}
	
}
