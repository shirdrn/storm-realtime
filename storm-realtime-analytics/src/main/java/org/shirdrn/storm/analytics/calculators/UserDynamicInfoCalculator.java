package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.AbstractIndicatorCalculator;
import org.shirdrn.storm.analytics.common.CallbackHandler;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class UserDynamicInfoCalculator extends AbstractIndicatorCalculator<KeyedResult<JSONObject>> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDynamicInfoCalculator.class);
	
	@SuppressWarnings("serial")
	@Override
	public KeyedResult<JSONObject> caculate(final Jedis jedis, JSONObject event, int indicator) {
		final String eventCode = event.getString(EventFields.EVENT_CODE);
		String udid = event.getString(EventFields.UDID);
		final String key = Constants.USER_BEHAVIOR_KEY + CommonConstants.REDIS_KEY_NS_SEPARATOR + udid;
		String time = event.getString(EventFields.EVENT_TIME);
		final String strDate = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
		KeyedResult<JSONObject> keyedObj = new KeyedResult<JSONObject>();
		keyedObj.setKey(key);
		keyedObj.setIndicator(indicator);
		
		// set callback handler for lazy computation
		final KeyedResult<JSONObject> result = keyedObj;
		keyedObj.setCallbackHandler(new CallbackHandler<Jedis>() {

			@Override
			public void call(final Jedis client) throws Exception {
				JSONObject info = null;
				String field = null;
				// first open date
				if(eventCode.equals(EventCode.OPEN)) {
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
					result.setData(info);
					String value = info.getString(field);
					client.hset(key, field, value);
					logRedisCmd(LOG, "HSET " + key + " " + field + " " + value);
				}
			}
			
		});
		
		return keyedObj;
	}
	
}
