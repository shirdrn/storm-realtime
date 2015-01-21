package org.shirdrn.storm.analytics.utils;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.caculators.AUCalculator;
import org.shirdrn.storm.analytics.caculators.LaunchTimesCalculator;
import org.shirdrn.storm.analytics.caculators.NUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayAUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayAUDurationCalculator;
import org.shirdrn.storm.analytics.caculators.PlayNUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayNUDurationCalculator;
import org.shirdrn.storm.analytics.caculators.PlayTimesCalculator;
import org.shirdrn.storm.analytics.caculators.UserDeviceInfoCalculator;
import org.shirdrn.storm.analytics.caculators.UserDynamicInfoCalculator;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedObject;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class IndicatorCalculatorUtils {

	//// Statistical indicator calculators
	
	private static final IndicatorCalculator<StatResult> AU_CALCULATOR = new AUCalculator();
	private static final IndicatorCalculator<StatResult> NU_CALCULATOR = new NUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_AU_CALCULATOR = new PlayAUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_NU_CALCULATOR = new PlayNUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_TIMES_CALCULATOR = new PlayTimesCalculator();
	private static final IndicatorCalculator<StatResult> LAUNCH_TIMES_CALCULATOR = new LaunchTimesCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_NU_DURATION_CALCULATOR = new PlayNUDurationCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_AU_DURATION_CALCULATOR = new PlayAUDurationCalculator();
	
	public static IndicatorCalculator<StatResult> getAUCalculator() {
		return AU_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getNUCalculator() {
		return NU_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getPlayAUCalculator() {
		return PLAY_AU_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getPlayNUCalculator() {
		return PLAY_NU_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getPlayTimesCalculator() {
		return PLAY_TIMES_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getLaunchTimesCalculator() {
		return LAUNCH_TIMES_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getPlayNUDurationCalculator() {
		return PLAY_NU_DURATION_CALCULATOR;
	}

	public static IndicatorCalculator<StatResult> getPlayAUDurationCalculator() {
		return PLAY_AU_DURATION_CALCULATOR;
	}
	
	
	//// Other calculators
	
	private static final IndicatorCalculator<KeyedObject<JSONObject>> USER_DEVICE_INFO_CALCULATOR = new UserDeviceInfoCalculator();
	private static final IndicatorCalculator<KeyedObject<JSONObject>> USER_DYNAMIC_INFO_CALCULATOR = new UserDynamicInfoCalculator();
	
	public static final IndicatorCalculator<KeyedObject<JSONObject>> getUserDeviceInfoCalculator() {
		return USER_DEVICE_INFO_CALCULATOR;
	}
	
	public static IndicatorCalculator<KeyedObject<JSONObject>> getUserDynamicInfoCalculator() {
		return USER_DYNAMIC_INFO_CALCULATOR;
	}
	
	
	//// Utilities for computing some metrics
	
	public static JSONObject getUserInfo(Jedis jedis, String udid) {
		JSONObject user = null;
		String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		String userInfo = jedis.get(userKey);
		if(userInfo != null) {
			user = JSONObject.fromObject(userInfo);
		}
		return user;
	}
	
	public static boolean isNewUserOpen(Jedis jedis, String udid, JSONObject user, String eventDatetime) {
		String key = Constants.USER_DYNAMIC_INFO_KEY_PREFIX + udid;
		String firstOpenDate = jedis.hget(key, Constants.FIRST_OPEN_DATE);
		boolean isNewUserOpen = true;
		if(firstOpenDate != null) {
			String eventDate = DateTimeUtils.format(eventDatetime, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
			long days = DateTimeUtils.getDaysBetween(firstOpenDate, eventDate, Constants.DT_DATE_PATTERN);
			if(days > 180) {
				isNewUserOpen = false;
			}
		}
		return isNewUserOpen;
	}
	
	public static boolean isNewUserPlay(Jedis jedis, String udid, JSONObject user, String eventDatetime) {
		String key = Constants.USER_DYNAMIC_INFO_KEY_PREFIX + udid;
		String firstPlayDate = jedis.hget(key, Constants.FIRST_PLAY_DATE);
		boolean isNewUserPlay = true;
		if(firstPlayDate != null) {
			String eventDate = DateTimeUtils.format(eventDatetime, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
			long days = DateTimeUtils.getDaysBetween(firstPlayDate, eventDate, Constants.DT_DATE_PATTERN);
			if(days > 180) {
				isNewUserPlay = false;
			}
		}
		return isNewUserPlay;
	}
}
