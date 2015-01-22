package org.shirdrn.storm.analytics.utils;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

/**
 * Utilities for computing some metrics related to users and events.
 * 
 * @author yanjun
 */
public class EventUtils {

	public static JSONObject getUserInfo(final Jedis jedis, String udid) {
		JSONObject user = null;
		String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		String userInfo = jedis.get(userKey);
		if(userInfo != null) {
			user = JSONObject.fromObject(userInfo);
		}
		return user;
	}
	
	public static boolean isNewUserOpen(final Jedis jedis, String udid, final JSONObject user, String eventDatetime) {
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
	
	public static boolean isNewUserPlay(final Jedis jedis, String udid, final JSONObject user, String eventDatetime) {
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
