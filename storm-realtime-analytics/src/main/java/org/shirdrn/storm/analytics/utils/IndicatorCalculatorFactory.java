package org.shirdrn.storm.analytics.utils;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.caculators.OpenAUCalculator;
import org.shirdrn.storm.analytics.caculators.OpenTimesCalculator;
import org.shirdrn.storm.analytics.caculators.OpenNUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayAUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayAUDurationCalculator;
import org.shirdrn.storm.analytics.caculators.PlayNUCalculator;
import org.shirdrn.storm.analytics.caculators.PlayNUDurationCalculator;
import org.shirdrn.storm.analytics.caculators.PlayTimesCalculator;
import org.shirdrn.storm.analytics.caculators.UserDeviceInfoCalculator;
import org.shirdrn.storm.analytics.caculators.UserDynamicInfoCalculator;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.common.StatResult;

public class IndicatorCalculatorFactory {

	//// Statistical indicator calculators
	
	private static final IndicatorCalculator<StatResult> AU_CALCULATOR = new OpenAUCalculator();
	private static final IndicatorCalculator<StatResult> NU_CALCULATOR = new OpenNUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_AU_CALCULATOR = new PlayAUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_NU_CALCULATOR = new PlayNUCalculator();
	private static final IndicatorCalculator<StatResult> PLAY_TIMES_CALCULATOR = new PlayTimesCalculator();
	private static final IndicatorCalculator<StatResult> OPEN_TIMES_CALCULATOR = new OpenTimesCalculator();
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
	
	public static IndicatorCalculator<StatResult> getOPenTimesCalculator() {
		return OPEN_TIMES_CALCULATOR;
	}
	
	public static IndicatorCalculator<StatResult> getPlayNUDurationCalculator() {
		return PLAY_NU_DURATION_CALCULATOR;
	}

	public static IndicatorCalculator<StatResult> getPlayAUDurationCalculator() {
		return PLAY_AU_DURATION_CALCULATOR;
	}
	
	
	//// Other calculators
	
	private static final IndicatorCalculator<KeyedResult<JSONObject>> USER_DEVICE_INFO_CALCULATOR = new UserDeviceInfoCalculator();
	private static final IndicatorCalculator<KeyedResult<JSONObject>> USER_DYNAMIC_INFO_CALCULATOR = new UserDynamicInfoCalculator();
	
	public static final IndicatorCalculator<KeyedResult<JSONObject>> getUserDeviceInfoCalculator() {
		return USER_DEVICE_INFO_CALCULATOR;
	}
	
	public static IndicatorCalculator<KeyedResult<JSONObject>> getUserDynamicInfoCalculator() {
		return USER_DYNAMIC_INFO_CALCULATOR;
	}
	
}
