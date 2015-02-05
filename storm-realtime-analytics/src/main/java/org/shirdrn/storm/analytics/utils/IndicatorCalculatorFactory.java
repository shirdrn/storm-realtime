package org.shirdrn.storm.analytics.utils;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.calculators.OpenAUCalculator;
import org.shirdrn.storm.analytics.calculators.OpenNUCalculator;
import org.shirdrn.storm.analytics.calculators.OpenTimesCalculator;
import org.shirdrn.storm.analytics.calculators.PlayAUCalculator;
import org.shirdrn.storm.analytics.calculators.PlayAUDurationCalculator;
import org.shirdrn.storm.analytics.calculators.PlayNUCalculator;
import org.shirdrn.storm.analytics.calculators.PlayNUDurationCalculator;
import org.shirdrn.storm.analytics.calculators.PlayTimesCalculator;
import org.shirdrn.storm.analytics.calculators.UserDeviceInfoCalculator;
import org.shirdrn.storm.analytics.calculators.UserDynamicInfoCalculator;
import org.shirdrn.storm.analytics.common.AbstractResult;
import org.shirdrn.storm.analytics.common.IndicatorCalculator;
import org.shirdrn.storm.commons.constants.StatIndicators;
import org.shirdrn.storm.commons.utils.ReflectionUtils;

import com.google.common.collect.Maps;

public class IndicatorCalculatorFactory {
	
	private static final Log LOG = LogFactory.getLog(IndicatorCalculatorFactory.class);
	private static final Map<Integer, IndicatorCalculator<? extends AbstractResult>> CALCULATORS = Maps.newHashMap();
	
	static {
		// register basic calculators
		registerCalculator(StatIndicators.USER_DEVICE_INFO, 	UserDeviceInfoCalculator.class);
		registerCalculator(StatIndicators.USER_DYNAMIC_INFO, 	UserDynamicInfoCalculator.class);
		
		// register statistical calculators
		registerCalculator(StatIndicators.OPEN_AU, 				OpenAUCalculator.class);
		registerCalculator(StatIndicators.OPEN_NU, 				OpenNUCalculator.class);
		registerCalculator(StatIndicators.OPEN_TIMES, 			OpenTimesCalculator.class);
		registerCalculator(StatIndicators.PLAY_AU, 				PlayAUCalculator.class);
		registerCalculator(StatIndicators.PLAY_AU_DURATION, 	PlayAUDurationCalculator.class);
		registerCalculator(StatIndicators.PLAY_NU, 				PlayNUCalculator.class);
		registerCalculator(StatIndicators.PLAY_NU_DURATION, 	PlayNUDurationCalculator.class);
		registerCalculator(StatIndicators.PLAY_TIMES, 			PlayTimesCalculator.class);
	}
	
	private IndicatorCalculatorFactory() {
		super();
	}
	
	@SuppressWarnings("unchecked")
	public static synchronized void registerCalculator(int indicator, Class<?> calculatorClazz) {
		IndicatorCalculator<? extends AbstractResult> calculator = CALCULATORS.get(indicator);
		if(calculator == null) {
			IndicatorCalculator<? extends AbstractResult> instance = 
					(IndicatorCalculator<? extends AbstractResult>) ReflectionUtils.getInstance(calculatorClazz, new Object[] { indicator });
			if(instance == null) {
				throw new RuntimeException("Fail to reflect class: " + calculatorClazz.getName());
			}
			CALCULATORS.put(indicator, instance);
			LOG.info("Factory registered: " + String.format("%02d", indicator) + " <-> " + calculatorClazz.getName());
		}
	}

	public static IndicatorCalculator<? extends AbstractResult> getCalculator(int indicator) throws NoSuchElementException {
		IndicatorCalculator<? extends AbstractResult> calculator = CALCULATORS.get(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for: indicator = " + indicator);
		}
		return calculator;
	}
	
}
