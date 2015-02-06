package org.shirdrn.storm.analytics.common;

import java.util.NoSuchElementException;

import net.sf.json.JSONObject;

import org.shirdrn.storm.api.AbstractEventHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.api.utils.IndicatorCalculatorFactory;

import redis.clients.jedis.Jedis;

public abstract class JedisEventHandler extends AbstractEventHandler<Result, Jedis, JSONObject> {

	private static final long serialVersionUID = 1L;
	protected final String eventCode;
	protected final JedisRichBolt jedisBolt;
	
	public JedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(eventCode);
		this.jedisBolt = jedisBolt;
		this.eventCode = eventCode;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected IndicatorCalculator<Result, Jedis, JSONObject> getIndicatorCalculator(int indicator) {
		return (IndicatorCalculator<Result, Jedis, JSONObject>) 
				IndicatorCalculatorFactory.newIndicatorCalculator(indicator);
	}
	
	@Override
	protected Result compute(int indicator, JSONObject event) {
		Jedis jedis = jedisBolt.getJedis();
		IndicatorCalculator<Result, Jedis, JSONObject> calculator = selectCalculator(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		if(calculator instanceof Loggingable) {
			((Loggingable) calculator).setPrintRedisCmdLogLevel(jedisBolt.getRedisCmdLogLevel());
		}
		Result result = calculator.calculate(jedis, event);
		jedisBolt.returnResource(jedis);
		return result;
	}
	
}
