package org.shirdrn.storm.analytics.common;

import java.util.NoSuchElementException;

import net.sf.json.JSONObject;

import org.shirdrn.storm.api.GenericEventHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.api.utils.IndicatorCalculatorFactory;

import redis.clients.jedis.Jedis;

public abstract class JedisEventHandler extends GenericEventHandler<Result, Jedis, JSONObject> {

	private static final long serialVersionUID = 1L;
	protected final JedisRichBolt jedisBolt;
	
	public JedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(eventCode);
		this.jedisBolt = jedisBolt;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected IndicatorCalculator<Result, Jedis, JSONObject> getIndicatorCalculator(int indicator) {
		return (IndicatorCalculator<Result, Jedis, JSONObject>) 
				IndicatorCalculatorFactory.newIndicatorCalculator(indicator);
	}
	
	@Override
	protected Result processEvent(int indicator, JSONObject event) {
		Jedis jedis = jedisBolt.getConnection();
		IndicatorCalculator<Result, Jedis, JSONObject> calculator = selectCalculator(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		// set writing Redis command log level
		if(calculator instanceof Loggingable) {
			((Loggingable) calculator).setPrintRedisCmdLogLevel(jedisBolt.getRedisCmdLogLevel());
		}
		Result result = calculator.calculate(jedis, event);
		jedisBolt.releaseConnection(jedis);
		return result;
	}
	
}
