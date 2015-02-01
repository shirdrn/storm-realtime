package org.shirdrn.storm.analytics.common;

import java.util.NoSuchElementException;
import java.util.TreeSet;

import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;

public abstract class JedisEventHandler<R, E> extends MappedEventHandler<R, E> {

	private static final long serialVersionUID = 1L;
	protected final JedisRichBolt jedisBolt;
	
	public JedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(eventCode);
		this.jedisBolt = jedisBolt;
	}
	
	protected void compute(TreeSet<AbstractResult> results, int indicator, JSONObject event) {
		Jedis jedis = jedisBolt.getJedis();
		IndicatorCalculator<? extends AbstractResult> calculator = selectCalculator(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		calculator.setPrintRedisCmdLogLevel(jedisBolt.getRedisCmdLogLevel());
		AbstractResult result = calculator.calculate(jedis, event, indicator);
		if(result != null) {
			results.add(result);
		}
		jedisBolt.returnResource(jedis);
	}
	
}
