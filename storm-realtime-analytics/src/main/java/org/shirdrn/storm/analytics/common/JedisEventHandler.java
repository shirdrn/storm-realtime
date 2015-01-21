package org.shirdrn.storm.analytics.common;

import java.util.NoSuchElementException;
import java.util.TreeSet;

import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;


public abstract class JedisEventHandler<R, E> implements EventHandler<R, E> {

	private static final long serialVersionUID = 1L;
	protected final String eventCode;
	protected final JedisRichBolt jedisBolt;
	
	public JedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		this.jedisBolt = jedisBolt;
		this.eventCode = eventCode;
	}
	
	protected void compute(TreeSet<AbstractResult> results, int indicator, JSONObject event) {
		Jedis jedis = jedisBolt.getJedis();
		IndicatorCalculator<? extends AbstractResult> calculator = select(indicator);
		if(calculator == null) {
			throw new NoSuchElementException("Not found calculator for indicator: " + indicator);
		}
		AbstractResult result = calculator.caculate(jedis, event, indicator);
		if(result != null) {
			results.add(result);
		}
		jedisBolt.returnResource(jedis);
	}
	
	protected abstract IndicatorCalculator<? extends AbstractResult> select(int indicator) throws NoSuchElementException;
	
}
