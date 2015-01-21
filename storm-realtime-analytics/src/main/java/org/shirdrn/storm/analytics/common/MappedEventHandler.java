package org.shirdrn.storm.analytics.common;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class MappedEventHandler<R, E> extends JedisEventHandler<R, E> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(MappedEventHandler.class);
	private final Collection<Integer> registeredIndicators = Sets.newTreeSet();
	private final Map<Integer, IndicatorCalculator<? extends AbstractResult>> registeredCalculators = Maps.newHashMap();
	
	public MappedEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(jedisBolt, eventCode);
	}
	
	@Override
	public Collection<Integer> getMappedIndicators() {
		return registeredIndicators;
	}
	
	protected void mapTo(int indicator, IndicatorCalculator<? extends AbstractResult> calculator) {
		registeredIndicators.add(indicator);
		registeredCalculators.put(indicator, calculator);
		LOG.info("Mapped[" + this.getClass().getSimpleName() + "\t] " + eventCode + " -> " + String.format("%02d", indicator) + " -> " + calculator);
	}
	
	@Override
	protected IndicatorCalculator<? extends AbstractResult> select(int indicator) throws NoSuchElementException {
		return registeredCalculators.get(indicator);
	}

}
