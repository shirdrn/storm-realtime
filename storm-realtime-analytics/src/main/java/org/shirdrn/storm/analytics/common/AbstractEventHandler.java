package org.shirdrn.storm.analytics.common;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.IndicatorCalculator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class AbstractEventHandler<R, C, E> implements EventHandler<TreeSet<R>, E> {

	private static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(AbstractEventHandler.class);
	protected final String eventCode;
	private final Collection<Integer> registeredIndicators = Sets.newTreeSet();
	private final Map<Integer, IndicatorCalculator<R, C, E>> registeredCalculators = Maps.newHashMap();
	
	public AbstractEventHandler(String eventCode) {
		super();
		this.eventCode = eventCode;
	}
	
	@Override
	public Collection<Integer> getMappedIndicators() {
		return registeredIndicators;
	}
	
	@Override
	public TreeSet<R> handle(E event) throws Exception {
		LOG.info(getClass().getSimpleName() + ": indicators=" + registeredIndicators);
		TreeSet<R> results = new TreeSet<R>();
		for(int indicator : registeredIndicators) {
			R result = compute(indicator, event);
			if(result != null) {
				results.add(result);
			}
		}
		LOG.info(getClass().getSimpleName() + ":  results=" + results);
		return results;
	}
	
	protected void registerIndicatorInternal(int indicator) {
		IndicatorCalculator<R, C, E> calculator = getIndicatorCalculator(indicator);
		registeredIndicators.add(indicator);
		registeredCalculators.put(indicator, calculator);
		LOG.info("Registered[" + this.getClass().getSimpleName() + "\t] " + eventCode + " -> " + String.format("%02d", indicator) + " -> " + calculator);
	}
	
	protected IndicatorCalculator<R, C, E> selectCalculator(int indicator) throws NoSuchElementException {
		return registeredCalculators.get(indicator);
	}
	
	protected abstract IndicatorCalculator<R, C, E> getIndicatorCalculator(int indicator);

	protected abstract R compute(int indicator, E event);
}