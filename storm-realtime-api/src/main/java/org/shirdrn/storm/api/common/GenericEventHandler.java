package org.shirdrn.storm.api.common;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.api.Result;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Generic event handler. It manages the relations between event and
 * {@link IndicatorCalculator}, that is say, mapping the event code to a set
 * of indicators belonging to the event.
 * 
 * @author Yanjun
 *
 * @param <R> Computed {@link Result}
 * @param <C> Storage engine connection
 * @param <E> Event data object
 */
public abstract class GenericEventHandler<R, C, E> implements EventHandler<TreeSet<R>, C, E> {

	private static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(GenericEventHandler.class);
	private final String eventCode;
	private final Collection<Integer> registeredIndicators = Sets.newTreeSet();
	private final Map<Integer, IndicatorCalculator<R, C, E>> registeredCalculators = Maps.newHashMap();
	
	public GenericEventHandler(String eventCode) {
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
			R result = processEvent(indicator, event);
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
	
	/**
	 * Get a {@link IndicatorCalculator} object form the given indicator. Usually the {@link IndicatorCalculator}
	 * instance should be a singleton object.
	 * @param indicator
	 * @return
	 */
	protected abstract IndicatorCalculator<R, C, E> getIndicatorCalculator(int indicator);

	/**
	 *  Process a event for a known indicator.
	 * @param indicator
	 * @param event
	 * @return
	 */
	protected abstract R processEvent(int indicator, E event);
	
	@Override
	public String getEventCode() {
		return eventCode;
	}
}
