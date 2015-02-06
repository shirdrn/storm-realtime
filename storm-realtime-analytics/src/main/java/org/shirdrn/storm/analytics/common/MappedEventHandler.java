//package org.shirdrn.storm.analytics.common;
//
//import java.util.Collection;
//import java.util.Map;
//import java.util.NoSuchElementException;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.shirdrn.storm.analytics.utils.IndicatorCalculatorFactory;
//
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//
//public abstract class MappedEventHandler<R, E> implements EventHandler<R, E> {
//
//	private static final long serialVersionUID = 1L;
//	private static final Log LOG = LogFactory.getLog(MappedEventHandler.class);
//	protected final String eventCode;
//	private final Collection<Integer> registeredIndicators = Sets.newTreeSet();
//	private final Map<Integer, IndicatorCalculator<? extends AbstractResult>> registeredCalculators = Maps.newHashMap();
//	
//	public MappedEventHandler(String eventCode) {
//		super();
//		this.eventCode = eventCode;
//	}
//	
//	@Override
//	public Collection<Integer> getMappedIndicators() {
//		return registeredIndicators;
//	}
//	
//	protected void registerIndicatorInternal(int indicator) {
//		IndicatorCalculator<? extends AbstractResult> calculator = IndicatorCalculatorFactory.getCalculator(indicator);
//		registeredIndicators.add(indicator);
//		registeredCalculators.put(indicator, calculator);
//		LOG.info("Registered[" + this.getClass().getSimpleName() + "\t] " + eventCode + " -> " + String.format("%02d", indicator) + " -> " + calculator);
//	}
//	
//	protected IndicatorCalculator<? extends AbstractResult> selectCalculator(int indicator) throws NoSuchElementException {
//		return registeredCalculators.get(indicator);
//	}
//
//}
