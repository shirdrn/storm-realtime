package org.shirdrn.storm.api;

import java.io.Serializable;

public interface IndicatorCalculator<R, C, E> extends Serializable {

	int getIndicator();
	R calculate(final C client, E event);
}
