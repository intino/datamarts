package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class LagFilter implements Filter {
	private final int lag;

	public LagFilter(int lag) {
		if (lag < 0) throw new IllegalArgumentException("Lag must be non-negative");
		this.lag = lag;
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		for (int i = 0; i < input.length; i++) {
			output[i] = (i < lag) ? Double.NaN : input[i - lag];
		}

		return output;
	}
}
