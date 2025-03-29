package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class RollingMaxFilter implements Filter {
	private final int windowSize;

	public RollingMaxFilter(int windowSize) {
		if (windowSize <= 1) throw new IllegalArgumentException("Window size must be greater than 1");
		this.windowSize = windowSize;
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];

		for (int i = 0; i < input.length; i++) {
			if (i < windowSize - 1) {
				output[i] = Double.NaN;
				continue;
			}

			double max = input[i - windowSize + 1];
			for (int j = i - windowSize + 1; j <= i; j++) {
				if (input[j] > max) max = input[j];
			}
			output[i] = max;
		}

		return output;
	}
}