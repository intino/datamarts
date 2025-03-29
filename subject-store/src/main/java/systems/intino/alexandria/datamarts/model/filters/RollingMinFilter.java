package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class RollingMinFilter implements Filter {
	private final int windowSize;

	public RollingMinFilter(int windowSize) {
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

			double min = input[i - windowSize + 1];
			for (int j = i - windowSize + 1; j <= i; j++) {
				if (input[j] < min) min = input[j];
			}
			output[i] = min;
		}

		return output;
	}
}