package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class RollingSumFilter implements Filter {
	private final int windowSize;

	public RollingSumFilter(int windowSize) {
		if (windowSize <= 1) throw new IllegalArgumentException("Window size must be greater than 1");
		this.windowSize = windowSize;
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		double sum = 0.0;

		for (int i = 0; i < input.length; i++) {
			sum += input[i];
			if (i >= windowSize)
				sum -= input[i - windowSize];
			output[i] = i >= windowSize - 1 ? sum : Double.NaN;
		}

		return output;
	}
}
