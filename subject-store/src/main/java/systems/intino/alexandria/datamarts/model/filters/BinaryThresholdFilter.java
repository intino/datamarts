package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

import static java.lang.Double.isNaN;

public class BinaryThresholdFilter implements Filter {
	private final double threshold;

	public BinaryThresholdFilter(double threshold) {
		this.threshold = threshold;
	}

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		for (int i = 0; i < input.length; i++)
			output[i] = isNaN(input[i]) ? valueOf(input[i]) : Double.NaN;
		return output;
	}

	private int valueOf(double value) {
		return value > threshold ? 1 : 0;
	}
}