package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class DifferencingFilter implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		output[0] = Double.NaN;
		for (int i = 1; i < input.length; i++)
			output[i] = input[i] - input[i - 1];
		return output;
	}
}
