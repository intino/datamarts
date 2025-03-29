package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public class CumulativeSumFilter implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		double sum = 0.0;

		for (int i = 0; i < input.length; i++) {
			sum += input[i];
			output[i] = sum;
		}

		return output;
	}
}