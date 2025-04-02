package systems.intino.alexandria.datamarts.model.filters;

import systems.intino.alexandria.datamarts.model.Filter;

public record SinFilter() implements Filter {

	@Override
	public double[] apply(double[] input) {
		double[] output = new double[input.length];
		for (int i = 0; i < input.length; i++)
			output[i] = Math.sin(input[i]);
		return output;
	}
}
