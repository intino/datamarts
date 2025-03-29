package systems.intino.alexandria.datamarts.calculator;

import systems.intino.alexandria.datamarts.calculator.expressions.Constant;
import systems.intino.alexandria.datamarts.calculator.expressions.NamedFunction;
import systems.intino.alexandria.datamarts.calculator.expressions.Variable;
import systems.intino.alexandria.datamarts.calculator.parser.Parser;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class VectorCalculator {
	private final int size;
	private final Map<String, DoubleVector> variables;

	public VectorCalculator(int size) {
		this.size = size;
		this.variables = new HashMap<>();
	}

	public void add(String variable, DoubleVector vector) {
		if (vector.size() != size) return;
		variables.put(variable, vector);
	}

	public DoubleVector calculate(String formula) {
		Expression expression = fill(new Parser().parse(formula));
		double[] values = expression.evaluate().values();
		return new DoubleVector(values);
	}

	private Expression fill(Expression expression) {
		if (expression instanceof Expression.BinaryExpression e)
			fill(e.left(), e.right());
		if (expression instanceof Expression.UnaryExpression e)
			fill(e.on());
		if (expression instanceof NamedFunction e)
			fill(e.on());
		if (expression instanceof Constant e)
			e.set(new DoubleVector(fill(e.value())));
		if (expression instanceof Variable e)
			e.set(get(e.name()));
		return expression;
	}

	private void fill(Expression... expressions) {
		for (Expression expression : expressions) fill(expression);
	}

	public DoubleVector get(String name) {
		return switch (name) {
			case "#PI" -> new DoubleVector(fill(Math.PI));
			case "#E" -> new DoubleVector(fill(Math.E));
			case "#RANDOM" -> new DoubleVector(random());
			default -> variable(name);
		};
	}

	private DoubleVector variable(String name) {
		if (variables.containsKey(name)) return variables.get(name);
		throw new RuntimeException("Variable " + name + " not found");
	}

	private double[] random() {
		double[] values = new double[size];
		for (int i = 0; i < size; i++) values[i] = Math.random();
		return values;
	}

	private double[] fill(double value) {
		double[] values = new double[size];
		Arrays.fill(values, value);
		return values;
	}



}
