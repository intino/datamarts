package systems.intino.alexandria.datamarts.calculator.expressions;

import systems.intino.alexandria.datamarts.calculator.Expression;
import systems.intino.alexandria.datamarts.calculator.FunctionFactory;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

import java.util.function.Function;

public record NamedFunction(String name, Expression on) implements Expression {
	@Override
	public DoubleVector evaluate() {
		return functionOf(name).apply(on.evaluate());
	}

	@Override
	public String toString() {
		return "F_" + name + "[" + on + "]";
	}

	private Function<DoubleVector, DoubleVector> functionOf(String name) {
		return FunctionFactory.create(name);
	}
}
