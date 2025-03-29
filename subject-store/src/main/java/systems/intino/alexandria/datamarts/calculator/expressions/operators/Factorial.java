package systems.intino.alexandria.datamarts.calculator.expressions.operators;

import systems.intino.alexandria.datamarts.calculator.Expression;
import systems.intino.alexandria.datamarts.calculator.Expression.UnaryExpression;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

public record Factorial(Expression on) implements UnaryExpression {

	public static Expression with(Expression operand) {
		return new Factorial(operand);
	}

	@Override
	public String toString() {
		return "Factorial[" + on + "]";
	}


	@Override
	public DoubleVector evaluate() {
		return on.evaluate().factorial();
	}
}
