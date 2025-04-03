package systems.intino.alexandria.datamarts.subjectstore.calculator.expressions.operators;

import systems.intino.alexandria.datamarts.subjectstore.calculator.Expression;
import systems.intino.alexandria.datamarts.subjectstore.calculator.Expression.UnaryExpression;
import systems.intino.alexandria.datamarts.subjectstore.model.vectors.DoubleVector;

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
