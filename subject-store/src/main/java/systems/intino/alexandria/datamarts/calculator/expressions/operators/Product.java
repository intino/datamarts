package systems.intino.alexandria.datamarts.calculator.expressions.operators;

import systems.intino.alexandria.datamarts.calculator.Expression;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

public record Product(Expression left, Expression right) implements Expression.BinaryExpression {

	public static Expression with(Expression right, Expression left) {
		return new Product(left, right);
	}

	@Override
	public String toString() {
		return "Mul[" + left + ", " + right + "]";
	}


	@Override
	public DoubleVector evaluate() {
		return left.evaluate().mul(right.evaluate());
	}
}
