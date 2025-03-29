package systems.intino.alexandria.datamarts.calculator;

import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

public interface Expression {

	DoubleVector evaluate();

	interface UnaryExpression extends Expression {
		Expression on();
	}

	interface BinaryExpression extends Expression {
		Expression left();
		Expression right();
	}

}
