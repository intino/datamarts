package systems.intino.datamarts.subjectstore.calculator;

import systems.intino.datamarts.subjectstore.model.vectors.DoubleVector;

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
