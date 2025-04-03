package systems.intino.alexandria.datamarts.subjectstore.calculator;

import systems.intino.alexandria.datamarts.subjectstore.model.vectors.DoubleVector;

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
