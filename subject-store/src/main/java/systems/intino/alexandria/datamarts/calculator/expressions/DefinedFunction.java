package systems.intino.alexandria.datamarts.calculator.expressions;

import systems.intino.alexandria.datamarts.calculator.Expression;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;

import java.util.ArrayList;
import java.util.List;

public class DefinedFunction implements Expression {
	public final String name;
	public final List<Expression> operands;

	public DefinedFunction(String name) {
		this.name = name;
		this.operands = new ArrayList<>();
	}

	@Override
	public DoubleVector evaluate() {
		return null;
	}

	public void push(Expression expression) {
		operands.add(expression);
	}
}
