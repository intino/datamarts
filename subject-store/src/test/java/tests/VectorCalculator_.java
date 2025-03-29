package tests;

import org.junit.Test;
import systems.intino.alexandria.datamarts.calculator.VectorCalculator;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorCalculator_ {
	@Test
	public void evaluates_constants_and_builtin_functions() {
		VectorCalculator calculator = new VectorCalculator(2);
		assertThat(calculator.calculate("5").values()).containsExactly(5.0, 5.0);
		assertThat(calculator.calculate("#PI").values()).containsExactly(Math.PI, Math.PI);
		assertThat(calculator.calculate("SIN(2)").values()).containsExactly(Math.sin(2), Math.sin(2));
	}

	@Test
	public void supports_variable_definition_and_arithmetic() {
		VectorCalculator calculator = new VectorCalculator(2);

		calculator.add("A", calculator.calculate("5"));
		calculator.add("B", calculator.calculate("2"));
		calculator.add("C", calculator.calculate("A * B"));
		calculator.add("D", calculator.calculate("A % 2"));

		assertThat(calculator.get("A").values())
				.containsExactly(5.0, 5.0);

		assertThat(calculator.get("B").values())
				.containsExactly(2.0, 2.0);

		assertThat(calculator.get("C").values())
				.containsExactly(10.0, 10.0);

		assertThat(calculator.get("D").values())
				.containsExactly(1.0, 1.0);
	}

	@Test
	public void supports_functions_on_variables() {
		VectorCalculator calculator = new VectorCalculator(2);

		calculator.add("A", calculator.calculate("5"));
		calculator.add("F", calculator.calculate("sin(A)"));

		assertThat(calculator.get("F").values())
				.containsExactly(Math.sin(5), Math.sin(5));
	}
}
