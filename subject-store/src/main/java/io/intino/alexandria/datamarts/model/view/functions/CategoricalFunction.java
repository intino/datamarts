package io.intino.alexandria.datamarts.model.view.functions;

import io.intino.alexandria.datamarts.model.series.Sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface CategoricalFunction extends Function<Sequence, Object> {

	CategoricalFunction Count = Sequence::count;
	CategoricalFunction Entropy = s -> s.isEmpty() ? "" : s.summary().entropy();
	CategoricalFunction Mode = s -> s.isEmpty() ? "" : s.summary().mode();

	Map<String, CategoricalFunction> FUNCTIONS_MAP = new HashMap<>() {{
		put("COUNT", Count);
		put("ENTROPY", Entropy);
		put("MODE", Mode);
	}};

	static CategoricalFunction fromString(String functionName) {
		CategoricalFunction function = FUNCTIONS_MAP.get(functionName.toUpperCase());
		if (function == null) {
			throw new IllegalArgumentException("Unknown Function: " + functionName);
		}
		return function;
	}
}
