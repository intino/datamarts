package io.intino.alexandria.datamarts.model.view.functions;

import io.intino.alexandria.datamarts.model.Series;
import io.intino.alexandria.datamarts.model.series.Signal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface NumericalFunction extends Function<Signal, Object> {

	NumericalFunction Count = Series::count;
	NumericalFunction Sum = s -> s.summary().sum();
	NumericalFunction Average = s -> s.summary().mean();
	NumericalFunction StandardDeviation = s -> s.isEmpty() ? "" : s.summary().sd();
	NumericalFunction First = s -> s.isEmpty() ? "" : s.summary().first().value();
	NumericalFunction Last = s -> s.isEmpty() ? "" : s.summary().last().value();
	NumericalFunction Min = s -> s.isEmpty() ? "" : s.summary().min().value();
	NumericalFunction Max = s -> s.isEmpty() ? "" : s.summary().max().value();
	NumericalFunction TsMin = s -> s.isEmpty() ? "" : s.summary().min().instant();
	NumericalFunction TsMax = s -> s.isEmpty() ? "" : s.summary().max().instant();

	Map<String, NumericalFunction> FUNCTIONS_MAP = new HashMap<>() {{
		put("COUNT", Count);
		put("SUM", Sum);
		put("AVERAGE", Average);
		put("STANDARDDEVIATION", StandardDeviation);
		put("FIRST", First);
		put("LAST", Last);
		put("MIN", Min);
		put("MAX", Max);
		put("TSMIN", TsMin);
		put("TSMAX", TsMax);
	}};

	static NumericalFunction fromString(String functionName) {
		NumericalFunction function = FUNCTIONS_MAP.get(functionName.toUpperCase());
		if (function == null) {
			throw new IllegalArgumentException("Unknown Function: " + functionName);
		}
		return function;
	}
}
