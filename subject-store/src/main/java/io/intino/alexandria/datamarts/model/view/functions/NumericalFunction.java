package io.intino.alexandria.datamarts.model.view.functions;

import io.intino.alexandria.datamarts.model.Series;
import io.intino.alexandria.datamarts.model.series.Signal;

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
}
