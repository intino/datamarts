package io.intino.alexandria.datamarts.model.view.functions;

import io.intino.alexandria.datamarts.model.series.Sequence;

import java.util.function.Function;

public interface CategoricalFunction extends Function<Sequence, Object> {
	CategoricalFunction Count = Sequence::count;
	CategoricalFunction Entropy = s -> s.isEmpty() ? "" : s.summary().entropy();
	CategoricalFunction Mode = s -> s.isEmpty() ? "" : s.summary().mode();

}
