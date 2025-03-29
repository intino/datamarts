package systems.intino.alexandria.datamarts.model.view.functions;

import systems.intino.alexandria.datamarts.model.series.Sequence;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.lang.Double.NaN;

public interface CategoricalFunction extends Function<Sequence, Object> {
	Map<String, CategoricalFunction> map = create();

	private static Map<String, CategoricalFunction> create() {
		Map<String, CategoricalFunction> map = new HashMap<>();
		map.put("count", s -> (double) s.count());
		map.put("entropy", s -> s.isEmpty() ? NaN : s.summary().entropy());
		map.put("mode", s -> s.isEmpty() ? "" : s.summary().mode());
		return map;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	Set<String> numericFunctions = Set.of(
		"count", "entropy"
	);

	static boolean isNumeric(String function) {
		return numericFunctions.contains(function);
	}

	static CategoricalFunction of(String name) {
		return map.containsKey(name) ? map.get(name) : s-> "Unknown function: " + name;
	}

}
