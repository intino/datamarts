package systems.intino.alexandria.datamarts.model.view;

import systems.intino.alexandria.datamarts.model.Filter;

import java.util.*;

public class Column {
	public final String name;
	public final String definition;
	public final List<Filter> filters;

	public Column(String name, String definition) {
		this.name = name;
		this.definition = definition;
		this.filters = new ArrayList<>();
	}

	public Column add(Filter filter) {
		filters.add(filter);
		return this;
	}

	private static final Set<String> AlphanumericRules = Set.of(
			"ts.year-quarter",
			"ts.year-month",
			"ts.year-month-day",
			"ts.year-month-day-hour",
			"ts.year-month-day-hour-minute",
			"ts.year-month-day-hour-minute-second",
			".mode"
	);

	public boolean isAlphanumeric() {
		return AlphanumericRules.stream()
				.anyMatch(definition::contains);
	}

}
