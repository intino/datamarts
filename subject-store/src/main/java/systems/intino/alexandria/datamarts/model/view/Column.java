package systems.intino.alexandria.datamarts.model.view;

import systems.intino.alexandria.datamarts.model.Filter;
import systems.intino.alexandria.datamarts.model.view.functions.CategoricalFunction;
import systems.intino.alexandria.datamarts.model.view.functions.NumericalFunction;
import systems.intino.alexandria.datamarts.model.view.functions.TemporalFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Column {
	public final String name;
	public final String function;
	public final String attribute;
	public final Type type;
	public final List<Filter> filters;

	public Column(String definition) {
		this(map(definition));
	}

	private Column(Map<String,String> definition) {
		this.name = definition.get("name");
		this.function = definition.get("function");
		this.attribute = definition.get("attribute");
		this.type = typeIn(function);
		this.filters = new ArrayList<>();
	}

	private static Map<String, String> map(String definition) {
		String[] nameAndRest = definition.split("=", 2);
		String[] functionAndAttribute = nameAndRest[1].split("\\.", 2);

		return createResult(nameAndRest[0], functionAndAttribute);
	}

	private static Map<String, String> createResult(String name, String[] functionAndAttribute) {
		Map<String, String> result = new HashMap<>();
		result.put("name", name);
		result.put("function", functionAndAttribute.length == 1 ? functionAndAttribute[0] : functionAndAttribute[1]);
		result.put("attribute", functionAndAttribute.length > 1 ? functionAndAttribute[0] : null);
		return result;
	}

	private static Type typeIn(String function) {
		if (TemporalFunction.contains(function)) return Type.Temporal;
		if (NumericalFunction.contains(function)) return Type.Numerical;
		if (CategoricalFunction.contains(function)) return Type.Categorical;
		return Type.Formula;
	}

	public Column add(Filter filter) {
		filters.add(filter);
		return this;
	}

	public enum Type {
		Temporal, Numerical, Categorical, Formula
	}

}
