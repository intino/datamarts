package io.intino.alexandria.datamarts.io;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.intino.alexandria.datamarts.model.TemporalReferences.Legacy;

public final class Feed {
	public final Instant instant;
	public final String source;
	public final Map<String, Object> facts;

	public Feed(Instant instant, String source) {
		this.instant = instant;
		this.source = source;
		this.facts = new HashMap<>();
	}

	public void put(String name, Object value) {
		facts.put(name, value);
	}

	public Set<String> tags() {
		return facts.keySet();
	}

	public Object get(String tag) {
		return facts.get(tag);
	}

	public boolean isLegacy() {
		return instant.equals(Legacy);
	}
}
