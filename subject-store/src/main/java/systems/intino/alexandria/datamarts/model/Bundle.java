package systems.intino.alexandria.datamarts.model;

import java.time.Instant;

public interface Bundle extends Iterable<Bundle.Tuple> {

	interface Tuple {
		Data at(int index);

		interface Data {
			int asInt();
			long asLong();
			double asDouble();
			Instant asInstant();
			String asString();
		}
	}
	void close();

}
