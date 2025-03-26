package systems.intino.datamarts.led.allocators;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.buffers.store.ByteStore;

import static java.util.Objects.requireNonNull;

public abstract class SchemaFactory<T extends Schema> {

	private final Class<T> schemaClass;

	public SchemaFactory(Class<T> schemaClass) {
		this.schemaClass = requireNonNull(schemaClass);
	}

	public final Class<T> schemaClass() {
		return schemaClass;
	}

	public abstract T newInstance(ByteStore store);
}
