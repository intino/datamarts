package systems.intino.datamarts.led.leds;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.indexed.IndexedAllocator;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CachedIndexedLed<T extends Schema> extends IndexedLed<T> {

	private final Map<Integer, T> cache;

	public CachedIndexedLed(IndexedAllocator<T> allocator) {
		this(allocator, WeakHashMap::new);
	}

	public CachedIndexedLed(IndexedAllocator<T> allocator, Supplier<Map<Integer, T>> cacheSupplier) {
		super(allocator);
		this.cache = requireNonNull(cacheSupplier.get());
	}

	@Override
	public T schema(int index) {
		if(cache.containsKey(index)) return cache.get(index);
		final T schema = super.schema(index);
		cache.put(index, schema);
		return schema;
	}

	public void clearCache() {
		cache.clear();
	}
}
