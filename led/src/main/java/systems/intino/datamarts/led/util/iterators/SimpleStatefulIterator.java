package systems.intino.datamarts.led.util.iterators;

import java.util.Iterator;

public class SimpleStatefulIterator<T> implements StatefulIterator<T> {

	private final Iterator<T> iterator;
	private T current = null;

	public SimpleStatefulIterator(Iterator<T> iterator) {
		this.iterator = iterator;
	}

	@Override
	public T current() {
		return current;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return current = hasNext() ? iterator.next() : null;
	}
}
