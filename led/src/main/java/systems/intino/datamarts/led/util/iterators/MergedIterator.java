package systems.intino.datamarts.led.util.iterators;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class MergedIterator<T> implements StatefulIterator<T> {

	private final List<StatefulIterator<T>> iterators;
	private final Comparator<StatefulIterator<T>> comparator;
	private T current = null;

	public MergedIterator(Stream<? extends Iterator<T>> iterators, Comparator<T> comparator) {
		this.iterators = iterators.map(SimpleStatefulIterator::new).collect(toList());
		this.comparator = (o1, o2) -> comparator.compare(o1.current(), o2.current());
		prepareIterators();
	}

	private void prepareIterators() {
		iterators.removeIf(i -> !i.hasNext() || i.next() == null);
		iterators.sort(comparator);
	}

	public T current() {
		return current;
	}

	@Override
	public boolean hasNext() {
		return !iterators.isEmpty();
	}

	@Override
	public T next() {
		StatefulIterator<T> iterator = iterators.remove(0);
		current = iterator.current();
		updateIterators(iterator);
		return current;
	}

	private void updateIterators(StatefulIterator<T> iterator) {
		iterator.next();
		if (iterator.current() == null) return;
		int index = Collections.binarySearch(iterators, iterator, comparator);
		index = index < 0 ? (index + 1) * -1 : index;
		iterators.add(index, iterator);
	}
}
