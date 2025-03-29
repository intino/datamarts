package systems.intino.datamarts.zet;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class ListZetStream implements ZetStream {
	private final Iterator<Long> iterator;
	private long current;

	public ListZetStream(List<Long> ids) {
		this(ids.stream());
	}

	public ListZetStream(long... ids) {
		this(stream(ids).boxed());
	}

	public ListZetStream(Stream<Long> ids) {
		iterator = ids.iterator();
	}

	@Override
	public long current() {
		return current;
	}

	@Override
	public long next() {
		return current = iterator.next();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}
}
