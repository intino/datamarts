package systems.intino.datamarts.zet;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.zet.io.ZInputStream;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings({"WeakerAccess", "unused"})
public class ZetReader implements ZetStream {
	private final Iterator<Long> iterator;
	private long current = -1;

	public ZetReader(File file) {
		this(iteratorOf(inputStream(file)));
	}

	public ZetReader(InputStream is) {
		this(iteratorOf(zInputStream(is)));
	}

	public ZetReader(long... ids) {
		this(Arrays.stream(ids).boxed());
	}

	public ZetReader(List<Long> ids) {
		this(ids.stream());
	}

	public long current() {
		return this.current;
	}

	public long next() {
		return this.current = this.iterator.next();
	}

	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	public ZetReader(Stream<Long> stream) {
		this(stream.sorted().iterator());
	}

	public ZetReader(Iterator<Long> iterator) {
		this.iterator = iterator;
	}

	private static Iterator<Long> iteratorOf(final ZInputStream stream) {
		return new Iterator<>() {
			private long next = read();

			public Long next() {
				long next = this.next;
				this.next = read();
				return next;
			}

			public boolean hasNext() {
				return this.next != -1;
			}

			private long read() {
				try {
					return stream.readLong();
				} catch (IOException e) {
					this.close();
					return -1;
				}
			}

			private void close() {
				try {
					stream.close();
				} catch (IOException e) {
					Logger.error(e);
				}

			}
		};
	}

	private static ZInputStream inputStream(File file) {
		try {
			return zInputStream(new BufferedInputStream(new FileInputStream(file)));
		} catch (IOException e) {
			return zInputStream(new ByteArrayInputStream(new byte[0]));
		}
	}

	private static ZInputStream zInputStream(InputStream inputStream) {
		return new ZInputStream(inputStream);
	}
}
