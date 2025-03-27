package systems.intino.datamarts.zet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused")
public class Zet {
	private final long[] ids;

	public Zet(ZetStream stream) {
		List<Long> longList = new ArrayList<>();
		while (stream.hasNext()) longList.add(stream.next());
		ids = ids(longList);
	}

	private static long[] ids(List<Long> longList) {
		long[] longs = new long[longList.size()];
		for (int i = 0; i < longList.size(); i++) longs[i] = longList.get(i);
		return longs;
	}

	public long[] ids() {
		return ids;
	}

	public boolean isIn(long id) {
		return Arrays.binarySearch(ids, id) >= 0;
	}

	public int size() {
		return ids.length;
	}
}