package systems.intino.alexandria.datamarts.io;

import systems.intino.alexandria.datamarts.model.Point;

import java.io.Closeable;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;

public interface Registry extends Closeable {

	String name();

	String id();

	String type();

	int size();

	List<String> tags();

	List<Instant> instants();

	default boolean isEmpty() { return size() == 0; }

	void register(List<Feed> feeds);

	String ss(int feed);

	Point<Double> readNumber(String tag);

	Point<String> readText(String tag);

	List<Point<Double>> readNumbers(String tag, Instant from, Instant to);

	List<Point<String>> readTexts(String tag, Instant from, Instant to);

	void dump(OutputStream os);
}
