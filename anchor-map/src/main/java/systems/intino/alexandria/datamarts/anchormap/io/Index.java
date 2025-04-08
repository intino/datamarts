package systems.intino.alexandria.datamarts.anchormap.io;

import java.io.Closeable;
import java.util.List;

public interface Index extends Closeable {
	int anchor(String anchor, String type);
	int token(String value);

	void push(int anchor, int token);
	void drop(int anchor, int token);
	void commit();

	List<String> get(String anchor, String type);
	List<String> search(String type, List<String> tokens);
}
