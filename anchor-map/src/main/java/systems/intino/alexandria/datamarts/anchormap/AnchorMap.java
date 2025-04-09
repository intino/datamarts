package systems.intino.alexandria.datamarts.anchormap;

import systems.intino.alexandria.datamarts.anchormap.io.Index;
import systems.intino.alexandria.datamarts.anchormap.io.indexes.SqliteIndex;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnchorMap implements Closeable {
	private final Index index;

	public AnchorMap(File file) {
		this.index = new SqliteIndex(file);
	}

	public Indexing on(String anchor) {
		return on(anchor, "document");
	}

	public Indexing on(String anchor, String type) {
		return new Indexing() {
			private final int id = index.anchor(anchor, type);
			public Indexing set(String token) {
				index.push(id, index.token(token));
				return this;
			}

			@Override
			public Indexing unset(String token) {
				index.drop(id, index.token(token));
				return this;
			}

			@Override
			public void commit() {
				index.commit();
			}
		};
	}

	public Search search() {
		return search("document");
	}

	public Search search(String type) {
		return new Search() {
			private final List<String> tokens = new ArrayList<>();
			@Override
			public Search with(String token) {
				tokens.add("+" + token);
				return this;
			}

			@Override
			public Search without(String token) {
				tokens.add("-" + token);
				return this;
			}

			@Override
			public List<String> execute() {
				return index.search(type, tokens);
			}
		};
	}

	public List<String> get(String anchor) {
		return get(anchor, "document");
	}

	public List<String> get(String anchor, String type) {
		return index.get(anchor, type);
	}

	@Override
	public void close() throws IOException {
		index.close();
	}

	public interface Indexing {
		Indexing set(String token);

		Indexing unset(String token);

		default Indexing set(String key, String value) {
			return set(key + '=' + value);
		}

		default Indexing unset(String key, String value) {
			return unset(key + '=' + value);
		}

		void commit();
	}

	public interface Search {
		Search with(String token);
		Search without(String token);

		default Search with(String key, String value) {
			return with(key + '=' + value);
		}

		default Search without(String key, String value) {
			return without(key + '=' + value);
		}

		List<String> execute();
	}

}
