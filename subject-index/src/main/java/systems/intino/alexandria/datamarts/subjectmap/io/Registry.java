package systems.intino.alexandria.datamarts.subjectmap.io;

import java.io.Closeable;
import java.util.List;

public interface Registry extends Closeable {
	List<String> subjects();
	List<String> tokens();

	List<Integer> tokensOf(int subject);
	List<Integer> exclusiveTokensOf(int subject);
	List<Integer> subjectsFilteredBy(List<Integer> subjects, List<Integer> tokens);

	void rename(int id, String name);

	int insertSubject(String name);
	int insertToken(String name);

	void link(int subject, int token);
	void unlink(int subject, int token);

	void drop(int subject);
	void commit();

}
