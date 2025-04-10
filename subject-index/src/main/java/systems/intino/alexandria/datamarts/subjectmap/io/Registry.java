package systems.intino.alexandria.datamarts.subjectmap.io;

import java.io.Closeable;
import java.util.List;

public interface Registry extends Closeable {
	List<String> subjects();
	List<String> tokens();

	int insertSubject(String subject);
	int insertToken(String token);

	void link(int subject, int token);
	void unlink(int subject, int token);
	void drop(int subject);

	void commit();
	List<Integer> tokensOf(int subject);
	List<Integer> subjectsFilteredBy(List<Integer> tokens);
}
