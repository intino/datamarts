package systems.intino.test;

import org.junit.Test;
import systems.intino.datamarts.zet.ZetReader;
import systems.intino.datamarts.zet.ZetStream;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class Difference_ {
	@Test
	public void should_make_a_difference_of_three_files_without_repeated_values() {
		ZetStream.Difference difference = new ZetStream.Difference(asList(
				new ZetReader(new File("test-res/testsets/rep1.zet")),
				new ZetReader(new File("test-res/testsets/rep2.zet")),
				new ZetReader(new File("test-res/testsets/rep3.zet"))));

		assertThat(difference.next()).isEqualTo(5);
		assertThat(difference.hasNext()).isFalse();
		assertThat(difference.next()).isEqualTo(-1);
	}

	@Test
	public void should_make_a_difference_of_three_streams() {
		ZetStream difference = new ZetStream.Difference(asList(
				new ZetReader(1, 2, 3, 5),
				new ZetReader(2, 5, 6, 8, 10),
				new ZetReader(4, 6, 7, 9)));

		List<Long> longs = new ArrayList<>();
		while (difference.hasNext()) longs.add(difference.next());

		assertThat(longs.size()).isEqualTo(2);
		assertThat(longs.get(0)).isEqualTo(1L);
		assertThat(longs.get(1)).isEqualTo(3L);
	}
}