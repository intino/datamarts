package systems.intino.test;

import org.junit.Test;
import systems.intino.datamarts.zet.ZetReader;
import systems.intino.datamarts.zet.ZetStream;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class Union_ {

	@Test
	public void should_read_three_sorted_streams_without_duplicates() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(1, 3, 5),
				new ZetReader(2, 6, 8, 10),
				new ZetReader(4, 7, 9)));
		for (int i = 1; i <= 10; i++)
			assertThat(union.next()).isEqualTo(i);
		assertThat(union.hasNext()).isFalse();
		assertThat(union.next()).isEqualTo(-1);
	}

	@Test
	public void should_make_a_union_of_streams_without_repeated_values() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(1, 2, 3, 5),
				new ZetReader(2, 5, 6, 8, 10),
				new ZetReader(4, 6, 7, 9)));
		for (int i = 1; i <= 10; i++)
			assertThat(union.next()).isEqualTo(i);
		assertThat(union.hasNext()).isFalse();
		assertThat(union.next()).isEqualTo(-1);
	}

	@Test
	public void should_read_streams_with_duplicates_imposing_freq_between_2_and_5() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(1, 2, 3, 5),
				new ZetReader(2, 6, 8, 10),
				new ZetReader(2, 6, 8, 10),
				new ZetReader(2, 6, 8, 10),
				new ZetReader(2, 4, 7, 9),
				new ZetReader(2, 4, 7, 9),
				new ZetReader(2, 4, 7, 9)
		), 2, 5, false);

		List<Long> longs = new ArrayList<>();
		while (union.hasNext()) longs.add(union.next());

		assertThat(longs.size()).isEqualTo(6);
		assertThat(longs.get(0)).isEqualTo(4L);
		assertThat(longs.get(1)).isEqualTo(6L);
		assertThat(longs.get(2)).isEqualTo(7L);
		assertThat(longs.get(3)).isEqualTo(8L);
		assertThat(longs.get(4)).isEqualTo(9L);
		assertThat(longs.get(5)).isEqualTo(10L);
	}


	@Test
	public void should_read_streams_with_duplicates_consecutive_between_2_and_5() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(1, 2, 3, 5),
				new ZetReader(2, 6, 8, 10, 11),
				new ZetReader(6, 8, 10),
				new ZetReader(6, 8, 10, 11),
				new ZetReader(2, 4, 7, 9),
				new ZetReader(2, 4, 7, 9),
				new ZetReader(2, 4, 7, 9)
		), 2, 5, true);

		List<Long> longs = new ArrayList<>();
		while (union.hasNext()) longs.add(union.next());

		assertThat(longs.size()).isEqualTo(7);
		assertThat(longs.get(0)).isEqualTo(2L);
		assertThat(longs.get(1)).isEqualTo(4L);
		assertThat(longs.get(2)).isEqualTo(6L);
		assertThat(longs.get(3)).isEqualTo(7L);
		assertThat(longs.get(4)).isEqualTo(8L);
		assertThat(longs.get(5)).isEqualTo(9L);
		assertThat(longs.get(6)).isEqualTo(10L);
	}

	@Test
	public void should_read_three_sorted_files_without_duplicates() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(new File("test-res/testsets/norep1.zet")),
				new ZetReader(new File("test-res/testsets/norep2.zet")),
				new ZetReader(new File("test-res/testsets/norep3.zet"))));

		int count = 0;
		assertThat(union.next()).isEqualTo(0);
		count++;
		assertThat(union.next()).isEqualTo(1);
		count++;
		assertThat(union.next()).isEqualTo(2);
		count++;

		while (union.hasNext()) {
			union.next();
			count++;
		}

		assertThat(count).isEqualTo(1000);
		assertThat(union.next()).isEqualTo(-1);
	}

	@Test
	public void should_make_a_join_of_three_files_without_repeated_values() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(new File("test-res/testsets/rep1.zet")),
				new ZetReader(new File("test-res/testsets/rep2.zet")),
				new ZetReader(new File("test-res/testsets/rep3.zet"))));

		assertThat(union.next()).isEqualTo(1);
		assertThat(union.next()).isEqualTo(2);
		assertThat(union.next()).isEqualTo(3);
		assertThat(union.next()).isEqualTo(4);
		assertThat(union.next()).isEqualTo(5);
		assertThat(union.hasNext()).isFalse();
		assertThat(union.next()).isEqualTo(-1);
	}

	@Test
	public void should_read_three_sorted_files_with_duplicates_imposing_freq_over_1() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(new File("test-res/testsets/rep1.zet")),
				new ZetReader(new File("test-res/testsets/rep2.zet")),
				new ZetReader(new File("test-res/testsets/rep3.zet"))), 2, 5, false);

		List<Long> longs = new ArrayList<>();
		while (union.hasNext()) longs.add(union.next());

		assertThat(longs.size()).isEqualTo(4);
		assertThat(longs.get(0)).isEqualTo(1L);
		assertThat(longs.get(1)).isEqualTo(2L);
		assertThat(longs.get(2)).isEqualTo(3L);
		assertThat(longs.get(3)).isEqualTo(4L);
	}

	@Test
	public void should_read_three_sorted_files_with_duplicates_imposing_freq_over_2() {
		ZetStream.Union union = new ZetStream.Union(asList(
				new ZetReader(new File("test-res/testsets/rep1.zet")),
				new ZetReader(new File("test-res/testsets/rep2.zet")),
				new ZetReader(new File("test-res/testsets/rep3.zet"))), 3, 5, false);

		List<Long> longs = new ArrayList<>();
		while (union.hasNext()) longs.add(union.next());

		assertThat(longs.size()).isEqualTo(1);
		assertThat(longs.get(0)).isEqualTo(2L);
	}
}