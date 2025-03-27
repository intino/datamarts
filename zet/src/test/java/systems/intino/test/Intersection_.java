package systems.intino.test;

import org.junit.Test;
import systems.intino.datamarts.zet.ZetReader;
import systems.intino.datamarts.zet.ZetStream;

import java.io.File;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class Intersection_ {
	@Test
	public void should_make_an_intersection_of_three_streams_without_repeated_values() {
		ZetStream.Intersection intersection = new ZetStream.Intersection(asList(
				new ZetReader(1, 2, 3, 4, 5),
				new ZetReader(1, 2, 3),
				new ZetReader(2, 4)));
		assertThat(intersection.next()).isEqualTo(2);
		assertThat(intersection.hasNext()).isFalse();
		assertThat(intersection.next()).isEqualTo(-1);
	}

	@Test
	public void should_make_an_empty_intersection() {
		ZetStream.Intersection intersection = new ZetStream.Intersection(asList(
				new ZetReader(1, 2, 3, 4, 5),
				new ZetReader(1, 2, 3),
				new ZetReader(4)));
		assertThat(intersection.hasNext()).isFalse();
		assertThat(intersection.next()).isEqualTo(-1);
	}

	@Test
	public void should_make_an_intersection_of_three_files_without_repeated_values() {
		ZetStream.Intersection intersection = new ZetStream.Intersection(asList(
				new ZetReader(new File("test-res/testsets/rep1.zet")),
				new ZetReader(new File("test-res/testsets/rep2.zet")),
				new ZetReader(new File("test-res/testsets/rep3.zet"))));
		assertThat(intersection.next()).isEqualTo(2);
		assertThat(intersection.hasNext()).isFalse();
		assertThat(intersection.next()).isEqualTo(-1);
	}

}
