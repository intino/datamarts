package systems.intino.test;

import org.junit.Test;
import systems.intino.datamarts.zet.ZetReader;
import systems.intino.datamarts.zet.ZetStream;

import java.io.File;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class Merge_ {

	@Test
	public void should_read_three_sorted_files_without_duplicates() {
		ZetStream.Merge merge = new ZetStream.Merge(asList(
				new ZetReader(new File("test-res/testsets/norep1.zet")),
				new ZetReader(new File("test-res/testsets/norep2.zet")),
				new ZetReader(new File("test-res/testsets/norep3.zet"))));

		int count = 0;

		for (int i = 0; i < 1000; i++) {
			assertThat(merge.next()).isEqualTo(i);
			count++;
		}

		assertThat((long) count).isEqualTo(1000);
		assertThat(merge.next()).isEqualTo(-1);
	}

}