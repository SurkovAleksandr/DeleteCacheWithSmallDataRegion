package startup;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

public class ServerNodeSpringStartup {
	private static final String CACHE_NAME = "MyCache";

	private static IgniteConfiguration createConfiguration(boolean isFirstStep) {
		long dataRegionMaxSize = (isFirstStep ? 100 : 10) * 1024 * 1024;

		IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
		igniteConfiguration.setDataStorageConfiguration(
				new DataStorageConfiguration()
				.setDefaultDataRegionConfiguration(
						new DataRegionConfiguration()
							.setMaxSize(dataRegionMaxSize)
							.setPersistenceEnabled(true)
				)
		);

		return igniteConfiguration;
	}

	@Test
	public void firstStep_PopulateCache() throws Exception {
		try (Ignite ignite = Ignition.start(createConfiguration(true))) {
			ignite.cluster().active(true);

			IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);
			//Populating the cache with data.
			for (long i = 1; i < 500; i++) {
				cache.put(i, "Vasja Out of memory in data region" + i);
			}
		}
	}

	@Test
	public void secondStep_DestroyCache() throws Exception {
		try (Ignite ignite = Ignition.start(createConfiguration(false))) {
			ignite.cluster().active(true);

			IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

			cache.destroy();
		}
	}
}