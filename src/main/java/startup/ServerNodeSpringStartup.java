package startup;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
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
					.setDataRegionConfigurations()
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

			final long memory = Runtime.getRuntime().maxMemory();

			IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);
			//Populating the cache with data.
			for (long i = 1; i < 500; i++) {
				cache.put(i, "Vasja Out of memory in data region" + i);
			}
		}
	}

	@Test
	public void secondStep_DestroyCache() throws Exception {
		try (Ignite ignite = Ignition.start(createConfiguration(true))) {
			ignite.cluster().active(true);

			/* After change Max Data Region does not need to delete cache.
			OOM error happened without deleting cache.
			IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

			cache.destroy();*/
		}
	}
}