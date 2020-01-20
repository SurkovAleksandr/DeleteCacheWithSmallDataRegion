package startup;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.junit.Test;

public class PopulateCacheWithStreamAndDestroy {
    private static final String CACHE_NAME_1 = "MemCache_1";
    private static final String CACHE_NAME_2 = "MemCache_2";
    private static final String CACHE_GROUP = "GroupMemCache";

    private static IgniteConfiguration createConfiguration() {

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDataRegionConfigurations()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );
        igniteConfiguration.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME_1)
                .setGroupName(CACHE_GROUP),
            new CacheConfiguration(CACHE_NAME_2)
                .setGroupName(CACHE_GROUP)
        );

        return igniteConfiguration;
    }

    /**
     * Тест воспроизводит ситуацию, когда во время удаления кэша из кэш группы происходит {@link IgniteOutOfMemoryException}.
     * Особенностью я вляется то, что вычисляется доступная память и наполняется кэш на этот размер.
     * После этого вызывается destroy на кэше.
     * */
    @Test
    public void populateCacheWithStreamAndDestroy() {
        try (Ignite ignite = Ignition.start(createConfiguration())) {
            ignite.cluster().active(true);


            try (final IgniteDataStreamer<Object, Object> streamer1 = ignite.dataStreamer(CACHE_NAME_1);
                 final IgniteDataStreamer<Object, Object> streamer2 = ignite.dataStreamer(CACHE_NAME_2)) {

                final long memorySize = ((com.sun.management.OperatingSystemMXBean)java.lang.management.ManagementFactory.getOperatingSystemMXBean())
                    .getFreePhysicalMemorySize();
                long iteration = memorySize / 4096;

                for (int i = 0; i < 100; i++) {
                    streamer1.addData(i, new byte[2001]);
                }

                for (int i = 0; i <= iteration; i++) {
                    streamer2.addData(i, new byte[2001]);
                }
            }

            IgniteCache<Object, Object> cache2 = ignite.getOrCreateCache(CACHE_NAME_2);

            cache2.destroy();
        }
    }
}
