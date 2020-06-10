package edu.utexas.atallah.idgen;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class GlobalIdTest {
    private static final Logger log = LoggerFactory.getLogger(GlobalIdTest.class);

    @Test
    public void rateAssuranceTest() {
        GlobalId.init();
        StopWatch stopWatch = StopWatch.createStarted();
        long count = 750_000;
        long total = 0;         // Generate sum of IDs to ensure optimizer doesn't throw off results
        for (long i = 0; i < count; i++) {
            long id = GlobalId.getId();
            total += id;
        }
        long t = stopWatch.getTime(TimeUnit.MILLISECONDS);
        log.info("Elapsed time: {} total: {}", t, total);
        double rate = count / (double) t * 1000.0;
        log.info(String.format("Rate: %.0f operations/sec", rate));
        Assert.assertTrue(rate > 100_000L);
    }
    @Test
    public void basicCorrectnessTest() {
        GlobalId.init();
        StopWatch stopWatch = StopWatch.createStarted();

        Set<Long> ids = new HashSet<>();
        long count = 500_000;
        for (long i = 0; i < count; i++) {
            long id = GlobalId.getId();
            if (ids.contains(id)) {
                throw new IllegalStateException(String.format("Duplicate ID: %d (0x%x), i=%d", id, id, i));
            } else {
                ids.add(id);
            }
        }
        long t = stopWatch.getTime(TimeUnit.MILLISECONDS);
        log.info("Elapsed time: {}", t);
    }
    @Test
    public void multiThreadedTest() {
        GlobalId.init();
        Map<Long, Long> ids = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        StopWatch stopWatch = StopWatch.createStarted();
        executor.submit(() -> {
            for (int i = 0; i < 200_000; i++) {
                long id = GlobalId.getId();
                synchronized (ids) {
                    if (ids.get(id) == null) {
                        ids.put(id, id);
                    } else {
                        throw new IllegalStateException("Duplicate ID: " + id);
                    }
                }

            }
        });
        long t = stopWatch.getTime(TimeUnit.MILLISECONDS);
        log.info("Elapsed time: {}", t);
    }
}
