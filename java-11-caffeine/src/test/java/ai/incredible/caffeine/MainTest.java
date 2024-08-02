package ai.incredible.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class MainTest {
	private AsyncCache<String, Data> cache;
	private FakeTicker fakeTicker;

	public static class FakeTicker implements Ticker {
		long nanoseconds = 0;

		@Override
		public long read() {
			return nanoseconds;
		}

		public void advance(long n, TimeUnit unit) {
			nanoseconds += unit.toNanos(n);
		}
	}

	@BeforeEach
	public void setup() {
		fakeTicker = new FakeTicker();
		cache = Caffeine.newBuilder()
			//			.expireAfterWrite(5, TimeUnit.SECONDS)
			.refreshAfterWrite(1, TimeUnit.SECONDS)
			.ticker(fakeTicker)
			.buildAsync(new AsyncCacheLoader<String, Data>() {

				@Override
				public CompletableFuture<? extends Data> asyncLoad(String key, Executor executor) {
					return CompletableFuture.completedFuture(createData(key));
				}

				@Override
				public CompletableFuture<? extends Data> asyncReload(String key, Data oldValue,
					Executor executor) throws Exception {
					return CompletableFuture.completedFuture(createData(key));
				}
			});
	}

	protected Data createData(String key) {

		return Data.builder().name(key).money(1000).build();
	}

	@Test
	public void testCacheExpiration() throws ExecutionException, InterruptedException {
		String key = "key1";
		cache.put(key, CompletableFuture.supplyAsync(() -> createData("haha")));
		CompletableFuture<Data> future = cache.getIfPresent(key);
		Assertions.assertEquals("haha", future.get().getName());

		fakeTicker.advance(2, TimeUnit.SECONDS);
		future =
			cache.get(key, (k, executor) -> CompletableFuture.supplyAsync(() -> createData("CC")));
		Assertions.assertEquals(key, future.get().getName());

		key = "key2";
		fakeTicker.advance(5, TimeUnit.SECONDS);
		CompletableFuture<Data> ifPresent =
			cache.get(key, (k, executor) -> CompletableFuture.supplyAsync(() -> createData("CC")));
		Assertions.assertNull(ifPresent);
		cache.synchronous();

		fakeTicker.advance(5, TimeUnit.SECONDS);
		ifPresent = cache.getIfPresent(key);
		Assertions.assertNull(ifPresent);
	}

}