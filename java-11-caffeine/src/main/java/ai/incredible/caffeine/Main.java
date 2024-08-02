package ai.incredible.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.SneakyThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class Main {
	@SneakyThrows
	public static void main(String[] args) {
		AsyncCache<String, Data> cache = Caffeine.newBuilder()
			.maximumSize(5)
			.refreshAfterWrite(1, TimeUnit.MINUTES)
			.buildAsync(new CustomAsyncCacheLoader());

		CompletableFuture<Data> futureValue = cache.getIfPresent("haha");

	}

	public static class CustomAsyncCacheLoader implements AsyncCacheLoader<String, Data> {

		@Override
		public CompletableFuture<? extends Data> asyncLoad(String key, Executor executor)
			throws Exception {
			System.out.println("asyncLoad: "+ key);
			return retrieveData(key);
		}

		@Override
		public CompletableFuture<? extends Data> asyncReload(String key, Data oldValue,
			Executor executor) throws Exception {
			System.out.println("asyncReload: "+ key);
			return retrieveData(key);
		}

		public CompletableFuture<? extends Data> retrieveData(String key) {

			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				return Data.builder().money(100).name(key).build();
			});
		}

	}

}