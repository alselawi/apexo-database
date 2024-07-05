import { describe, it, assert, beforeEach } from 'vitest';
import { cache } from '../src/cache';
import { Miniflare } from 'miniflare';

let kv: any = null;

describe('cache class - methods', async () => {
	beforeEach(async () => {
		const mf = new Miniflare({
			modules: true,
			script: ``,
			kvNamespaces: ['TEST_NAMESPACE'],
		});
		kv = await mf.getKVNamespace('TEST_NAMESPACE');
	});

	it('should hash data correctly', async () => {
		const data = 'testData';
		const hashed = cache.hash(data);
		assert.equal(typeof hashed, 'string');
		assert.equal(cache.hash('abc'), cache.hash('abc'));
		assert.notEqual(cache.hash('abc'), cache.hash('def'));
	});

	it('should generate correct registry key', async () => {
		assert.equal(cache.registryKey('users', 'user123'), '[RK]users[IN]user123');
	});

	it('should put data into cache', async () => {
		const tableName = 'appointments';
		const account = 'alex';
		const cacheKey = 'url-x';
		const data = JSON.stringify({ id: 1, name: 'John Doe' });

		await cache.put({ cacheKV: kv, tableName, account, cacheKey, data });

		const cachedData = await kv.get(cacheKey + '[IN]' + account);
		assert.equal(cachedData, data);

		const registryKey = cache.registryKey(tableName, account);
		const registry = await kv.get(registryKey);
		assert(registry.includes(cacheKey));
	});

	it('should get data from cache', async () => {
		const cacheKey = 'url';
		const data = JSON.stringify({ id: 1, name: 'John Doe' });

		await kv.put(cacheKey + '[IN]' + 'taj', data);

		const retrievedData = await cache.get({ cacheKV: kv, cacheKey, account: 'taj' });
		assert.equal(retrievedData, data);
	});

	it('should nullify cache entries', async () => {
		await cache.put({ cacheKV: kv, tableName: 'appointments', account: 'taj', cacheKey: 'url1', data: 'data1' });
		await cache.put({ cacheKV: kv, tableName: 'appointments', account: 'taj', cacheKey: 'url2', data: 'data2' });

		await cache.put({ cacheKV: kv, tableName: 'appointments', account: 'arw', cacheKey: 'url1', data: 'data1' });
		await cache.put({ cacheKV: kv, tableName: 'appointments', account: 'arw', cacheKey: 'url2', data: 'data2' });

		await cache.put({ cacheKV: kv, tableName: 'staff', account: 'taj', cacheKey: 'url11', data: 'data1' });
		await cache.put({ cacheKV: kv, tableName: 'staff', account: 'taj', cacheKey: 'url12', data: 'data2' });

		assert.equal(await cache.get({ cacheKV: kv, cacheKey: 'url1', account: "taj" }), 'data1');
		assert.equal(await cache.get({ cacheKV: kv, cacheKey: 'url2', account: "taj" }), 'data2');

		await cache.nullify({ cacheKV: kv, tableName: 'appointments', account: 'taj' });

		assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url1", account: "taj" }), "");
		assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url2", account: "taj" }), "");

        // other accounts should not be affected
        assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url1", account: "arw" }), "data1");
        assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url2", account: "arw" }), "data2");

        // other tables should not be affected
        assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url11", account: "taj" }), "data1");
        assert.equal(await cache.get({ cacheKV: kv, cacheKey: "url12", account: "taj" }), "data2");

		const registry = await kv.get('[RK]appointments[IN]taj');
		assert.equal(registry, '[]');
	});

    it('should handle concurrent access to put method', async () => {
        const tableName = 'concurrent';
        const account = 'user123';
        const cacheKey = 'concurrentKey';
        const data1 = 'data1';
        const data2 = 'data2';
    
        await Promise.all([
          cache.put({ cacheKV: kv, tableName, account, cacheKey, data: data1 }),
          cache.put({ cacheKV: kv, tableName, account, cacheKey, data: data2 }),
        ]);
    
        const cachedData = await kv.get(cacheKey + '[IN]' + account);
        assert.ok([data1, data2].includes(cachedData)); // One of the data should persist
      });

      it('should handle cache key collisions', async () => {
        const tableName = 'collision';
        const account = 'user123';
        const cacheKey = 'sameKey';
        const data1 = 'data1';
        const data2 = 'data2';
    
        await cache.put({ cacheKV: kv, tableName, account, cacheKey, data: data1 });
        await cache.put({ cacheKV: kv, tableName, account, cacheKey, data: data2 });
    
        const cachedData = await kv.get(cacheKey + '[IN]' + account);
        assert.equal(cachedData, data2); // Last write should persist
      });

      it('should handle large data sets', async () => {
        const tableName = 'largeData';
        const account = 'user123';
        const cacheKey = 'largeDataKey';
        const data = 'a'.repeat(10 * 1024 * 1024); // 10MB of data
    
        await cache.put({ cacheKV: kv, tableName, account, cacheKey, data });
        const cachedData = await kv.get(cacheKey + '[IN]' + account);
        assert.equal(cachedData, data);
      });

      it('should handle null values in cache put and get', async () => {
        const tableName = 'users';
        const account = 'user123';
        const cacheKey = 'user123_data';
        const data = null;
    
        await cache.put({ cacheKV: kv, tableName, account, cacheKey, data: JSON.stringify(data) });
        const cachedData = await kv.get(cacheKey + '[IN]' + account);
        assert.equal(cachedData, JSON.stringify(data));
    
        const retrievedData = await cache.get({ cacheKV: kv, cacheKey, account });
        assert.equal(retrievedData, JSON.stringify(data));
      });

      it('should handle empty strings in hash function', async () => {
        const hashed = cache.hash('');
        assert.equal(typeof hashed, 'string');
      });
});
