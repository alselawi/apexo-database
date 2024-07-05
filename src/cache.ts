export class cache {
	static hash(data: string) {
		let h32 = 0x811c9dc5; // Initial seed
		const PRIME32_1 = 2654435761;

		for (let i = 0; i < data.length; i++) {
			h32 ^= data.charCodeAt(i);
			h32 = (h32 * PRIME32_1) >>> 0;
		}
		return h32.toString();
	}

	static registryKey(tableName: string, account: string) {
		return `[RK]${tableName}[IN]${account}`;
	}

	static async put({
		cacheKV,
		tableName,
		account,
		cacheKey,
		data,
	}: {
		cacheKV: KVNamespace;
		tableName: string;
		account: string;
		cacheKey: string;
		data: string;
	}) {
		cacheKey = cacheKey + '[IN]' + account;
		await cacheKV.put(cacheKey, data);
		// add cacheKey to the registry
		const registryKey = this.registryKey(tableName, account);
		let registry = (JSON.parse((await cacheKV.get(registryKey)) || '[]') as string[]).concat([cacheKey]);
		await cacheKV.put(registryKey, JSON.stringify(registry));
	}

	static async get({ cacheKV, cacheKey, account }: { cacheKV: KVNamespace; cacheKey: string; account: string }) {
		return await cacheKV.get(cacheKey + '[IN]' + account);
	}

	/**
	 * should be called when the data in the database is updated
	 * both for insert and delete operations
	 */
	static async nullify({ cacheKV, tableName, account }: { cacheKV: KVNamespace; tableName: string; account: string }) {
		const registryKey = this.registryKey(tableName, account);
		const registry = await cacheKV.get(registryKey);
		if (!registry) return;
		const cacheKeys = JSON.parse(registry) as string[];
		for (const cacheKey of cacheKeys) {
			await cacheKV.put(cacheKey, '');
		}
		await cacheKV.put(registryKey, '[]');
	}
}
