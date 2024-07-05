import { cache } from './cache';
import { MAX_FETCH_ROWS, MAX_VARIABLES } from './variables';

export class D1 {
	constructor(public db: D1Database, public table: string, public account: string, public cacheKV: KVNamespace) {}

	async fetchAll(page: number) {
		if (page < 0) return [];
		const statement = this.db.prepare(
			`SELECT id, data FROM ${this.table} WHERE account = ? LIMIT ${MAX_FETCH_ROWS} OFFSET ${page * MAX_FETCH_ROWS};`
		);
		const result = (await statement.bind(this.account).all()).results as Record<string, string>[];
		return result;
	}

	async fetchRows(ids: string[]) {
		if (ids.length === 0) return [];
		ids = [...new Set(ids)];
		let allResults: Record<string, string>[] = [];
		for (let i = 0; i < ids.length; i += MAX_VARIABLES) {
			const batch = ids.slice(i, i + MAX_VARIABLES);
			const placeholders = batch.map(() => '?').join(', ');
			const statement = this.db.prepare(`SELECT id, data FROM ${this.table} WHERE id IN (${placeholders}) AND account = ?;`);
			const result = (await statement.bind(...[...batch, this.account]).all()).results as Record<string, string>[];
			allResults = allResults.concat(result || []);
		}
		return allResults;
	}

	async deleteRows(ids: string[]) {
		if (ids.length === 0) return;
		ids = [...new Set(ids)];
		for (let i = 0; i < ids.length; i += MAX_VARIABLES) {
			const batch = ids.slice(i, i + MAX_VARIABLES);
			const placeholders = batch.map(() => '?').join(', ');
			const query = `DELETE FROM ${this.table} WHERE id IN (${placeholders}) AND account = ?;`;
			await this.db
				.prepare(query)
				.bind(...[...batch, this.account])
				.run();
		}
	}

	async upsertRow(data: Record<string, string>) {
		for (const [id, value] of Object.entries(data)) {
			if (!id) continue;
			const query = `INSERT OR REPLACE INTO ${this.table} (id, account, data) VALUES (?, ?, ?);`;
			await this.db.prepare(query).bind(id, this.account, value).run();
		}
	}

	async recordChange(version: number, ids: string[]) {
		await this.db
			.prepare(`INSERT INTO ${this.table}_changes (version, account, ids) VALUES ( ?, ?, ?);`)
			.bind(version, this.account, ids.join(','))
			.run();
	}

	async getUpdatedRowsSince(version: number, page: number) {
		let ids = [];
		const cachedIds = await cache.get({ cacheKV: this.cacheKV, cacheKey: version.toString(), account: this.account });
		if (cachedIds) {
			ids = JSON.parse(cachedIds);
		} else {
			const query = `SELECT ids FROM ${this.table}_changes WHERE account = ? AND version > ? ORDER BY version ASC;`;
			const result = await this.db.prepare(query).bind(this.account, version).all();
			ids = result.results.map((row) => (row.ids as string).split(',')).flat();
			ids = [...new Set(ids)];
			await cache.put({
				cacheKV: this.cacheKV,
				cacheKey: version.toString(),
				data: JSON.stringify(ids),
				tableName: this.table,
				account: this.account,
			});
		}
		const start = page * MAX_FETCH_ROWS;
		const end = start + MAX_FETCH_ROWS;
		ids = ids.slice(start, end);
		return this.fetchRows(ids);
	}
}
