import { cache } from './cache';
import { MAX_FETCH_ROWS, MAX_VARIABLES } from './variables';

export class D1 {
	constructor(public db: D1Database, public table: string, public account: string, public cacheKV: KVNamespace) {}

	async fetchAll(page: number) {
		if (page === Infinity) {
			return {
				rows: [],
				version: await this.latestChangeVersion(),
			};
		}

		if (page < 0)
			return {
				rows: [],
				version: 0,
			};
		const statement = this.db.prepare(
			`SELECT id, data FROM ${this.table} WHERE account = ? LIMIT ${MAX_FETCH_ROWS} OFFSET ${page * MAX_FETCH_ROWS};`
		);
		const rows = (await statement.bind(this.account).all()).results as Record<string, string>[];
		const version = await this.latestChangeVersion();
		return {
			rows,
			version,
		};
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
		if (ids.length === 0) {
			// reset everything, the changes and the table
			const query = `DELETE FROM ${this.table} WHERE account = ?;`;
			await this.db.prepare(query).bind(this.account).run();

			const query2 = `DELETE FROM ${this.table}_changes WHERE account = ?;`;
			await this.db.prepare(query2).bind(this.account).run();
			return;
		}
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
		if (ids.length === 0) return;
		await this.db
			.prepare(`INSERT INTO ${this.table}_changes (version, account, ids) VALUES ( ?, ?, ?);`)
			.bind(version, this.account, ids.join(','))
			.run();
	}

	async getUpdatedRowsSince(version: number, page: number) {
		let changesData: {
			latestVersion: number;
			allIds: string[];
			idMap: { [key: string]: number };
			changeRows: {
				version: number;
				ids: string[];
			}[];
		} = {
			latestVersion: version,
			allIds: [],
			changeRows: [],
			idMap: {},
		};
		const cachedResult = (await cache.get({ cacheKV: this.cacheKV, cacheKey: version.toString(), account: this.account })) || '';
		if (cachedResult) {
			changesData = JSON.parse(cachedResult);
		} else {
			const query = `SELECT ids, version FROM ${this.table}_changes WHERE account = ? AND version > ? ORDER BY version ASC;`;
			const queryResult = (await this.db.prepare(query).bind(this.account, version).all()).results as Record<string, string>[];

			const latestVersion = Math.max(...[...queryResult.map((row) => Number(row.version)), version]);

			const idMap: { [key: string]: number } = {};
			let changeRows = queryResult.map((row) => {
				const ids = row.ids.split(',');
				const currentVersion = Number(row.version);
				ids.forEach((id) => {
					if (idMap[id] === undefined || idMap[id] < currentVersion) {
						idMap[id] = currentVersion;
					}
				});
				return { version: currentVersion, ids };
			});

			changeRows = changeRows.filter((row) => {
				row.ids = row.ids.filter((id) => idMap[id] === row.version);
				return row.ids.length > 0;
			});
			const allIds = new Set(Object.keys(idMap));

			changesData = {
				latestVersion,
				allIds: [...allIds],
				changeRows,
				idMap,
			};

			await cache.put({
				cacheKV: this.cacheKV,
				cacheKey: version.toString(),
				data: JSON.stringify(changesData),
				tableName: this.table,
				account: this.account,
			});
		}

		const start = Math.max(0, page * MAX_FETCH_ROWS);
		const end = Math.min(changesData.allIds.length, start + MAX_FETCH_ROWS);
		const fetchedRows = await this.fetchRows(changesData.allIds.slice(start, end));

		for (let index = 0; index < fetchedRows.length; index++) {
			const element = fetchedRows[index];
			fetchedRows[index].ts = changesData.idMap[element.id].toString();
		}

		return {
			rows: fetchedRows,
			version: changesData.latestVersion,
		};
	}

	async latestChangeVersion() {
		const query = `SELECT MAX(version) as version FROM ${this.table}_changes WHERE account = ?;`;
		const result = await this.db.prepare(query).bind(this.account).first();
		const version = Number(result?.version);
		return isNaN(version) ? 0 : version;
	}
}
