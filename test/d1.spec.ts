import { describe, it, expect, beforeEach } from 'vitest';
import { D1 } from '../src/d1';
import { Miniflare } from 'miniflare';
import { cache } from '../src/cache';
import { MAX_FETCH_ROWS, MAX_VARIABLES } from '../src/variables';

// Define the testing environment
let db: D1Database;
let kv: KVNamespace;
let d1: D1;

describe('D1 class - methods', () => {
	beforeEach(async () => {
		const mf = new Miniflare({
			modules: true,
			script: ``,
			kvNamespaces: ['TEST_NAMESPACE'],
			d1Databases: ['TEST_DB'],
		});
		kv = (await mf.getKVNamespace('TEST_NAMESPACE')) as any;
		db = await mf.getD1Database('TEST_DB');

		// Create a new instance of the D1 class for each test
		await db.exec(`CREATE TABLE test_table (id TEXT PRIMARY KEY, account TEXT, data TEXT);`);
		await db.exec(`CREATE TABLE test_table_changes (version INTEGER, account TEXT, ids TEXT);`);
		d1 = new D1(db, 'test_table', 'test_account', kv);
	});

	it('should fetch all rows correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);

		// Fetch all rows
		const result = await d1.fetchAll(0);

		expect(result).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);
	});

	it('should fetch rows by IDs correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);

		// Fetch rows by IDs
		const result = await d1.fetchRows(['1', '2']);

		expect(result).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);
	});

	it('should delete rows correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);

		// Delete rows by IDs
		await d1.deleteRows(['1', '2']);

		// Verify rows are deleted
		const result = await db.prepare(`SELECT id, data FROM test_table WHERE account = ?;`).bind('test_account').all();
		expect(result.results).toEqual([]);
	});

	it('should upsert rows correctly', async () => {
		// Upsert a row
		await d1.upsertRow({ '1': 'data1' });

		// Verify the row is upserted
		const result = await db.prepare(`SELECT id, data FROM test_table WHERE account = ?;`).bind('test_account').all();
		expect(result.results).toEqual([{ id: '1', data: 'data1' }]);

		// Upsert another row
		await d1.upsertRow({ '2': 'data2' });

		// Verify both rows are upserted
		const updatedResult = await db.prepare(`SELECT id, data FROM test_table WHERE account = ?;`).bind('test_account').all();
		expect(updatedResult.results).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);
	});

	it('should record changes correctly', async () => {
		// Record a change
		await d1.recordChange(1, ['1', '2']);

		// Verify the change is recorded
		const result = await db.prepare(`SELECT version, account, ids FROM test_table_changes WHERE account = ?;`).bind('test_account').all();
		expect(result.results).toEqual([{ version: 1, account: 'test_account', ids: '1,2' }]);
	});

	it('should get updated rows since a version correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);

		// Record a change
		await d1.recordChange(1, ['1', '2']);

		// Get updated rows since version 0
		const result = await d1.getUpdatedRowsSince(0, 0);

		expect(result).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);
	});

	it('should handle empty fetchAll correctly', async () => {
		const result = await d1.fetchAll(0);
		expect(result).toEqual([]);
	});

	it('should handle empty fetchRows correctly', async () => {
		const result = await d1.fetchRows([]);
		expect(result).toEqual([]);
	});

	it('should fetch rows with pagination', async () => {
		for (let i = 1; i <= 1500; i++) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${i}', 'test_account', 'data${i}');`);
		}

		const allDocs = (await db.prepare(`SELECT id, data FROM test_table WHERE account = 'test_account';`).all()).results;
		expect(allDocs.length).toBe(1500);

		const resultPage1 = await d1.fetchAll(0);
		const resultPage2 = await d1.fetchAll(1);
		const resultPage3 = await d1.fetchAll(3);

		expect(resultPage1.length).toBe(MAX_FETCH_ROWS);
		expect(resultPage2.length).toBe(500);
		expect(resultPage3.length).toBe(0);
		expect(new Set([...resultPage1.map((x) => x.id), ...resultPage2.map((x) => x.id), ...resultPage3.map((x) => x.id)])).toEqual(
			new Set(allDocs.map((x) => x.id))
		);

		expect(resultPage1.length).toBeLessThanOrEqual(MAX_FETCH_ROWS);
		expect(resultPage2.length).toBeLessThanOrEqual(MAX_FETCH_ROWS);
	});

	it('should fetch updated rows with caching', async () => {
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);
		await d1.recordChange(1, ['1', '2']);

		// First call should populate the cache
		const firstResult = await d1.getUpdatedRowsSince(0, 0);
		expect(firstResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);

		// Modify the data to ensure cache is used
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('3', 'test_account', 'data3');`);
		await d1.recordChange(2, ['3']); // on the second version a third row was added

		// make sure it has been added
		expect((await db.prepare(`SELECT * FROM test_table;`).all()).results).toEqual([
			{ id: '1', account: 'test_account', data: 'data1' },
			{ id: '2', account: 'test_account', data: 'data2' },
			{ id: '3', account: 'test_account', data: 'data3' },
		]);

		const secondResult = await d1.getUpdatedRowsSince(0, 0);
		expect(secondResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);

		await cache.nullify({ cacheKV: kv, tableName: 'test_table', account: 'test_account' });
		const thirdResult = await d1.getUpdatedRowsSince(0, 0);
		expect(thirdResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
	});

	it('should handle maximum variables for fetchRows', async () => {
		const ids = [];
		for (let i = 1; i <= MAX_VARIABLES + MAX_VARIABLES * 2; i++) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${i}', 'test_account', 'data${i}');`);
			ids.push(`${i}`);
		}

		const result = await d1.fetchRows(ids);
		expect(result.length).toEqual(ids.length);
	});

	it('should handle maximum variables for deleteRows', async () => {
		const ids = [];
		for (let i = 1; i <= MAX_VARIABLES + MAX_VARIABLES * 3; i++) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${i}', 'test_account', 'data${i}');`);
			ids.push(`${i}`);
		}

		await d1.deleteRows(ids);
		const result = await db.prepare(`SELECT id, data FROM test_table WHERE account = ?;`).bind('test_account').all();
		expect(result.results).toEqual([]);
	});

	it('should upsert existing rows correctly', async () => {
		// Insert a row
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);

		// Upsert the same row with new data
		await d1.upsertRow({ '1': 'updated_data1' });

		// Verify the row is updated
		const result = await db.prepare(`SELECT id, data FROM test_table WHERE account = ?;`).bind('test_account').all();
		expect(result.results).toEqual([{ id: '1', data: 'updated_data1' }]);
	});

	it('should record multiple changes correctly', async () => {
		// Record multiple changes
		await d1.recordChange(1, ['1']);
		await d1.recordChange(2, ['2', '3']);

		// Verify the changes are recorded
		const result = await db
			.prepare(`SELECT version, account, ids FROM test_table_changes WHERE account = ? ORDER BY version;`)
			.bind('test_account')
			.all();
		expect(result.results).toEqual([
			{ version: 1, account: 'test_account', ids: '1' },
			{ version: 2, account: 'test_account', ids: '2,3' },
		]);
	});

	it('should get updated rows since a version with pagination correctly', { timeout: 8000 }, async () => {
		// Insert some test data into the database
		for (let i = 1; i <= 2100; i++) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${i}', 'test_account', 'data${i}');`);
		}

		// Record changes
		await d1.recordChange(
			1,
			Array.from({ length: 600 }, (_, i) => `${i + 1}`)
		);
		await d1.recordChange(
			2,
			Array.from({ length: 600 }, (_, i) => `${i + 601}`)
		);
		await d1.recordChange(
			3,
			Array.from({ length: 600 }, (_, i) => `${i + 1201}`)
		);
		await d1.recordChange(
			4,
			Array.from({ length: 400 }, (_, i) => `${i + 1801}`)
		);
		await d1.recordChange(
			5,
			Array.from({ length: 100 }, (_, i) => `${i + 2101}`)
		);

		// Get updated rows since version 0 with pagination
		const resultPage1 = await d1.getUpdatedRowsSince(0, 0);
		const resultPage2 = await d1.getUpdatedRowsSince(0, 1);
		const resultPage3 = await d1.getUpdatedRowsSince(0, 2);

		expect(resultPage1.length).toBe(MAX_FETCH_ROWS);
		expect(resultPage2.length).toBe(MAX_FETCH_ROWS);
		expect(resultPage3.length).toBe(100);
	});

	it('should handle caching with different versions correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);
		await d1.recordChange(1, ['1', '2']);

		// First call should populate the cache
		const firstResult = await d1.getUpdatedRowsSince(0, 0);
		expect(firstResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);

		// Modify the data and record a new change
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('3', 'test_account', 'data3');`);
		await d1.recordChange(2, ['3']);

		// Fetch updated rows since version 1 (should get the new data)
		const resultVersion1 = await d1.getUpdatedRowsSince(1, 0);
		expect(resultVersion1).toEqual([{ id: '3', data: 'data3' }]);
	});

	it('should handle caching with different versions correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);
		await d1.recordChange(1, ['1', '2']);

		// First call should populate the cache
		const firstResult = await d1.getUpdatedRowsSince(0, 0);
		expect(firstResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);

		// Modify the data and record a new change
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('3', 'test_account', 'data3');`);
		await d1.recordChange(2, ['3']);

		// Fetch updated rows since version 1 (should get the new data)
		const resultVersion1 = await d1.getUpdatedRowsSince(1, 0);
		expect(resultVersion1).toEqual([{ id: '3', data: 'data3' }]);
	});

	it('should handle fetchAll with edge cases correctly', async () => {
		// Insert some test data into the database
		for (let i = 1; i <= 10; i++) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${i}', 'test_account', 'data${i}');`);
		}

		// Test fetchAll with negative page number
		const negativePageResult = await d1.fetchAll(-1);
		expect(negativePageResult).toEqual([]);

		// Test fetchAll with very large page number
		const largePageResult = await d1.fetchAll(1000);
		expect(largePageResult).toEqual([]);
	});

	it('should handle fetchRows with edge cases correctly', async () => {
		// Insert some test data into the database
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('1', 'test_account', 'data1');`);
		await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('2', 'test_account', 'data2');`);

		// Test fetchRows with duplicate IDs
		const duplicateIdsResult = await d1.fetchRows(['1', '1', '2']);
		expect(duplicateIdsResult).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
		]);

		// Test fetchRows with non-existent IDs
		const nonExistentIdsResult = await d1.fetchRows(['3', '4']);
		expect(nonExistentIdsResult).toEqual([]);
	});

	it('should handle maximum variables for fetchRows at the boundary', async () => {
		const ids = Array.from({ length: MAX_VARIABLES }, (_, i) => `${i + 1}`);
		for (const id of ids) {
			await db.exec(`INSERT INTO test_table (id, account, data) VALUES ('${id}', 'test_account', 'data${id}');`);
		}

		const result = await d1.fetchRows(ids);
		expect(result.length).toEqual(ids.length);
	});
});
