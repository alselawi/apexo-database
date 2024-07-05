import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { RequestHandler } from '../src/index';
import { Miniflare, Request as Req } from 'miniflare';
import { Request } from '@cloudflare/workers-types/experimental';
import { operationResult } from '../src/types';

const testToken = 'eyJwYXlsb2FkIjp7InByZWZpeCI6Im15LWFjY291bnQifX0=';

function addRows(db: D1Database, account = 'my-account') {
	return db.exec(
		`INSERT INTO staff (id, account, data) VALUES ('1', '${account}', 'data1'), ('2', '${account}', 'data2'), ('3', '${account}', 'data3');`
	);
}

describe('RequestHandler class - handle method', () => {
	const env: { DB: D1Database; CACHE: KVNamespace } = {} as any;
	beforeEach(async () => {
		const mf = new Miniflare({
			modules: true,
			script: ``,
			kvNamespaces: ['TEST_NAMESPACE'],
			d1Databases: ['TEST_DB'],
		});
		env.CACHE = (await mf.getKVNamespace('TEST_NAMESPACE')) as any;
		env.DB = await mf.getD1Database('TEST_DB');
		await env.DB.exec(`CREATE TABLE staff (id TEXT PRIMARY KEY, account TEXT, data TEXT);`);
		await env.DB.exec(`CREATE TABLE staff_changes (version INTEGER, account TEXT, ids TEXT);`);
	});

	it('Reject invalid methods', async () => {
		const req = new Req('http://db.website.com/', { method: 'POST' }) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;

		expect(result.success).toBe(false);
		expect(result.output).toBe('Invalid method');
	});

	it('OPTIONS & HEAD are OK', async () => {
		const req = new Req('http://db.website.com/', { method: 'OPTIONS' }) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		expect(result.output).toBe('OK');

		const req2 = new Req('http://db.website.com/', { method: 'HEAD' }) as unknown as Request;
		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		expect(result2.output).toBe('OK');
	});

	it('All valid methods require authorization', async () => {
		const req = new Req('http://db.website.com/', { method: 'GET' }) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Authorization header is missing');
	});

	it('Authorization header must be valid', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: false }) });
		const req = new Req('http://db.website.com/', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Authorization failed');
	});

	it('Table name must be valid', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/invalid_table/', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Invalid table name');
	});

	it('Handle GET requests to fetch all rows', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const res = JSON.parse(result.output);
		expect(res.rows.length).toBe(3);
		expect(res.rows).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
		expect(res.version).toBe(0);
	});

	it('Undefined version is 0 i.e. all rows', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const res = JSON.parse(result.output);
		expect(res.rows.length).toBe(3);
		expect(res.rows).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
		expect(res.version).toBe(0);
	});

	it('Invalid version returns error', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/invalid', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Invalid version');
	});

	it('Negative version returns error', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/-1', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Invalid version');
	});

	it('Infinity version returns error', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/Infinity', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Invalid version');

		const req2 = new Req('http://db.website.com/staff/-Infinity', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(false);
		expect(result2.output).toBe('Invalid version');
	});

	it('Over pagination returns empty array', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0/2', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		expect(JSON.parse(result.output)).toEqual({
			rows: [],
			version: 0,
		});
	});

	it('Handle GET requests to fetch rows in pages (batches) if they are too many', { timeout: 10000 }, async () => {
		let x = 2000;
		while (x--) await env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('${x}', 'my-account', 'data${x}');`);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(1000);
		expect(JSON.parse(result.output).version).toBe(0);

		const req2 = new Req('http://db.website.com/staff/0/1', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = JSON.parse(result2.output).rows;
		expect(rows2.length).toBe(1000);

		const req3 = new Req('http://db.website.com/staff/0/2', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler3 = new RequestHandler(req3, env);
		const result3 = (await (await handler3.handle()).json()) as operationResult;
		expect(result3.success).toBe(true);
		const rows3 = JSON.parse(result3.output).rows;
		expect(rows3.length).toBe(0);
	});

	it("Don't fetch rows from other accounts", async () => {
		await addRows(env.DB, 'other-account');
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(0);
		expect(rows).toEqual([]);
	});

	it('Serve response from cache', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(3);
		expect(rows).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
		await env.DB.exec(`DELETE FROM staff;`);
		const handler2 = new RequestHandler(req, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = JSON.parse(result2.output).rows;
		expect(rows2.length).toBe(3);
		expect(rows2).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
	});

	it('Handle GET requests to fetch rows since a certain change', async () => {
		env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('1', 'my-account', 'data1');`);
		env.DB.exec(`INSERT INTO staff_changes (version, account, ids) VALUES (1, 'my-account', '1');`);

		env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('2', 'my-account', 'data2');`);
		env.DB.exec(`INSERT INTO staff_changes (version, account, ids) VALUES (2, 'my-account', '2');`);

		env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('3', 'my-account', 'data3');`);
		env.DB.exec(`INSERT INTO staff_changes (version, account, ids) VALUES (3, 'my-account', '3');`);

		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/3', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(0); // nothing has changed since version 3

		const req2 = new Req('http://db.website.com/staff/2', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = JSON.parse(result2.output).rows;
		expect(rows2.length).toBe(1); // only 3 has changed since version 2
		expect(rows2).toEqual([{ id: '3', data: 'data3' }]);

		const req3 = new Req('http://db.website.com/staff/1', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler3 = new RequestHandler(req3, env);
		const result3 = (await (await handler3.handle()).json()) as operationResult;
		expect(result3.success).toBe(true);
		const rows3 = JSON.parse(result3.output).rows;
		expect(rows3.length).toBe(2); // 2 and 3 have changed since version 1
		expect(rows3).toEqual([
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);

		const req4 = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler4 = new RequestHandler(req4, env);
		const result4 = (await (await handler4.handle()).json()) as operationResult;
		expect(result4.success).toBe(true);
		const rows4 = JSON.parse(result4.output).rows;
		expect(rows4.length).toBe(3); // all rows have changed since version 0
		expect(rows4).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);
	});

	it("Don't serve cache from other accounts", async () => {
		await addRows(env.DB, 'other-account');
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + btoa(JSON.stringify({ payload: { prefix: 'other-account' } })) },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(3);
		expect(rows).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);

		await env.DB.exec(`DELETE FROM staff;`);
		const handler2 = new RequestHandler(req, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = JSON.parse(result2.output).rows;
		expect(rows2.length).toBe(3);
		expect(rows2).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);

		const req3 = new Req('http://db.website.com/staff/0', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler3 = new RequestHandler(req3, env);
		const result3 = (await (await handler3.handle()).json()) as operationResult;
		expect(result3.success).toBe(true);
		const rows3 = JSON.parse(result3.output).rows;
		expect(rows3.length).toBe(0);
	});

	it('DELETE request must be given IDs', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('No IDs provided');
	});

	it('Handle DELETE requests', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/1/2', {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(1);
		expect(rows[0].id).toBe('3');
	});

	it("duplicate IDs doesn't cause error", async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff/1/2/1/2/3', {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(0);
	});

	it("too many IDs doesn't cause error", async () => {
		let x = 600;
		while (x--) await env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('${x}', 'my-account', 'data${x}');`);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const ids = Array.from({ length: 600 }, (_, i) => i).join('/');
		const req = new Req(`http://db.website.com/staff/${ids}`, {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(0);
	});

	it("Don't delete rows from other accounts", async () => {
		let x = 10;
		while (x--) await env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('a${x}', 'my-account', 'data${x}');`);

		let y = 10;
		while (y--) await env.DB.exec(`INSERT INTO staff (id, account, data) VALUES ('b${y}', 'other-account', 'data${y}');`);

		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(20);

		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const ids = 'a' + Array.from({ length: 10 }, (_, i) => i).join('/a');
		const req = new Req(`http://db.website.com/staff/${ids}`, {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows2 = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows2.length).toBe(10);
	});

	it('Delete operations nullify cache', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });

		const req = new Req('http://db.website.com/staff', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const rows = JSON.parse(result.output).rows;
		expect(rows.length).toBe(3);
		expect(rows).toEqual([
			{ id: '1', data: 'data1' },
			{ id: '2', data: 'data2' },
			{ id: '3', data: 'data3' },
		]);

		const cacheKeys = (await env.CACHE.list()).keys.map((k) => k.name);
		expect(cacheKeys.length).toBe(2);
		expect(cacheKeys).toContain('[RK]staff[IN]my-account');

		const req2 = new Req('http://db.website.com/staff/1/2', {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows2.length).toBe(1);
		expect(await env.CACHE.get('[RK]staff[IN]my-account')).toBe('[]');

		const cacheKeys2 = (await env.CACHE.list()).keys.map((k) => k.name);
		expect(cacheKeys2.length).toBe(2);
		expect(cacheKeys2).toContain('[RK]staff[IN]my-account');
		const thisCacheKey = cacheKeys2.find((k) => k !== '[RK]staff[IN]my-account');
		expect(thisCacheKey).toBeDefined();
		expect(await env.CACHE.get(thisCacheKey!)).toBe('');
	});

	it('Delete operations record change', async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });

		const req = new Req('http://db.website.com/staff', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const res = JSON.parse(result.output);
		expect(res).toEqual({
			rows: [
				{ id: '1', data: 'data1' },
				{ id: '2', data: 'data2' },
				{ id: '3', data: 'data3' },
			],
			version: 0,
		});

		const req2 = new Req('http://db.website.com/staff/1/2', {
			method: 'DELETE',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const version = parseInt(result2.output);
		const rows2 = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows2.length).toBe(1);

		const changes = (await env.DB.prepare(`SELECT * FROM staff_changes;`).all()).results;
		expect(changes.length).toBe(1);
		expect(changes[0].version).toBe(version);
		expect(changes[0].ids).toBe('1,2');

		const req3 = new Req('http://db.website.com/staff', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler3 = new RequestHandler(req3, env);
		const result3 = (await (await handler3.handle()).json()) as operationResult;
		expect(result3.success).toBe(true);
		const res3 = JSON.parse(result3.output);
		expect(res3).toEqual({
			rows: [{ id: '3', data: 'data3' }],
			version,
		});


		const req4 = new Req('http://db.website.com/staff/' + version, {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler4 = new RequestHandler(req4, env);
		const result4 = (await (await handler4.handle()).json()) as operationResult;
		expect(result4.success).toBe(true);
		const res4 = JSON.parse(result4.output);
		expect(res4).toEqual({
			rows: [],
			version, // no changes since version, so version is the same
		});
	});

	it('PUT request must have a body', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Request body is empty or invalid');
	});

	it('PUT request must have a valid JSON body', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: 'invalid-json',
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('Request body is empty or invalid');
	});

	it('PUT request must have a non-empty JSON body', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: JSON.stringify({}),
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(false);
		expect(result.output).toBe('JSON body is empty');
	});

	it("empty ID in PUT request doesn't cause error (ignored)", async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: JSON.stringify({ '': 'data4' }),
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;

		expect(result.success).toBe(true);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(3);
	});

	it('Handle regular PUT requests', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: JSON.stringify({ '4': 'data4' }),
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;

		expect(result.success).toBe(true);
		const version = parseInt(result.output);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(1);
		expect(rows[0].id).toBe('4');

		const changes = (await env.DB.prepare(`SELECT * FROM staff_changes;`).all()).results;
		expect(changes.length).toBe(1);
		expect(changes[0].version).toBe(version);
		expect(changes[0].ids).toBe('4');
	});

	it('Handle PUT requests with multiple rows', async () => {
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });
		const req = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: JSON.stringify({ '4': 'data4', '5': 'data5', '6': 'data6' }),
		}) as unknown as Request;
		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;

		expect(result.success).toBe(true);
		const version = parseInt(result.output);
		const rows = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows.length).toBe(3);
		expect(rows).toEqual([
			{ id: '4', account: 'my-account', data: 'data4' },
			{ id: '5', account: 'my-account', data: 'data5' },
			{ id: '6', account: 'my-account', data: 'data6' },
		]);

		const changes = (await env.DB.prepare(`SELECT * FROM staff_changes;`).all()).results;
		expect(changes.length).toBe(1);
		expect(changes[0].version).toBe(version);
		expect(changes[0].ids).toBe('4,5,6');
	});

	it("PUT Requests should nullify the cache", async () => {
		await addRows(env.DB);
		global.fetch = vi.fn().mockResolvedValue({ json: () => ({ success: true }) });

		const req = new Req('http://db.website.com/staff', {
			method: 'GET',
			headers: { Authorization: 'Bearer ' + testToken },
		}) as unknown as Request;

		const handler = new RequestHandler(req, env);
		const result = (await (await handler.handle()).json()) as operationResult;
		expect(result.success).toBe(true);
		const res = JSON.parse(result.output);
		expect(res).toEqual({
			rows: [
				{ id: '1', data: 'data1' },
				{ id: '2', data: 'data2' },
				{ id: '3', data: 'data3' },
			],
			version: 0,
		});

		const cacheKeys = (await env.CACHE.list()).keys.map((k) => k.name);
		expect(cacheKeys.length).toBe(2);
		expect(cacheKeys).toContain('[RK]staff[IN]my-account');

		const req2 = new Req('http://db.website.com/staff', {
			method: 'PUT',
			headers: { Authorization: 'Bearer ' + testToken },
			body: JSON.stringify({ '4': 'data4', '5': 'data5', '6': 'data6' }),
		}) as unknown as Request;

		const handler2 = new RequestHandler(req2, env);
		const result2 = (await (await handler2.handle()).json()) as operationResult;
		expect(result2.success).toBe(true);
		const rows2 = (await env.DB.prepare(`SELECT * FROM staff;`).all()).results;
		expect(rows2.length).toBe(6);

		const cacheKeys2 = (await env.CACHE.list()).keys.map((k) => k.name);
		expect(cacheKeys2.length).toBe(2);
		expect(cacheKeys2).toContain('[RK]staff[IN]my-account');
		const thisCacheKey = cacheKeys2.find((k) => k !== '[RK]staff[IN]my-account');
		expect(thisCacheKey).toBeDefined();
		expect(await env.CACHE.get(thisCacheKey!)).toBe('');
	});
});
