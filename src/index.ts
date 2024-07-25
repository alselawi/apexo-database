import { Auth } from './auth';
import { cache } from './cache';
import { corsHeaders, corsRes } from './cors';
import { D1 } from './d1';
import { handleDownload, handleUpload } from './files';
import { SUPPORTED_METHODS, VALID_TABLES } from './variables';

export class RequestHandler {
	constructor(private request: Request, private env: Env) {}

	async handle() {
		if (!SUPPORTED_METHODS.includes(this.request.method)) {
			return this.corsResponse('Invalid method');
		}

		if (this.request.method === 'OPTIONS' || this.request.method === 'HEAD') {
			return this.corsResponse('OK', true);
		}

		const authHeader = this.request.headers.get('Authorization');
		if (!authHeader) {
			return this.corsResponse('Authorization header is missing');
		}

		const authResult = await Auth.authenticate(authHeader.split(' ')[1]);
		if (!authResult.success || !authResult.account) {
			return this.corsResponse('Authorization failed');
		}

		const { account } = authResult;
		const url = new URL(this.request.url);
		const tableName = url.pathname.split('/')[1];
		const args = url.pathname
			.split('/')
			.slice(2)
			.filter((arg) => arg);

		if (!VALID_TABLES.includes(tableName)) {
			return this.corsResponse('Invalid table name');
		}

		const dbHandler = new D1(this.env.DB, tableName, account, this.env.CACHE);
		try {
			switch (this.request.method) {
				case 'GET':
					return await this.handleGet(dbHandler, args);
				case 'DELETE':
					return await this.handleDelete(dbHandler, args);
				case 'PUT':
					return await this.handlePut(dbHandler);
				default:
					return this.corsResponse('Invalid method');
			}
		} catch (error) {
			return this.corsResponse(`Error during DB operation: ${error}`);
		}
	}

	private async handleGet(dbHandler: D1, args: string[]) {
		if (dbHandler.table === 'backups') {
			const filename = args[0];
			const account = dbHandler.account;
			// make sure the user is authorized to access the specific backup
			if (!filename.includes('_' + account + '_')) {
				return this.corsResponse("You don't have access to this backup", false);
			}
			const downloadProcess = await handleDownload(filename, this.env.BACKUPS);
			return this.corsResponse(downloadProcess.output, downloadProcess.success);
		}

		const cacheKey = cache.hash(this.request.url);
		const cachedResponse = await cache.get({ cacheKV: this.env.CACHE, cacheKey, account: dbHandler.account });
		if (cachedResponse) {
			return this.corsResponse(cachedResponse, true);
		}

		const version = parseInt(args[0] || '0', 10);
		if (!isFinite(version) || version < 0 || isNaN(version)) {
			return this.corsResponse('Invalid version');
		}

		const page = Number(args[1] || '0') || 0;
		if (page < 0 || isNaN(page)) {
			return this.corsResponse('Invalid page');
		}

		let res: {
			rows: Record<string, string>[];
			version: number;
		} = {
			rows: [],
			version: 0,
		};

		if (version === 0 || page === Infinity) {
			res = await dbHandler.fetchAll(page);
		} else {
			res = await dbHandler.getUpdatedRowsSince(version, page);
		}

		let output = JSON.stringify(res);

		await cache.put({
			cacheKV: this.env.CACHE,
			cacheKey,
			data: output,
			tableName: dbHandler.table,
			account: dbHandler.account,
		});
		return this.corsResponse(output, true);
	}

	private async handleDelete(dbHandler: D1, ids: string[]) {
		if (ids.length === 0) {
			return this.corsResponse('No IDs provided');
		}

		await dbHandler.deleteRows(ids);

		await cache.nullify({ account: dbHandler.account, tableName: dbHandler.table, cacheKV: this.env.CACHE });

		const changeVersion = Date.now();
		await dbHandler.recordChange(changeVersion, ids);
		return this.corsResponse(changeVersion.toString(), true);
	}

	private async handlePut(dbHandler: D1) {

		if(dbHandler.table === 'backups') {
			const uploadProcess = await handleUpload(this.request, this.env.BACKUPS, dbHandler.account);
			return this.corsResponse(uploadProcess.output, uploadProcess.success);
		}

		let data: Record<string, string> = {};
		try {
			data = (await this.request.json()) as Record<string, string>;
		} catch {
			return this.corsResponse('Request body is empty or invalid');
		}
		if (Object.keys(data).length === 0) {
			return this.corsResponse('JSON body is empty');
		}

		await dbHandler.upsertRow(data);
		await cache.nullify({ account: dbHandler.account, tableName: dbHandler.table, cacheKV: this.env.CACHE });

		const changeVersion = Date.now();
		await dbHandler.recordChange(changeVersion, Object.keys(data));
		return this.corsResponse(changeVersion.toString(), true);
	}

	private corsResponse(output: string, success = false) {
		return corsRes({ success, output });
	}
}

export default {
	async fetch(request: Request, env: Env) {
		const handler = new RequestHandler(request, env);
		return handler.handle();
	},
};
