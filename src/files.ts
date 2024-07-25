const MAX_FILE_SIZE = 1024 * 1024 * 10; // 10 MB
const ALLOWED_CONTENT_TYPES = ['text/plain'];

function random(): string {
	return 'xxxyxxxyxxxyxxxy'.replace(/[xy]/g, (c) => {
		const r = (Math.random() * 16) | 0;
		const v = c === 'x' ? r : (r & 0x3) | 0x8;
		return v.toString(16);
	});
}

export async function handleUpload(request: Request, bucket: R2Bucket, account: string) {
	const contentType = request.headers.get('Content-Type');
	const contentLength = parseInt(request.headers.get('Content-Length') || '0', 10);

	if (!contentType || !ALLOWED_CONTENT_TYPES.includes(contentType)) {
		return {
			success: false,
			output: 'Invalid content type',
		};
	}

	if (contentLength > MAX_FILE_SIZE) {
		return {
			success: false,
			output: 'File too large',
		};
	}

	const fileContent = await request.arrayBuffer();
	const fileName = `${account}_${contentLength}_${Date.now()}_${random()}`;

	await bucket.put(fileName, fileContent, {
		httpMetadata: { contentType },
	});

	return {
		success: true,
		output: fileName,
	};
}

export async function handleDownload(filename: string, bucket: R2Bucket) {
	if (!filename) {
		return {
			success: false,
			output: 'Invalid file name',
		};
	}

	const file = await bucket.get(filename);

	if (!file) {
		return {
			success: false,
			output: 'File not found',
		};
	}

	return {
		success: true,
		output: await file.text(),
	};
}
