import { Auth } from '../src/auth';
import { describe, it, expect, vi, beforeAll } from 'vitest';

describe('Auth class - authenticate method', () => {
	beforeAll(() => {
		const mocked = vi.fn();
		mocked.mockResolvedValue({
			json: vi.fn().mockResolvedValue({ success: true }),
		});
        global.fetch = mocked;
	});

	it('should authenticate with valid token', async () => {
		const token = 'eyJwYXlsb2FkIjp7InByZWZpeCI6Im15LWFjY291bnQifX0='; // 'payload' in base64
		const result = await Auth.authenticate(token);
		expect(result.success).toBe(true);
		expect(result.account).toBe('my-account'); // Assuming account is returned on success
	});

	it('should handle authentication failure', async () => {
        global.fetch = vi.fn().mockResolvedValue({success: false});
		const token = 'eyJwYXlsb2FkIjp7InByZWZpeCI6Im15LWFjY291bnQifX0';
		const result = await Auth.authenticate(token);
		expect(result.success).toBe(false);
		expect(result.account).toBeUndefined(); // No account should be returned on failure
	});

	it('should handle network errors', async () => {
        global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));
		const token = 'eyJwYXlsb2FkIjp7InByZWZpeCI6Im15LWFjY291bnQifX0=';
		const result = await Auth.authenticate(token);
		expect(result.success).toBe(false);
		expect(result.account).toBeUndefined(); // No account should be returned on network error
	});

	it('should handle malformed token', async () => {
        global.fetch = vi.fn().mockResolvedValue({success: true});
		const token = 'malformed_jwt_token';
		const result = await Auth.authenticate(token);
		expect(result.success).toBe(false);
		expect(result.account).toBeUndefined(); // No account should be returned on malformed token
	});
});
