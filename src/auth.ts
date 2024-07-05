import { operationResult } from "./types";

export class Auth {
	static async authenticate(token: string): Promise<{ success: boolean; account?: string }> {
		try {
			const response = await fetch('https://auth1.apexo.app', {
				method: 'PUT',
				body: JSON.stringify({ operation: 'jwt', token }),
			});
			const result = (await response.json()) as operationResult;
			
			if (!result.success) {
				return { success: false };
			}

			const account = JSON.parse(atob(token)).payload.prefix as string;
			return { success: true, account };
		} catch (e) {
			return { success: false };
		}
	}
}