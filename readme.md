# Sync Server

A cloudflare worker that is intended to work with xWebDB for synchronization multiple databases using cloudflare D1 as a backend database and KV as a cache manager.

### Base URL

This Worker likely runs on a custom domain or a Cloudflare Workers subdomain. The base URL is not provided in the code, but it will be followed by the API endpoints listed below.

### Authentication

All requests except OPTIONS and HEAD require an authorization header with a valid JWT token. The code expects the token in the format Authorization: Bearer <token>. Token should be generated using the `auth1.apexo.app` API.

### CORS

The API supports CORS (Cross-Origin Resource Sharing) by allowing requests from any origin (\*).

### Supported Methods:

- GET
- PUT
- DELETE
- OPTIONS
- HEAD

Methods `OPTIONS` and `HEAD` are the only methods that doesn't require authentication, all other methods must be provided with `authorization` header.

`OPTIONS` is used by the browsers to verify CORS response. You can use the `HEAD` method for checking server status.

### Error Responses:

All error responses will be a JSON object with the following properties:

- `success`: A boolean value indicating if the request was successful (false for errors).
- `output`: An error message string describing the issue.

## API Endpoints:

### Fetching data

- Method: `GET`
- Endpoint: `/table/version/page`
  - `table`: The name of the table to fetch data from (e.g., staff, patients).
  - `version`: `optional, defaults to 0` The version of the data to fetch (0 for all data, a positive integer for updated data since the specified version).
  - `page`: `optional, defaults to 0` The page number of the results to fetch (0-based). If the response results in an empty array it means that the later page was the last one.
- Headers:

```
Authorization: Bearer your-jwt-token
```

#### Response:

```json
{
    success: boolean,
    output: "stringified JSON object that has 'version' and 'rows'"
}
```

#### Examples

```typescript
// get all rows (page 0)
const response = await fetch('https://your-worker-url/staff/0/0', {
	method: 'GET',
	headers: {
		Authorization: 'Bearer your-jwt-token',
	},
});
const result = await response.json();
// result is {success: true, output: "version: 1234, rows: [{...}, {...}, {...}]"}

// get all rows (page 0)
const response = await fetch('https://your-worker-url/staff', {
	method: 'GET',
	headers: {
		Authorization: 'Bearer your-jwt-token',
	},
});
const result = await response.json();
// result is {success: true, output: "version: 1234, rows: [{...}, {...}, {...}]"}

// get changed rows that has changes since version 123 (page 3)
const response = await fetch('https://your-worker-url/123/3', {
	method: 'GET',
	headers: {
		Authorization: 'Bearer your-jwt-token',
	},
});
const result = await response.json();
// result is {success: true, output: "version: 1234, rows: [{...}, {...}, {...}]"}
```

---

### Deleting data

- Method: `DELETE`
- Endpoint: `/table/id1/id2/id3...`
  - `table`: The name of the table to fetch data from (e.g., staff, patients).
  - `id1/id2..`: The IDs of the rows to be deleted
- Headers:

```
Authorization: Bearer your-jwt-token
```

#### Response:

```json
{
    success: boolean,
    output: "stringified version number"
}
```

#### Examples

```typescript
const response = await fetch('https://your-worker-url/staff/id1/id2', {
	method: 'DELETE',
	headers: {
		Authorization: 'Bearer your-jwt-token',
	},
});
const result = await response.json();
// result is {success: true, output: "89244"}
```

---

### Inserting/Updating

- Method: `PUT`
- Endpoint: `/table`
  - `table`: The name of the table to fetch data from (e.g., staff, patients).
- Request payload (body):

```json
{
	"row-id": "row-data"
}
```

- Headers:

```
Authorization: Bearer your-jwt-token
```

#### Response:

```json
{
    success: boolean,
    output: "stringified version number"
}
```

#### Examples

```typescript
const response = await fetch('https://your-worker-url/staff', {
	method: 'PUT',
	headers: {
		Authorization: 'Bearer your-jwt-token',
	},
	body: {
		'row-id1': 'row-data',
		'row-id2': 'row-data',
		//...etc
	},
});
const result = await response.json();
// result is {success: true, output: "89244"}
```

### Error Responses

#### Invalid Method:

```json
{ "success": false, "output": "Invalid method" }
```

#### Authorization Header Missing:

```json
{ "success": false, "output": "Authorization header is missing" }
```

#### Authorization Failed:

```json
{ "success": false, "output": "Authorization failed" }
```

#### Invalid Table Name:

```json
{ "success": false, "output": "Invalid table name" }
```

#### Invalid Version:

```json
{ "success": false, "output": "Invalid version" }
```

#### Invalid Page:

```json
{ "success": false, "output": "Invalid page" }
```

#### Request Body Empty or Invalid:

```json
{ "success": false, "output": "Request body is empty or invalid" }
```

#### No IDs Provided:

```json
{ "success": false, "output": "No IDs provided" }
```
