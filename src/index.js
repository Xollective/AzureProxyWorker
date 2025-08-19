/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

export default {
	async fetch(request, env, ctx) {
		const url = new URL(request.url);
		const [ organization, webhookName ] = url.pathname.split('/').filter(Boolean);

		const azdevUrl = `https://dev.azure.com/${organization}/_apis/public/distributedtask/webhooks/${webhookName}?api-version=7.2-preview.2`;

		const signature = request.headers.get('X-Hub-Signature');
		let sha1 = '';
		if (signature && signature.startsWith('sha1=')) {
			sha1 = signature.slice(5); // remove prefix
		}

		const signature256 = request.headers.get('X-Hub-Signature-256');
		let sha256 = '';
		if (signature256 && signature256.startsWith('sha256=')) {
			sha256 = signature256.slice(7); // remove prefix
		}

		console.log(`Forwarding request to Azure DevOps: ${azdevUrl}`);
		console.log(`X-Hub-Signature: ${sha1}`);
		console.log(`X-Hub-Signature-256: ${sha256}`);
		const contentLength = request.headers.get('content-length');
		console.log(`Content-Length: ${contentLength !== null ? contentLength : 'unknown'}`);


		const randomFileName = `webhook-${sha1}-${Date.now()}.json`;
		const azureStorageUrl = `https://medix.blob.core.windows.net/media/webhook/${randomFileName}?sv=2023-01-03&st=2025-08-19T01%3A54%3A00Z&se=2025-08-29T01%3A54%3A00Z&sr=c&sp=acw&sig=iwMnApkIq9ISq7bQxTAXgIM160Iv%2F7OYocudVZ7%2FHdo%3D`;

		console.log(`Uploading request body to Azure Storage: ${azureStorageUrl}`);
		
		const body = await request.clone().arrayBuffer();
		const uploadResponse = await fetch(azureStorageUrl, {
			method: 'PUT',
			headers: {
				'Content-Type': 'application/json',
				'x-ms-blob-type': 'BlockBlob',
				'Content-Length': body.byteLength,
			},
			body,
		});
		console.log(`Azure Storage upload status: ${uploadResponse.status}`);

		const azdevRequest = new Request(
			azdevUrl,
			{
				method: 'POST',
				headers: (() => {
					const headers = new Headers(request.headers);
					if (signature) {
						headers.set('X-Hub-Signature', sha1);
					}
					if (signature256) {
						headers.set('X-Hub-Signature-256', sha256);
					}
					return headers;
				})(),
				body: body,
				redirect: 'follow',
			}
		);

		const azdevResponse = await fetch(azdevRequest);

		return azdevResponse;
	},
};
