const REDIRECT_STATUS = 307;

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const [ account, share, container, ...pathParts ] = url.pathname.split('/').filter(Boolean);
    const sas = url.search;

    if (!account || !share || !container) {
      return new Response(`Missing account(${account})/share(${share})/container(${container}) in path(${url.pathname})`, { status: 400 });
    }

    const relativePath = pathParts.length === 0 ? "" : `/${pathParts.join('/')}`;
    const fileUrl = `https://${account}.file.core.windows.net/${share}${relativePath}${sas}`;
    //const fileUrl = `https://eodqfx6tlz9yxwa.m.pipedream.net/${account}.file.core.windows.net/${share}${relativePath}${sas}`;

    // Redirect if no path parts after container
    if (pathParts.length === 0) {
      return Response.redirect(fileUrl, REDIRECT_STATUS);
    }
    
    //https://eodqfx6tlz9yxwa.m.pipedream.net
    const blobBaseUrl = `https://${account}.blob.core.windows.net/${container}${relativePath}`;
    const blobUrlWithSas = `${blobBaseUrl}${sas}`;

    const query = new URLSearchParams(url.search);

    if (
      request.method !== 'GET' ||
      query.has('comp') ||
      query.has('restype')
    ) {
      return Response.redirect(fileUrl, REDIRECT_STATUS);
    }

    try {
      const blobMetaResp = await fetch(blobUrlWithSas, { method: 'HEAD' });

      if (blobMetaResp.status === 404) {
        return Response.redirect(fileUrl, REDIRECT_STATUS);
      }

      if (!blobMetaResp.ok) {
        return new Response(`Blob metadata fetch failed: ${blobMetaResp.status}: ${blobUrlWithSas}`, { status: 502 });
      }

      const state = blobMetaResp.headers.get('x-ms-meta-state')?.toLowerCase() || '';

      if (!state) {
        return Response.redirect(fileUrl, REDIRECT_STATUS);
      }

      if (state === 'committed') {
        return Response.redirect(blobUrlWithSas, REDIRECT_STATUS);
      }

      if (state === 'uncommitted') {
        const getBlockListUrl = `${blobBaseUrl}?comp=blocklist&blocklisttype=all${sas}`;
        const blockListResp = await fetch(getBlockListUrl);
        if (!blockListResp.ok) {
          return new Response(`Failed to get block list: ${blockListResp.status}`, { status: 502 });
        }

        const xml = await blockListResp.text();
        const uncommittedBlockNames = extractUncommittedBlockNames(xml);
        if (uncommittedBlockNames.length === 0) {
          return new Response('No uncommitted blocks found.', { status: 400 });
        }

        const commitXml = buildPutBlockListXml(uncommittedBlockNames);

        const commitUrl = `${blobBaseUrl}?comp=blocklist${sas}`;
        const commitResp = await fetch(commitUrl, {
          method: 'PUT',
          headers: {
            'x-ms-blob-content-type': 'application/octet-stream',
            'x-ms-meta-state': 'committed',
            'Content-Type': 'application/xml',
          },
          body: commitXml
        });

        if (!commitResp.ok) {
          return new Response(`Failed to commit blocks: ${commitResp.status}`, { status: 502 });
        }

        return Response.redirect(blobUrlWithSas, REDIRECT_STATUS);
      }

      return Response.redirect(fileUrl, REDIRECT_STATUS);

    } catch (err) {
      return new Response(`Unexpected error: ${err.message}`, { status: 500 });
    }
  }
};

function extractUncommittedBlockNames(xml) {
  const uncommitted = [];
  const match = xml.match(/<UncommittedBlocks>([\s\S]*?)<\/UncommittedBlocks>/i);
  if (!match) return [];
  const blockList = match[1];
  const nameRegex = /<Name>(.*?)<\/Name>/g;
  let nameMatch;
  while ((nameMatch = nameRegex.exec(blockList)) !== null) {
    uncommitted.push(nameMatch[1]);
  }
  return uncommitted;
}

function buildPutBlockListXml(blockNames) {
  const blockListXml = blockNames.map(name => `<Latest>${name}</Latest>`).join('');
  return `<?xml version="1.0" encoding="utf-8"?><BlockList>${blockListXml}</BlockList>`;
}
