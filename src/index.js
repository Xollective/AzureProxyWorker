export default {
  async fetch(request) {
    const url = new URL(request.url);
    const [_, storageAccount, container, ...pathParts] = url.pathname.split('/');
    const blobPath = pathParts.join('/');
    const sas = url.search;

    if (!storageAccount || !container || !sas) {
      return new Response(`Invalid URL. Usage: /{account}(${storageAccount})/{container}(${container})/{path}(${blobPath})?{sas}(${sas})`, { status: 400 });
    }

    const blobServiceUrl = storageAccount.includes('.blob.core.windows.net')
      ? `https://${storageAccount}`
      : `https://${storageAccount}.blob.core.windows.net`;
    const prefix = blobPath ? (blobPath.endsWith('/') ? blobPath : blobPath + '/') : '';
    const listUrl = `${blobServiceUrl}/${container}${sas}&restype=container&comp=list&prefix=${encodeURIComponent(prefix)}&delimiter=/&include=metadata,tags`;

    const listResponse = await fetch(listUrl);
    if (!listResponse.ok) {
      return new Response(`Failed to list blobs: ${listResponse.statusText} url: '${listUrl}'`, { status: listResponse.status });
    }

    const xml = await listResponse.text();
    const parsed = parseAzureListBlobsXML(xml);

    // If there are no blobs or prefixes, assume it's a file and redirect
    if (parsed.Blobs.length === 0 && parsed.Directories.length === 0 && blobPath) {
      const blobUrl = `${blobServiceUrl}/${container}/${blobPath}${sas}`;
      return Response.redirect(blobUrl, 302);
    }

    // Render directory view
    const rows = [];

    for (const dir of parsed.Directories) {
      const dirLink = `/${storageAccount}/${container}/${dir.name}${sas}`;
      const displayName = dir.name.slice(blobPath.length);
      rows.push(`<tr><td><a href="${dirLink}">${displayName}</a></td><td>—</td><td>—</td><td>—</td></tr>`);
      
      //rows.push(`<tr><td><a href="${dir.name}">${dir.name}</a></td><td>—</td><td>—</td><td>—</td></tr>`);
    }

    for (const blob of parsed.Blobs) {
      const size = blob.size;
      const lastModified = blob.lastModified;
      const state = blob.tags['state'] || '';
      const displayName = blob.name.slice(blobPath.length);
      const blobUrl = `${blobServiceUrl}/${container}/${encodeURIComponent(blob.name)}${sas}`;
      rows.push(`<tr><td><a href="${blobUrl}">${displayName}</a></td><td>${blob.size}</td><td>${blob.lastModified}</td><td>${blob.tags['state'] || ''}</td></tr>`);
      
      // rows.push(`<tr><td><a href="${name}">${name}</a></td><td>${size}</td><td>${lastModified}</td><td>${state}</td></tr>`);
    }

    const html = `
      <html>
      <head><title>Index of /${blobPath}</title></head>
      <body>
        <h1>Index of /${blobPath}</h1>
        <table border="1" cellpadding="4" cellspacing="0">
          <thead>
            <tr><th>Name</th><th>Size (bytes)</th><th>Last Modified</th><th>State</th></tr>
          </thead>
          <tbody>${rows.join('')}</tbody>
        </table>
      </body>
      </html>
    `;

    return new Response(html, {
      headers: { 'Content-Type': 'text/html' }
    });
  }
};

// Minimal XML parser via regex
function parseAzureListBlobsXML(xml) {
  const Directories = [];
  for (const m of xml.matchAll(/<BlobPrefix><Name>(.*?)<\/Name><\/BlobPrefix>/g)) {
    Directories.push({ name: m[1] });
  }

  const Blobs = [];
  const blobPattern = `<Blob>[\\s\\S]*?<Name>(.*?)<\\/Name>[\\s\\S]*?<Content-Length>(\\d+)<\\/Content-Length>[\\s\\S]*?<Last-Modified>(.*?)<\\/Last-Modified>[\\s\\S]*?<Tags>([\\s\\S]*?)<\\/Tags>[\\s\\S]*?<\\/Blob>`;
  const blobRe = new RegExp(blobPattern, 'g');

  let m;
  while ( (m = blobRe.exec(xml)) ) {
    const [, name, size, lastModified, tagsXml] = m;
    const tags = {};
    for (const t of tagsXml.matchAll(/<Tag><Key>(.*?)<\/Key><Value>(.*?)<\/Value><\/Tag>/g)) {
      tags[t[1]] = t[2];
    }
    Blobs.push({ name, size: Number(size), lastModified, tags });
  }

  return { Directories, Blobs };
}
