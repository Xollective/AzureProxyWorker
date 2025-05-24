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

function parseAzureListBlobsXML(xmlText) {
  const parser = new DOMParser();
  const xml = parser.parseFromString(xmlText, "application/xml");

  const blobElems = Array.from(xml.getElementsByTagName("Blob"));
  const blobs = blobElems.map(blob => {
    const name = blob.getElementsByTagName("Name")[0]?.textContent || '';
    const size = blob.getElementsByTagName("Content-Length")[0]?.textContent || '';
    const lastModified = blob.getElementsByTagName("Last-Modified")[0]?.textContent || '';
    const tagElems = blob.getElementsByTagName("Tag");
    const tags = {};
    for (const tag of tagElems) {
      const key = tag.getElementsByTagName("Key")[0]?.textContent;
      const value = tag.getElementsByTagName("Value")[0]?.textContent;
      if (key) tags[key] = value;
    }
    return { name, size, lastModified, tags };
  });

  const prefixElems = Array.from(xml.getElementsByTagName("BlobPrefix"));
  const directories = prefixElems.map(prefix => {
    return { name: prefix.getElementsByTagName("Name")[0]?.textContent || '' };
  });

  return { Blobs: blobs, Directories: directories };
}
