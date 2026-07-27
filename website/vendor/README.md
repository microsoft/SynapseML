# Vendored npm dependency

`brace-expansion-5.0.8.tgz` temporarily supplies the first release that fixes
CVE-2026-14257 because the configured Microsoft npm proxy does not yet mirror
version 5.0.8.

The archive was packed from upstream commit
`96a63c0011c0288846ad41773c73e3fbd0906b59`. Every included file matches the
SHA-256 digest published by the npm CDNs for `brace-expansion@5.0.8`:

- Package metadata: <https://unpkg.com/brace-expansion@5.0.8/?meta>
- Upstream source: <https://github.com/juliangruber/brace-expansion/tree/96a63c0011c0288846ad41773c73e3fbd0906b59>
- Vendored archive integrity:
  `sha512-SiTV3HwNuNobvdhqjsfoh+D6EbHLaeJJKyf9UEhFVJ8xP57eLhuD1zpXTDTMazlai9q9rWBuqZLdAvUjv3q2Xw==`

Remove the archive and its npm override after the proxy provides version 5.0.8
or a newer compatible release.
