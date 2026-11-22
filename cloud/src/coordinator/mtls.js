/**
 * Pipvane v3 - Cloud Control Plane
 * Mutual TLS (mTLS) Utilities & Certificate Generator
 *
 * Provides utilities to generate cryptographically valid self-signed
 * Server and Client certificates for mutual TLS 1.3 authentication.
 */

const selfsigned = require('selfsigned');

/**
 * Generate a complete set of mTLS certificates:
 * 1. Server Certificate (bound to localhost / 127.0.0.1)
 * 2. Trusted Client Certificate (for Pipvane local agent / engine)
 * 3. Untrusted Client Certificate (for testing unauthorized access rejection)
 *
 * @param {object} [options]
 * @param {number} [options.days=365] - Certificate validity in days
 * @param {string} [options.serverHost='localhost'] - Server CN
 * @param {string} [options.clientCN='pipvane-local-agent-01'] - Trusted Client CN
 * @returns {Promise<{ server: object, client: object, untrustedClient: object }>}
 */
async function generateMtlsCertificates(options = {}) {
  const days = options.days || 365;
  const serverHost = options.serverHost || 'localhost';
  const clientCN = options.clientCN || 'pipvane-local-agent-01';

  // 1. Generate Server Certificate
  const serverPems = await selfsigned.generate(
    [
      { name: 'commonName', value: serverHost },
      { name: 'organizationName', value: 'Pipvane Cloud Control Plane' },
    ],
    {
      days,
      keySize: 2048,
      extensions: [
        {
          name: 'subjectAltName',
          altNames: [
            { type: 2, value: serverHost },
            { type: 2, value: 'localhost' },
            { type: 7, ip: '127.0.0.1' },
          ],
        },
      ],
    }
  );

  // 2. Generate Trusted Client Certificate
  const clientPems = await selfsigned.generate(
    [
      { name: 'commonName', value: clientCN },
      { name: 'organizationName', value: 'Pipvane Agent Mutual TLS' },
    ],
    {
      days,
      keySize: 2048,
    }
  );

  // 3. Generate Untrusted Client Certificate (for negative auth tests)
  const untrustedPems = await selfsigned.generate(
    [
      { name: 'commonName', value: 'untrusted-intruder' },
      { name: 'organizationName', value: 'Malicious Entity' },
    ],
    {
      days,
      keySize: 2048,
    }
  );

  return {
    server: {
      key: serverPems.private,
      cert: serverPems.cert,
      publicKey: serverPems.public,
      fingerprint: serverPems.fingerprint,
    },
    client: {
      key: clientPems.private,
      cert: clientPems.cert,
      publicKey: clientPems.public,
      fingerprint: clientPems.fingerprint,
      cn: clientCN,
    },
    untrustedClient: {
      key: untrustedPems.private,
      cert: untrustedPems.cert,
      publicKey: untrustedPems.public,
      fingerprint: untrustedPems.fingerprint,
      cn: 'untrusted-intruder',
    },
  };
}

module.exports = {
  generateMtlsCertificates,
};
