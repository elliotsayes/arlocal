/**
 * @see https://github.com/ArweaveTeam/arweave-js/blob/0e9a8eeca5edc449360c25dee257dd89b15c7dc2/src/common/lib/crypto/node-driver.ts#L61
 * Utilities to verify signature generated by private key via public key
 */
import * as asn from 'asn1.js';
import { createVerify, constants } from 'crypto';

export async function verifySignature(
  publicModulus: string,
  data: Uint8Array,
  signature: Uint8Array,
): Promise<boolean> {
  return new Promise((resolve, _) => {
    const publicKey = {
      kty: 'RSA',
      e: 'AQAB',
      n: publicModulus,
    };

    const pem = jwkTopem(publicKey);
    resolve(
      createVerify('sha256').update(data).verify(
        {
          key: pem,
          padding: constants.RSA_PKCS1_PSS_PADDING,
        },
        signature,
      ),
    );
  });
}

function base64url2bn(str: string): any {
  return new asn.bignum(Buffer.from(str, 'base64'));
}

function string2bn(str: string): any {
  if (/^[0-9]+$/.test(str)) {
    return new asn.bignum(str, 10);
  }
  return base64url2bn(str);
}

function parse(jwk: any): any {
  return {
    n: string2bn(jwk.n),
    e: string2bn(jwk.e),
    d: jwk.d && string2bn(jwk.d),
    p: jwk.p && string2bn(jwk.p),
    q: jwk.q && string2bn(jwk.q),
    dp: jwk.dp && string2bn(jwk.dp),
    dq: jwk.dq && string2bn(jwk.dq),
    qi: jwk.qi && string2bn(jwk.qi),
  };
}

const Version = asn.define('Version', function (this: any) {
  this.int({
    0: 'two-prime',
    1: 'multi',
  });
});

const OtherPrimeInfos = asn.define('OtherPrimeInfos', function (this: any) {
  this.seq().obj(this.key('ri').int(), this.key('di').int(), this.key('ti').int());
});

const RSAPrivateKey = asn.define('RSAPrivateKey', function (this: any) {
  this.seq().obj(
    this.key('version').use(Version),
    this.key('n').int(),
    this.key('e').int(),
    this.key('d').int(),
    this.key('p').int(),
    this.key('q').int(),
    this.key('dp').int(),
    this.key('dq').int(),
    this.key('qi').int(),
    this.key('other').optional().use(OtherPrimeInfos),
  );
});

const RSAPublicKey = asn.define('RSAPublicKey', function (this: any) {
  this.seq().obj(this.key('n').int(), this.key('e').int());
});

export function jwkTopem(json: any): any {
  const jwk = parse(json);
  const isPrivate = !!jwk.d;
  const t = isPrivate ? 'PRIVATE' : 'PUBLIC';
  const header = '-----BEGIN RSA ' + t + ' KEY-----\n';
  const footer = '\n-----END RSA ' + t + ' KEY-----\n';
  let data = Buffer.alloc(0);
  if (isPrivate) {
    jwk.version = 'two-prime';
    data = RSAPrivateKey.encode(jwk, 'der');
  } else {
    data = RSAPublicKey.encode(jwk, 'der');
  }
  const body = data
    .toString('base64')
    .match(/.{1,64}/g)!
    .join('\n');
  return header + body + footer;
}