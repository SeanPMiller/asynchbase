/*
 * Copyright (C) 2026  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES ARE DISCLAIMED.
 */
package org.hbase.async.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.List;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * Coverage for {@link RefreshingSSLContext}, which previously had no tests.
 * Focuses on the PEM/DER parsing helpers -- especially the rewritten PKCS#1
 * to PKCS#8 wrapping path -- and on an end-to-end load of a key + certificate
 * from files (FILES SourceType).  Private helpers are reached with Whitebox;
 * the end-to-end tests drive the public Builder API.
 */
public class TestRefreshingSSLContext {

  static final String CERT_PEM =
      "-----BEGIN CERTIFICATE-----\n" +
      "MIIDTDCCAjSgAwIBAgIUXS3ZXlOAPgOEEqW6xTSG8Vk2RJ0wDQYJKoZIhvcNAQEL\n" +
      "BQAwLzEYMBYGA1UEAwwPYXN5bmNoYmFzZS10ZXN0MRMwEQYDVQQKDApBc3luY0hC\n" +
      "YXNlMB4XDTI2MDYyNTAwMjgyMloXDTM2MDYyMjAwMjgyMlowLzEYMBYGA1UEAwwP\n" +
      "YXN5bmNoYmFzZS10ZXN0MRMwEQYDVQQKDApBc3luY0hCYXNlMIIBIjANBgkqhkiG\n" +
      "9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtlvqSfL9ZnzxDWbKeYaIELyW4YPDgpR5eRwN\n" +
      "jENjqeG2Ekr3jP0hG6zqNgBVzO8hZMPRv5ExI1ASPtrOxZPpPyH/xZtRk74h3P95\n" +
      "Kax8D+EPBSrI67/gBbothOxYsuI4BS9Kw5wHgvXwN3iuFak1F/IQ3ZIFsaR1t6kb\n" +
      "Y7uif/ZzMVPxOECikacO/5uAoE7L8LGFXp3QOlcGR38nqRDVG36tD4vrKna7CKoD\n" +
      "HAR5tA8cZRT2Hlo6Z/ohcJBo+HgAPMvTYKYJYlM4f0KTxVpxP8Bi4P6SSDCuxLm2\n" +
      "DGAVGdnVrde4jWFq+dTwHwJo/nKKHBEiVdbmMbj01X6Y7WRf7wIDAQABo2AwXjAd\n" +
      "BgNVHQ4EFgQUS7AJkUyCQi8O/nzY0slv1+6kenUwHwYDVR0jBBgwFoAUS7AJkUyC\n" +
      "Qi8O/nzY0slv1+6kenUwDwYDVR0TAQH/BAUwAwEB/zALBgNVHQ8EBAMCBaAwDQYJ\n" +
      "KoZIhvcNAQELBQADggEBAJUGduR26nA92DEWLOexQsJ4RSUkkqYNaIrF0taUUl89\n" +
      "J5LQATMCFdKXUTsI6i/YA0/Mr6BLOyAmaUNQ357mVF8GUFJNFD81RSz89TIr/06q\n" +
      "BDihQIRXPszaeCCSP61wuSeQSMmiqo8OqzEAhNrvWFD+Cl61m0D5zAVM4X7DQ5Xd\n" +
      "AdBRauXj1M/GqadS9zB20u9Ip+7Z6kd7DydZoGjX+Ek/HXdm4r04hoOmPpZyfTU0\n" +
      "/2L3g3qWvOJzF+h+G+RAoWOE6IOfBY1Z3DELjuf9KnD4xlj07ES4QS2JGhjPWXxz\n" +
      "LRS+9kQjd73NgH8VEw1AbzRbes3rZdqSR9SNagmU04A=\n" +
      "-----END CERTIFICATE-----\n";

  static final String KEY_PKCS8_PEM =
      "-----BEGIN PRIVATE KEY-----\n" +
      "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC2W+pJ8v1mfPEN\n" +
      "Zsp5hogQvJbhg8OClHl5HA2MQ2Op4bYSSveM/SEbrOo2AFXM7yFkw9G/kTEjUBI+\n" +
      "2s7Fk+k/If/Fm1GTviHc/3kprHwP4Q8FKsjrv+AFui2E7Fiy4jgFL0rDnAeC9fA3\n" +
      "eK4VqTUX8hDdkgWxpHW3qRtju6J/9nMxU/E4QKKRpw7/m4CgTsvwsYVendA6VwZH\n" +
      "fyepENUbfq0Pi+sqdrsIqgMcBHm0DxxlFPYeWjpn+iFwkGj4eAA8y9NgpgliUzh/\n" +
      "QpPFWnE/wGLg/pJIMK7EubYMYBUZ2dWt17iNYWr51PAfAmj+coocESJV1uYxuPTV\n" +
      "fpjtZF/vAgMBAAECggEACSXINWhuhK8cym7PFlSEy84iBXL+IjU/eK9LEK+qtrFf\n" +
      "WECRdW2XNROx9f6DT1Hpi2v1QBZQ8DQKjkOcnrWiPCwA8BEwAOjHOxZHpisEWCmS\n" +
      "ftBcHtNTsgXaVbIjaunFJKFwLyrLB6s9nxK7LNP9mVNnvC2ggn4893cQrCqT5rZI\n" +
      "9z5DO0Rq69cDYbv6ZulrF/ZwanQ3Kmk1oOUYyLYaMrTw14ZNKZKzjHS+kYwS+Bz1\n" +
      "3DQ3ZQVeM2S4ANmrwVJjjz8K27kQIrcDiV46BuZi9KvKsUVePGfb1VDVtZPmb3KW\n" +
      "p2nEIRk0QDFp8XK9gYhTxr3ofAY08twQjvSPg2pgpQKBgQDTM+qdUV+4Z6PO/50b\n" +
      "YUbNOOjbURp2AwIsujGPZOAuBHJdJGRQ7N0nWQk8LPIJXdomKFMc38zUOAnTGVPO\n" +
      "UgADDNUhu2DYi2vRqdwAXQY9eXhJz7o0Wc3iUh90V3rGsfVD1Fg1r/Ue29wXrfJj\n" +
      "XOAcG0St8+5yJGwyNHgcQeqlbQKBgQDdCdBkQJzrO/eNWGNOKoa6qwynpamJ5vCM\n" +
      "m4kw3nX29hkURbUKRHaQhSBxuHk6FdAzAKyLFSTDOPBkVXiYZIBApt1C0JyIp++s\n" +
      "4zPF+YaOkTt+VVw712gdOZaOTJ5Jgwdosd481KzxGB5ySTepaTTiBm4OxDR1DyD2\n" +
      "4PKr3IDtSwKBgC1RIHAs+6rnogBSXHV5g+WY5m3YkyLrNlY+hU/NR0TYc2mE23l5\n" +
      "uWIt+otM4Yoc9yfk0yCI7LxEYedHsfG9Kn99C2Y3fGo6UCImniv9yc7691JvHfcJ\n" +
      "peF/2XvvIUrs+EFbmKm8XY4HgswZ6L5lOTkOqMfiZTFcm1KSa4s0oZJpAoGACNtV\n" +
      "0E6OexaCCtXZ1M37bAtijG0k5/Oq+9dXne+sBmNCCf/pCypHHe9Xp3be1Kb/GqwS\n" +
      "PdhxCqmDaGHMXT7ZrL2C7CRzwT4JaoIIFwyyiY/kNGGzOmqdL16ZW8ZSKVvothc6\n" +
      "cnGLJHX08ltgsZcXmV7slgbimzYp+fp1ftFtERUCgYBf1Yjyq6Kp+/lVHyBonufu\n" +
      "XeKJvzkaTTm9/tHsEWGQpULFpDxPUiFNCMeq0IcQ+baONDU3euBMe05KOb01MYA/\n" +
      "fPksJpsv+P2C9mn0IoIuUluXT3KdqhQvOK8O4uaiQEw7YICtxz6vO0yhRfxLObdN\n" +
      "qdL3KbszPBgUHiiaCtAg4g==\n" +
      "-----END PRIVATE KEY-----\n";

  static final String KEY_PKCS1_PEM =
      "-----BEGIN RSA PRIVATE KEY-----\n" +
      "MIIEogIBAAKCAQEAtlvqSfL9ZnzxDWbKeYaIELyW4YPDgpR5eRwNjENjqeG2Ekr3\n" +
      "jP0hG6zqNgBVzO8hZMPRv5ExI1ASPtrOxZPpPyH/xZtRk74h3P95Kax8D+EPBSrI\n" +
      "67/gBbothOxYsuI4BS9Kw5wHgvXwN3iuFak1F/IQ3ZIFsaR1t6kbY7uif/ZzMVPx\n" +
      "OECikacO/5uAoE7L8LGFXp3QOlcGR38nqRDVG36tD4vrKna7CKoDHAR5tA8cZRT2\n" +
      "Hlo6Z/ohcJBo+HgAPMvTYKYJYlM4f0KTxVpxP8Bi4P6SSDCuxLm2DGAVGdnVrde4\n" +
      "jWFq+dTwHwJo/nKKHBEiVdbmMbj01X6Y7WRf7wIDAQABAoIBAAklyDVoboSvHMpu\n" +
      "zxZUhMvOIgVy/iI1P3ivSxCvqraxX1hAkXVtlzUTsfX+g09R6Ytr9UAWUPA0Co5D\n" +
      "nJ61ojwsAPARMADoxzsWR6YrBFgpkn7QXB7TU7IF2lWyI2rpxSShcC8qywerPZ8S\n" +
      "uyzT/ZlTZ7wtoIJ+PPd3EKwqk+a2SPc+QztEauvXA2G7+mbpaxf2cGp0NyppNaDl\n" +
      "GMi2GjK08NeGTSmSs4x0vpGMEvgc9dw0N2UFXjNkuADZq8FSY48/Ctu5ECK3A4le\n" +
      "OgbmYvSryrFFXjxn29VQ1bWT5m9ylqdpxCEZNEAxafFyvYGIU8a96HwGNPLcEI70\n" +
      "j4NqYKUCgYEA0zPqnVFfuGejzv+dG2FGzTjo21EadgMCLLoxj2TgLgRyXSRkUOzd\n" +
      "J1kJPCzyCV3aJihTHN/M1DgJ0xlTzlIAAwzVIbtg2Itr0ancAF0GPXl4Sc+6NFnN\n" +
      "4lIfdFd6xrH1Q9RYNa/1HtvcF63yY1zgHBtErfPuciRsMjR4HEHqpW0CgYEA3QnQ\n" +
      "ZECc6zv3jVhjTiqGuqsMp6WpiebwjJuJMN519vYZFEW1CkR2kIUgcbh5OhXQMwCs\n" +
      "ixUkwzjwZFV4mGSAQKbdQtCciKfvrOMzxfmGjpE7flVcO9doHTmWjkyeSYMHaLHe\n" +
      "PNSs8Rgeckk3qWk04gZuDsQ0dQ8g9uDyq9yA7UsCgYAtUSBwLPuq56IAUlx1eYPl\n" +
      "mOZt2JMi6zZWPoVPzUdE2HNphNt5ebliLfqLTOGKHPcn5NMgiOy8RGHnR7HxvSp/\n" +
      "fQtmN3xqOlAiJp4r/cnO+vdSbx33CaXhf9l77yFK7PhBW5ipvF2OB4LMGei+ZTk5\n" +
      "DqjH4mUxXJtSkmuLNKGSaQKBgAjbVdBOjnsWggrV2dTN+2wLYoxtJOfzqvvXV53v\n" +
      "rAZjQgn/6QsqRx3vV6d23tSm/xqsEj3YcQqpg2hhzF0+2ay9guwkc8E+CWqCCBcM\n" +
      "somP5DRhszpqnS9emVvGUilb6LYXOnJxiyR19PJbYLGXF5le7JYG4ps2Kfn6dX7R\n" +
      "bREVAoGAX9WI8quiqfv5VR8gaJ7n7l3iib85Gk05vf7R7BFhkKVCxaQ8T1IhTQjH\n" +
      "qtCHEPm2jjQ1N3rgTHtOSjm9NTGAP3z5LCabL/j9gvZp9CKCLlJbl09ynaoULziv\n" +
      "DuLmokBMO2CArcc+rztMoUX8Szm3TanS9ym7MzwYFB4omgrQIOI=\n" +
      "-----END RSA PRIVATE KEY-----\n";

  // ------------------------------------------------------------------ //
  // Helpers                                                            //
  // ------------------------------------------------------------------ //

  private static String b64(final byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static KeyPair genRsa(final int bits) throws Exception {
    final KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(bits);
    return gen.generateKeyPair();
  }

  /** Encodes a DER definite length (short or long form). */
  private static void derLength(final ByteArrayOutputStream out, final int len) {
    if (len < 0x80) {
      out.write(len);
      return;
    }
    int num = 0;
    for (int t = len; t > 0; t >>>= 8) {
      num++;
    }
    out.write(0x80 | num);
    for (int s = (num - 1) * 8; s >= 0; s -= 8) {
      out.write((len >>> s) & 0xFF);
    }
  }

  private static byte[] tlv(final int tag, final byte[] value) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(tag);
    derLength(out, value.length);
    out.write(value, 0, value.length);
    return out.toByteArray();
  }

  private static byte[] derInt(final BigInteger i) {
    return tlv(0x02, i.toByteArray());
  }

  /** Encodes an RSA key as a PKCS#1 RSAPrivateKey DER blob (9 INTEGERs). */
  private static byte[] toPkcs1(final RSAPrivateCrtKey k) {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    final byte[][] ints = {
        derInt(BigInteger.ZERO),
        derInt(k.getModulus()),
        derInt(k.getPublicExponent()),
        derInt(k.getPrivateExponent()),
        derInt(k.getPrimeP()),
        derInt(k.getPrimeQ()),
        derInt(k.getPrimeExponentP()),
        derInt(k.getPrimeExponentQ()),
        derInt(k.getCrtCoefficient()) };
    for (final byte[] e : ints) {
      body.write(e, 0, e.length);
    }
    return tlv(0x30, body.toByteArray());
  }

  private static File writeTemp(final String prefix, final String content)
      throws Exception {
    final File f = File.createTempFile(prefix, ".pem");
    f.deleteOnExit();
    final FileOutputStream out = new FileOutputStream(f);
    try {
      out.write(content.getBytes("UTF-8"));
    } finally {
      out.close();
    }
    return f;
  }

  private static RefreshingSSLContext buildFromFiles(final String keyPem)
      throws Exception {
    final File cert = writeTemp("ahb-cert", CERT_PEM);
    final File key = writeTemp("ahb-key", keyPem);
    return RefreshingSSLContext.newBuilder()
        .setType(RefreshingSSLContext.SourceType.FILES)
        .setCert(cert.getAbsolutePath())
        .setKey(key.getAbsolutePath())
        .setInterval(0)  // don't schedule a refresh, so no Timer is needed
        .build();
  }

  // ------------------------------------------------------------------ //
  // parsePKCS1Key (the rewritten PKCS#1 -> PKCS#8 path)                 //
  // ------------------------------------------------------------------ //

  @Test
  public void parsePkcs1RecoversFullCrtKey() throws Exception {
    final KeyPair kp = genRsa(2048);
    final RSAPrivateCrtKey orig = (RSAPrivateCrtKey) kp.getPrivate();
    final RSAPrivateKey parsed = Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parsePKCS1Key", b64(toPkcs1(orig)));

    // The JDK parses the whole key, so we get back a (fast) CRT key, not a
    // modulus/exponent-only key.
    assertTrue(parsed instanceof RSAPrivateCrtKey);
    assertEquals(orig.getModulus(), parsed.getModulus());
    assertEquals(orig.getPrivateExponent(), parsed.getPrivateExponent());
    final RSAPrivateCrtKey crt = (RSAPrivateCrtKey) parsed;
    assertEquals(orig.getPrimeP(), crt.getPrimeP());
    assertEquals(orig.getPrimeQ(), crt.getPrimeQ());
    assertEquals(orig.getCrtCoefficient(), crt.getCrtCoefficient());

    // And the recovered key actually works: sign with it, verify with the pub.
    final byte[] data = "asynchbase".getBytes("UTF-8");
    final Signature signer = Signature.getInstance("SHA256withRSA");
    signer.initSign(parsed);
    signer.update(data);
    final byte[] sig = signer.sign();
    final Signature verifier = Signature.getInstance("SHA256withRSA");
    verifier.initVerify(kp.getPublic());
    verifier.update(data);
    assertTrue(verifier.verify(sig));
  }

  @Test
  public void parsePkcs1AcrossKeySizes() throws Exception {
    // 1024/2048/4096 exercise short-form and 1- and 2-byte long-form DER
    // length prefixes in the PKCS#8 wrapper.
    for (final int bits : new int[] { 1024, 2048, 4096 }) {
      final RSAPrivateCrtKey orig = (RSAPrivateCrtKey) genRsa(bits).getPrivate();
      final RSAPrivateKey parsed = Whitebox.invokeMethod(
          RefreshingSSLContext.class, "parsePKCS1Key", b64(toPkcs1(orig)));
      assertEquals("bits=" + bits, orig.getModulus(), parsed.getModulus());
      assertEquals("bits=" + bits, bits, parsed.getModulus().bitLength());
    }
  }

  @Test(expected = Exception.class)
  public void parsePkcs1RejectsGarbage() throws Exception {
    Whitebox.invokeMethod(RefreshingSSLContext.class, "parsePKCS1Key",
        Base64.getEncoder().encodeToString(new byte[] { 0x01, 0x02, 0x03 }));
  }

  // ------------------------------------------------------------------ //
  // parsePKCS8Key                                                      //
  // ------------------------------------------------------------------ //

  @Test
  public void parsePkcs8RecoversKey() throws Exception {
    final KeyPair kp = genRsa(2048);
    // RSAPrivateKey.getEncoded() is PKCS#8.
    final RSAPrivateKey parsed = Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parsePKCS8Key", b64(kp.getPrivate().getEncoded()));
    assertEquals(((RSAPrivateKey) kp.getPrivate()).getModulus(), parsed.getModulus());
  }

  @Test
  public void pkcs1AndPkcs8YieldSameKey() throws Exception {
    final RSAPrivateCrtKey orig = (RSAPrivateCrtKey) genRsa(2048).getPrivate();
    final RSAPrivateKey fromPkcs1 = Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parsePKCS1Key", b64(toPkcs1(orig)));
    final RSAPrivateKey fromPkcs8 = Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parsePKCS8Key", b64(orig.getEncoded()));
    assertEquals(fromPkcs1.getModulus(), fromPkcs8.getModulus());
    assertEquals(fromPkcs1.getPrivateExponent(), fromPkcs8.getPrivateExponent());
  }

  // ------------------------------------------------------------------ //
  // splitPem / parseCert                                               //
  // ------------------------------------------------------------------ //

  @Test
  public void splitPemSeparatesSectionsAndStripsNewlines() throws Exception {
    final List<String> parts = Whitebox.invokeMethod(
        RefreshingSSLContext.class, "splitPem", CERT_PEM + "\n" + KEY_PKCS8_PEM);
    assertEquals(2, parts.size());
    assertTrue(parts.get(0).contains("BEGIN CERTIFICATE"));
    assertTrue(parts.get(1).contains("BEGIN PRIVATE KEY"));
    assertFalse(parts.get(0).contains("\n"));  // new-lines are removed
  }

  @Test
  public void parseCertReturnsNullForCommentOrEmpty() throws Exception {
    // The comment/empty branches short-circuit before touching the static
    // CertificateFactory (initialised in the ctor), so no instance is needed.
    assertNull(Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parseCert", "# not a certificate"));
    assertNull(Whitebox.invokeMethod(
        RefreshingSSLContext.class, "parseCert", ""));
  }

  // ------------------------------------------------------------------ //
  // End-to-end FILES load (runKeyAndCert + parseCert + splitPem + ...)  //
  // ------------------------------------------------------------------ //

  @Test
  public void buildsSslContextFromPkcs8KeyAndCert() throws Exception {
    assertNotNull(buildFromFiles(KEY_PKCS8_PEM).context());
  }

  @Test
  public void buildsSslContextFromPkcs1KeyAndCert() throws Exception {
    // Drives parsePKCS1Key through the real load path.
    assertNotNull(buildFromFiles(KEY_PKCS1_PEM).context());
  }
}
