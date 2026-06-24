/*
 * Copyright (C) 2013  The Async HBase Authors.  All rights reserved.
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
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.protobuf;  // This is a lie.

import java.lang.reflect.Field;

/**
 * Helper class to extract byte arrays from {@link ByteString} without copy.
 * <p>
 * Without this protobufs would force us to copy every single byte array out
 * of the objects de-serialized from the wire (which already do one copy, on
 * top of the copies the JVM does to go from kernel buffer to C buffer and
 * from C buffer to JVM buffer).
 * <p>
 * <strong>This class isn't part of the public API of AsyncHBase.</strong>
 * @since 1.5
 */
public final class ZeroCopyLiteralByteString {

  /**
   * The package-private {@code com.google.protobuf.ByteString$LiteralByteString}
   * class, or {@code null} if it can't be located (e.g. a future protobuf that
   * renames it).
   */
  private static final Class<?> LITERAL_BYTE_STRING_CLASS =
      findLiteralByteStringClass();

  /**
   * The {@code byte[] bytes} backing field of {@code LiteralByteString}, made
   * accessible, or {@code null} if reflection is unavailable.
   */
  private static final Field BYTES_FIELD =
      findBytesField(LITERAL_BYTE_STRING_CLASS);

  /** Private constructor so this class cannot be instantiated. */
  private ZeroCopyLiteralByteString() {
    throw new UnsupportedOperationException("Should never be here.");
  }

  /**
   * Wraps a byte array in a {@link ByteString} without copying it.
   * <p>
   * Uses the public {@link UnsafeByteOperations#unsafeWrap(byte[])} API
   * (protobuf 3.x), which performs a true zero-copy wrap.
   * @param array A byte array that must be considered read-only from there on.
   */
  public static ByteString wrap(final byte[] array) {
    return UnsafeByteOperations.unsafeWrap(array);
  }

  /**
   * Extracts the byte array backing the given {@link ByteString} without a copy
   * when possible.
   * <p>
   * As of protobuf 3.x {@code LiteralByteString} is a private nested class with
   * no public accessor for its backing array, so we read it reflectively.  If
   * that isn't possible (reflection blocked, unexpected {@link ByteString}
   * subtype such as a rope/bounded/NIO-backed one, or a future protobuf
   * layout), we fall back to a safe defensive copy via
   * {@link ByteString#toByteArray()}.  This keeps the call correct on every JDK
   * from 8 through 17+.
   * @param buf A buffer from which to extract the array.
   */
  public static byte[] zeroCopyGetBytes(final ByteString buf) {
    // Only an exact LiteralByteString backs its content 1:1 with its array;
    // subtypes (BoundedByteString, RopeByteString, ...) do not, so copy those.
    if (BYTES_FIELD != null && buf.getClass() == LITERAL_BYTE_STRING_CLASS) {
      try {
        return (byte[]) BYTES_FIELD.get(buf);
      } catch (IllegalAccessException e) {
        // Fall through to the safe copy below.
      }
    }
    return buf.toByteArray();
  }

  private static Class<?> findLiteralByteStringClass() {
    try {
      return Class.forName("com.google.protobuf.ByteString$LiteralByteString");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static Field findBytesField(final Class<?> klass) {
    if (klass == null) {
      return null;
    }
    try {
      final Field f = klass.getDeclaredField("bytes");
      f.setAccessible(true);
      return f;
    } catch (NoSuchFieldException | RuntimeException e) {
      // RuntimeException covers JDK 9+ InaccessibleObjectException.
      return null;
    }
  }

}
