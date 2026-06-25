/*
 * Copyright (C) 2014-2018 The Async HBase Authors.  All rights reserved.
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
package org.hbase.async;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;


import org.hbase.async.HBaseRpc;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.ratelimiter.WriteRateLimiter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.powermock.reflect.Whitebox;

public class TestRegionClient extends BaseTestRegionClient {

  private MockedStatic<HBaseRpc> mockedHBaseRpc;

  private MockedStatic<Channels> mockedChannels;

  @Before
  public void setUpStaticMocks() {
    mockedHBaseRpc = Mockito.mockStatic(HBaseRpc.class);
    mockedChannels = Mockito.mockStatic(Channels.class);
  }

  @After
  public void tearDownStaticMocks() {
    if (mockedChannels != null) mockedChannels.close();
    if (mockedHBaseRpc != null) mockedHBaseRpc.close();
  }

  @Test
  public void ctor() throws Exception {
    assertNotNull(new RegionClient(hbase_client, null, "localhost"));
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullClient() throws Exception {
    new RegionClient(null, null, "localhost");
  }

  @Test
  public void getClosestRowBefore() throws Exception {
    final RegionClient rclient = Mockito.spy(
        new RegionClient(hbase_client, null, "localhost"));
    rclient.getClosestRowBefore(region, TABLE, KEY, FAMILY);
    // getClosestRowBefore() builds an internal GetClosestRowBefore RPC and
    // hands it to sendRpc(); we only care that sendRpc was invoked once.
    verify(rclient).sendRpc(any(HBaseRpc.class));
  }
  
  @Test
  public void getRemoteAddressChanNotSet() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    
    assertNull(Whitebox.getInternalState(rclient, "chan"));
    assertNull(rclient.getRemoteAddress());
  }
  
  @Test
  public void getRemoteAddressChanSet() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);
    Mockito.when(chan.getRemoteAddress().toString()).thenReturn("127.0.0.1");
    Mockito.when(rclient.getRemoteAddress()).thenCallRealMethod();
    
    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    String addy = rclient.getRemoteAddress();
    assertNotNull(addy);
    assertEquals(addy, "127.0.0.1");
  }
  
  @Test
  public void exceptionCaught() throws Exception {
    when(chan.isOpen()).thenReturn(true);
    final RegionClient rclient = Mockito.spy(new RegionClient(
        hbase_client, null, "localhost"));
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);
    final ExceptionEvent event = new DefaultExceptionEvent(chan,
        new RuntimeException("Boo!"));

    rclient.exceptionCaught(null, event);

    verify(rclient, never()).cleanup(chan);
    mockedChannels.verify(() -> Channels.close(chan));
  }

  @Test
  public void exceptionCaughtChNotOpen() throws Exception {
    final RegionClient rclient = Mockito.spy(new RegionClient(
        hbase_client, null, "localhost"));
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);
    final ExceptionEvent event = new DefaultExceptionEvent(chan,
        new RuntimeException("Boo!"));

    rclient.exceptionCaught(null, event);

    verify(rclient, times(1)).cleanup(chan);
    mockedChannels.verify(() -> Channels.close(chan), never());
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullEvent() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    rclient.exceptionCaught(null, null);
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullChannel() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    final ExceptionEvent event = new DefaultExceptionEvent(null, 
        new RuntimeException("Boo!"));
    rclient.exceptionCaught(null, event);
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullException() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    final ExceptionEvent event = new DefaultExceptionEvent(chan, null);
    rclient.exceptionCaught(null, event);
  }
  
  @Test
  public void exceptionCaughtDifferentChannel() throws Exception {
    when(chan.isOpen()).thenReturn(true);
    // honey badger don't care; apparently we can call this with any old channel.
    final Channel ch = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
    when(ch.isOpen()).thenReturn(true);
    final RegionClient rclient = Mockito.spy(new RegionClient(
        hbase_client, null, "localhost"));
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);
    final ExceptionEvent event = new DefaultExceptionEvent(ch,
        new RuntimeException("Boo!"));

    rclient.exceptionCaught(null, event);

    verify(rclient, never()).cleanup(ch);
    mockedChannels.verify(() -> Channels.close(ch));
  }

  @Test
  public void exceptionCaughtDifferentChannelNotOpen() throws Exception {
    // honey badger don't care; apparently we can call this with any old channel.
    final Channel ch = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
    final RegionClient rclient = Mockito.spy(new RegionClient(
        hbase_client, null, "localhost"));
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);
    final ExceptionEvent event = new DefaultExceptionEvent(ch,
        new RuntimeException("Boo!"));

    rclient.exceptionCaught(null, event);

    verify(rclient, times(1)).cleanup(ch);
    mockedChannels.verify(() -> Channels.close(ch), never());
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void channelDisconnected() throws Exception {
    RegionClient rclient = Mockito.spy(new RegionClient(
        hbase_client, null, "localhost"));
    Whitebox.getField(RegionClient.class, "chan").set(rclient, chan);

    when(cse.getChannel()).thenReturn(chan);
    // Prevent/stub logic in super.method()
    Mockito.doNothing().when((ReplayingDecoder)rclient)
      .channelDisconnected(ctx, cse);
    Mockito.doNothing().when(rclient).cleanup(chan);
    Mockito.doCallRealMethod().when(rclient).channelDisconnected(ctx, cse);

    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    rclient.channelDisconnected(ctx, cse);
    assertNull(Whitebox.getInternalState(rclient, "chan"));
  }
  
  @Test
  public void channelClosed() throws Exception {
    RegionClient rclient = Mockito.spy(
        new RegionClient(hbase_client, null, "localhost"));
    assertNotNull(rclient);
    rclient.becomeReady(chan, SERVER_VERSION_UNKNOWN);

    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    rclient.channelClosed(ctx, cse);

    assertNull(Whitebox.getInternalState(rclient, "chan"));
    // channelClosed() passes cse.getChannel() (an unstubbed mock event, so
    // null) to cleanup(); nullable() matches the null argument.
    verify(rclient).cleanup(Mockito.nullable(Channel.class));
  }

  @Test
  public void channelConnected95() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ChannelBuffer header = mock(ChannelBuffer.class);
    
    hbase_client.has_root = false;
    when(cse.getChannel()).thenReturn(chan);
    Mockito.doReturn(header).when(rclient).header095();
    Whitebox.getField(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

    rclient.channelConnected(ctx, cse);

    verify(rclient).header095();
    verify(rclient).becomeReady(chan,
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
  }
  
  @Test
  public void channelConnectedCDH3b3() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ChannelBuffer header = mock(ChannelBuffer.class);

    hbase_client.has_root = true;
    when(cse.getChannel()).thenReturn(chan);
    // System.class can't be mocked by Mockito; drive the real property instead.
    final String prev = System.getProperty("org.hbase.async.cdh3b3");
    System.setProperty("org.hbase.async.cdh3b3", "some value");
    try {
      Mockito.doNothing().when(rclient).helloRpc(chan, header);
      Mockito.doReturn(header).when(rclient).headerCDH3b3();
      Whitebox.getField(RegionClient.class, "hbase_client")
        .set(rclient, hbase_client);
      Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

      rclient.channelConnected(ctx, cse);

      verify(rclient).headerCDH3b3();
      verify(rclient).helloRpc(chan, header);
    } finally {
      if (prev == null) {
        System.clearProperty("org.hbase.async.cdh3b3");
      } else {
        System.setProperty("org.hbase.async.cdh3b3", prev);
      }
    }
  }
  
  @Test
  public void channelConnected090() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ChannelBuffer header = mock(ChannelBuffer.class);

    hbase_client.has_root = true;
    when(cse.getChannel()).thenReturn(chan);
    // System.class can't be mocked by Mockito; drive the real property instead.
    final String prev = System.getProperty("org.hbase.async.cdh3b3");
    System.clearProperty("org.hbase.async.cdh3b3");
    try {
      Mockito.doNothing().when(rclient).helloRpc(chan, header);
      Mockito.doReturn(header).when(rclient).header090();
      Whitebox.getField(RegionClient.class, "hbase_client")
        .set(rclient, hbase_client);
      Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

      rclient.channelConnected(ctx, cse);

      verify(rclient).header090();
      verify(rclient).helloRpc(chan, header);
    } finally {
      if (prev != null) {
        System.setProperty("org.hbase.async.cdh3b3", prev);
      }
    }
  }

  @Test
  public void channelConnected94Secure() throws Exception {
    try (MockedConstruction<SecureRpcHelper94> mockSecureRpcHelper94 = Mockito.mockConstruction(SecureRpcHelper94.class)) {
      RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
      Config config = new Config();
      config.overrideConfig("hbase.security.auth.enable", "true");
      config.overrideConfig("hbase.security.auth.94", "true");
      when(hbase_client.getConfig()).thenReturn(config);

      hbase_client.has_root = false;
      when(cse.getChannel()).thenReturn(chan);
      Whitebox.getField(RegionClient.class, "hbase_client")
          .set(rclient, hbase_client);
      Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

      rclient.channelConnected(ctx, cse);

      verify(mockSecureRpcHelper94.constructed().get(0), times(1)).sendHello(chan);
      verify(rclient, never()).helloRpc(eq(chan), any(ChannelBuffer.class));
      verify(rclient, never()).becomeReady(chan,
          RegionClient.SERVER_VERSION_095_OR_ABOVE);
    }
  }

  @Test
  public void channelConnected96Secure() throws Exception {
    try (MockedConstruction<SecureRpcHelper96> mockSecureRpcHelper96 = Mockito.mockConstruction(SecureRpcHelper96.class)) {
      RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
      Config config = new Config();
      config.overrideConfig("hbase.security.auth.enable", "true");
      when(hbase_client.getConfig()).thenReturn(config);

      hbase_client.has_root = false;
      when(cse.getChannel()).thenReturn(chan);
      Whitebox.getField(RegionClient.class, "hbase_client")
          .set(rclient, hbase_client);
      Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

      rclient.channelConnected(ctx, cse);

      verify(mockSecureRpcHelper96.constructed().get(0), times(1)).sendHello(chan);
      verify(rclient, never()).helloRpc(eq(chan), any(ChannelBuffer.class));
      verify(rclient, never()).becomeReady(chan,
          RegionClient.SERVER_VERSION_095_OR_ABOVE);
    }
  }

  @Test
  public void channelConnected96SplitMetaSecure() throws Exception {
    try (MockedConstruction<SecureRpcHelper96> mockSecureRpcHelper96 = Mockito.mockConstruction(SecureRpcHelper96.class)) {
      RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
      Config config = new Config();
      config.overrideConfig("hbase.security.auth.enable", "true");
      when(hbase_client.getConfig()).thenReturn(config);

      hbase_client.has_root = true;
      hbase_client.split_meta = true;
      when(cse.getChannel()).thenReturn(chan);
      Whitebox.getField(RegionClient.class, "hbase_client")
          .set(rclient, hbase_client);
      Mockito.doCallRealMethod().when(rclient).channelConnected(ctx, cse);

      rclient.channelConnected(ctx, cse);

      verify(mockSecureRpcHelper96.constructed().get(0), times(1)).sendHello(chan);
      verify(rclient, never()).helloRpc(eq(chan), any(ChannelBuffer.class));
      verify(rclient, never()).becomeReady(chan,
          RegionClient.SERVER_VERSION_095_OR_ABOVE);
    }
  }
  
  @Test (expected=NonRecoverableException.class)
  public void decodeHbase95orAboveNoCallId() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    ChannelBuffer buf = mock(ChannelBuffer.class);
    RPCPB.ResponseHeader header = mock(RPCPB.ResponseHeader.class);
      
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Integer, HBaseRpc> mcmap = mock(ConcurrentHashMap.class);
    Whitebox.getField(RegionClient.class, "rpcs_inflight").set(rclient, mcmap);
    Whitebox.getField(RegionClient.class, "server_version")
      .set(rclient, RegionClient.SERVER_VERSION_095_OR_ABOVE);

    mockedHBaseRpc.when(() -> HBaseRpc.readProtobuf(buf, RPCPB.ResponseHeader.PARSER))
      .thenReturn(header);
    Mockito.when(buf.readInt()).thenReturn(0);
    Mockito.when(header.hasCallId()).thenReturn(false);
    Mockito.doCallRealMethod().when(rclient).decode((ChannelHandlerContext)any(),
          (Channel)any(), (ChannelBuffer)any(), (VoidEnum)any());

    assertNull(rclient.decode(null, chan, buf, (VoidEnum)null));
  }

  @Test
  public void decodeHbase92orAboveRpcNotNull() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    Field rate_limiterField = rclient.getClass().getDeclaredField("rate_limiter");
    rate_limiterField.setAccessible(true);
    rate_limiterField.set(rclient, mock(WriteRateLimiter.class));
    ChannelBuffer buf = mock(ChannelBuffer.class);
    NotServingRegionException blowzup = mock(NotServingRegionException.class);
    HBaseRpc rpc = mock(HBaseRpc.class);
    RegionInfo region = mock(RegionInfo.class);

    ConcurrentHashMap<Integer, HBaseRpc> mcmap =
        new ConcurrentHashMap<Integer, HBaseRpc>(1);
    mcmap.put(0, rpc);
    Whitebox.getField(RegionClient.class, "rpcs_inflight").set(rclient, mcmap);
    Whitebox.getField(RegionClient.class, "hbase_client")
        .set(rclient, hbase_client);
    Whitebox.getField(RegionClient.class, "server_version")
        .set(rclient, RegionClient.SERVER_VERSION_092_OR_ABOVE);

    Mockito.when(buf.readInt()).thenReturn(0);
    Mockito.when(rpc.getRegion()).thenReturn(region);
    Mockito.when(region.name()).thenReturn(Bytes.fromInt(1337));
    Mockito.doNothing().when(hbase_client).handleNSRE(Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.doReturn(blowzup).when(rclient).deserialize(buf, rpc);

    Mockito.doCallRealMethod().when(rclient).decode((ChannelHandlerContext)any(),
        (Channel)any(), (ChannelBuffer)any(), (VoidEnum)any());

    assertNull(rclient.decode(null, chan, buf, (VoidEnum)null));
  }
  
  @Test
  public void encode() throws Exception {
      // TODO
  }
  
  @Test
  public void deserialize() throws Exception {
      // TOD
  }
  
  @Test
  public void numberOfKeyValuesAhead() throws Exception {
      // TODO
  }
  
  @Test
  public void parseResult() throws Exception {
      // TODO
  }
  
  @Test
  public void parseResults() throws Exception {
      // TODO
  }

  @Test(expected = InvalidResponseException.class)
  public void badResponse() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    // Whitebox.invokeMethod unwraps the InvocationTargetException that plain
    // Method.invoke would wrap around the InvalidResponseException.
    Whitebox.invokeMethod(rclient, "badResponse", "ZOMG ERRMSG");
  }

  @Test
  public void commonHeader() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client, null, "localhost");
    final byte[] buf = new byte[42];

    Method commonHeaderMethod = rclient.getClass().getDeclaredMethod("commonHeader", buf.getClass(), HRPC3.getClass());
    commonHeaderMethod.setAccessible(true);
    ChannelBuffer commonHeader = (ChannelBuffer)commonHeaderMethod.invoke(rclient, buf, HRPC3);

    assertNotNull(commonHeader);
  }

  @Test
  public void header090() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    final byte[] buf = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
    final String klass = "org.apache.hadoop.io.Writable";
    ChannelBuffer header = mock(ChannelBuffer.class);

    Mockito.doReturn(header).when(rclient).commonHeader(buf, HRPC3);
    Mockito.when(header.writerIndex()).thenReturn(0);
    Mockito.doCallRealMethod().when(rclient).header090();

    Method header090Method = rclient.getClass().getDeclaredMethod("header090");
    header090Method.setAccessible(true);
    ChannelBuffer header090 = (ChannelBuffer)header090Method.invoke(rclient);

    verify(rclient, Mockito.atMost(1)).commonHeader(buf, HRPC3);
    verify(header, Mockito.atMost(2)).writerIndex();
    verify(header, Mockito.atLeast(1)).writerIndex(Mockito.anyInt());
    verify(header, Mockito.atMost(3)).writeShort(klass.length());
    verify(header, Mockito.atMost(3)).writeBytes(Bytes.ISO88591(klass));
    verify(header, Mockito.atMost(1)).setInt(Mockito.eq(5), Mockito.anyInt());

    assertNotNull(header090);
  }

  @Test
  public void header092() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    final byte[] buf = new byte[4 + 1 + 4 + 1 + 44];
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    ChannelBuffer header = mock(ChannelBuffer.class);

    Mockito.doReturn(header).when(rclient).commonHeader(buf, HRPC3);
    Mockito.when(header.writerIndex()).thenReturn(0);
    Mockito.doCallRealMethod().when(rclient).header092();

    Method header092Method = rclient.getClass().getDeclaredMethod("header092");
    header092Method.setAccessible(true);
    ChannelBuffer header092 = (ChannelBuffer)header092Method.invoke(rclient);

    verify(rclient, Mockito.atMost(1)).commonHeader(buf, HRPC3);
    verify(header, Mockito.atMost(2)).writerIndex();
    verify(header, Mockito.atLeast(1)).writerIndex(Mockito.anyInt());
    verify(header, Mockito.atMost(1)).writeByte(klass.length());
    verify(header, Mockito.atMost(1)).writeBytes(Bytes.ISO88591(klass));
    verify(header, Mockito.atMost(1)).setInt(Mockito.eq(5), Mockito.anyInt());

    assertNotNull(header092);
  }

  @Test
  public void headerCDH3b3() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    byte[] user = Bytes.UTF8("some value");
    byte[] buf = new byte[4 + 1 + 4 + 4 + user.length];
    ChannelBuffer header = mock(ChannelBuffer.class);
    // System.class can't be mocked by Mockito; drive the real property instead.
    final String prev = System.getProperty("user.name");
    System.setProperty("user.name", "some value");
    try {
      Mockito.doReturn(header).when(rclient).commonHeader(buf, HRPC3);
      Mockito.doCallRealMethod().when(rclient).headerCDH3b3();

      ChannelBuffer headerCDH3b3 =
          (ChannelBuffer) Whitebox.invokeMethod(rclient, "headerCDH3b3");

      verify(rclient, Mockito.atMost(1)).commonHeader(buf, HRPC3);
      verify(header, Mockito.atMost(2)).writeInt(Mockito.anyInt());
      verify(header, Mockito.atMost(1)).writeBytes(user);

      assertNotNull(headerCDH3b3);
    } finally {
      if (prev == null) {
        System.clearProperty("user.name");
      } else {
        System.setProperty("user.name", prev);
      }
    }
  }

}
