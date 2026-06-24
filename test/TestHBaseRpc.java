/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

public class TestHBaseRpc extends BaseTestHBaseClient {
  private int default_timeout;

  @Before
  public void beforeLocal() throws Exception {
    timer.stop();
    when(regionclient.getHBaseClient()).thenReturn(client);
    Field rpc_timeoutField = client.getClass().getDeclaredField("rpc_timeout");
    rpc_timeoutField.setAccessible(true);
    rpc_timeoutField.set(client, 60000);
    default_timeout = 60000;
  }

  @Test
  public void setIdAndClient() throws Exception {
    GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    assertEquals(0, rpc.rpcId());
    assertNull(rpc.timeoutHandle());
    assertNull(rpc.regionClient());

    // set on a fresh RPC.
    rpc.setIdAndClient(42, regionclient);
    assertEquals(42, rpc.rpcId());
    assertNull(rpc.timeoutHandle());
    assertSame(regionclient, rpc.regionClient());
    verify(regionclient, never()).removeRpc(rpc, false);

    // retried on another client
    final RegionClient alt_client = mock(RegionClient.class);
    rpc.setIdAndClient(24, alt_client);
    assertEquals(24, rpc.rpcId());
    assertNull(rpc.timeoutHandle());
    assertSame(alt_client, rpc.regionClient());
    verify(regionclient, times(1)).removeRpc(rpc, false);
    verify(alt_client, never()).removeRpc(rpc, false);

    // nulled out on a reset
    rpc.setIdAndClient(0, null);
    assertEquals(0, rpc.rpcId());
    assertNull(rpc.timeoutHandle());
    assertNull(rpc.regionClient());
    verify(regionclient, times(1)).removeRpc(rpc, false);
    verify(alt_client, times(1)).removeRpc(rpc, false);

    // a timeout was set, want to cancel it
    rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setIdAndClient(42, regionclient);
    final Timeout timeout_handle = mock(Timeout.class);
    Field timeout_handleField = rpc.getClass().getDeclaredField("timeout_handle");
    timeout_handleField.setAccessible(true);
    timeout_handleField.set(rpc, timeout_handle);
    assertEquals(42, rpc.rpcId());
    assertSame(timeout_handle, rpc.timeoutHandle());
    assertSame(regionclient, rpc.regionClient());
    verify(regionclient, never()).removeRpc(rpc, false);
    verify(timeout_handle, never()).cancel();

    rpc.setIdAndClient(24, alt_client);
    assertEquals(24, rpc.rpcId());
    assertNull(rpc.timeoutHandle());
    assertSame(alt_client, rpc.regionClient());
    verify(regionclient, times(1)).removeRpc(rpc, false);
    verify(alt_client, never()).removeRpc(rpc, false);
    verify(timeout_handle, times(1)).cancel();
  }
  
  @Test
  public void enqueueTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    assertNull(rpc.timeoutHandle());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertEquals(1, timer.tasks.size());
    assertNotNull(rpc.timeoutHandle());
    assertEquals(default_timeout, rpc.getTimeout());
    assertEquals(default_timeout, (long)timer.tasks.get(0).getValue());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void enqueueTimeoutNullServer() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    //rpc.setIdAndClient(1, regionclient); // ID was never set
    assertNull(rpc.timeoutHandle());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    try {
      rpc.enqueueTimeout(regionclient);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertEquals(0, timer.tasks.size());
    assertNull(rpc.timeoutHandle());
    assertEquals(-1, rpc.getTimeout());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void enqueueTimeoutDifferentServer() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    assertNull(rpc.timeoutHandle());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    try {
      rpc.enqueueTimeout(mock(RegionClient.class));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertEquals(0, timer.tasks.size());
    assertNull(rpc.timeoutHandle());
    assertEquals(-1, rpc.getTimeout());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void enqueueTimeoutCustomTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setTimeout(42000);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    assertNull(rpc.timeoutHandle());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertEquals(1, timer.tasks.size());
    assertNotNull(rpc.timeoutHandle());
    assertEquals(42000, rpc.getTimeout());
    assertEquals(42000, (long)timer.tasks.get(0).getValue());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }

  @Test(expected = IllegalStateException.class)
  public void enqueueTimeoutAlreadyTimedout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setIdAndClient(1, regionclient);
    Field has_timedoutField = rpc.getClass().getDeclaredField("has_timedout");
    has_timedoutField.setAccessible(true);
    has_timedoutField.set(rpc, true);
    rpc.enqueueTimeout(regionclient);
  }
  
  @Test (expected = IllegalStateException.class)
  public void enqueueTimeoutNullRegionClient() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setIdAndClient(1, regionclient);
    rpc.enqueueTimeout(null);
  }
  
  @Test
  public void enqueueTimeoutZeroTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setTimeout(0);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    assertNull(rpc.timeoutHandle());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertNull(rpc.timeoutHandle());
    assertEquals(0, rpc.getTimeout());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }

  @Test
  public void enqueueTimeoutTimerShuttingDown() throws Exception {
    timer = mock(FakeTimer.class);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenThrow(new IllegalStateException("Shutdown!"));
    Field rpc_timeout_timerField = client.getClass().getDeclaredField("rpc_timeout_timer");
    rpc_timeout_timerField.setAccessible(true);
    rpc_timeout_timerField.set(client, timer);
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    assertNull(rpc.timeoutHandle());
    assertFalse(rpc.hasTimedOut());

    rpc.enqueueTimeout(regionclient);

    assertNull(rpc.timeoutHandle());
    assertEquals(default_timeout, rpc.getTimeout());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    verify(timer).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) {
    }
  }

  @Test
  public void callback() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    final Object response = new Object();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeoutHandle());
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(response);
    assertSame(response, deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackNullResult() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeoutHandle());
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(null);
    assertNull(deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackException() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeoutHandle());
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(new NonRecoverableException("Boo!"));
    try {
      deferred.join();
    } catch (NonRecoverableException e) { }
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }

  @Test
  public void callbackWithTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Timeout timeout_handle = mock(Timeout.class);
    final Deferred<Object> deferred = rpc.getDeferred();
    final Object response = new Object();
    Field timeout_handleField = rpc.getClass().getDeclaredField("timeout_handle");
    timeout_handleField.setAccessible(true);
    timeout_handleField.set(rpc, timeout_handle);
    assertTrue(rpc.hasDeferred());

    rpc.callback(response);
    assertSame(response, deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
    verify(timeout_handle).cancel();
  }
  
  @Test
  public void callbackWithoutDeferred() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Object response = new Object();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeoutHandle());
    assertFalse(rpc.hasDeferred());
    
    rpc.callback(response);
    assertEquals(4, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }

  @Test
  public void callbackWithTimeoutWithoutDeferred() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Timeout timeout_handle = mock(Timeout.class);
    final Object response = new Object();
    Field timeout_handleField = rpc.getClass().getDeclaredField("timeout_handle");
    timeout_handleField.setAccessible(true);
    timeout_handleField.set(rpc, timeout_handle);
    assertFalse(rpc.hasDeferred());

    rpc.callback(response);
    assertEquals(4, rpc.attempt);
    assertFalse(rpc.hasDeferred());
    verify(timeout_handle).cancel();
  }

  @Test
  public void timeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(rpc.timeoutHandle());
    
    assertNull(rpc.timeoutHandle());
    verify(regionclient).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) { }
  }
  
  @Test (expected = IllegalStateException.class)
  public void timeoutAlreadyTimedout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(rpc.timeoutHandle());
    // better not happen
    timer.tasks.get(0).getKey().run(rpc.timeoutHandle());
  }
  
  @Test
  public void timeoutDifferentHandle() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(mock(Timeout.class));
    
    assertNull(rpc.timeoutHandle());
    verify(regionclient).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) { }
  }

  @Test
  public void timeoutNulledRegionClient() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    rpc.enqueueTimeout(regionclient);
    Field region_clientField = rpc.getClass().getDeclaredField("region_client");
    region_clientField.setAccessible(true);
    region_clientField.set(rpc, (RegionClient)null);
    timer.tasks.get(0).getKey().run(rpc.timeoutHandle());

    assertNull(rpc.timeoutHandle());
    verify(regionclient, never()).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) {
    }
  }

  @Test
  public void getRetryDelay() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    assertEquals(400, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(600, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(800, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1000, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1016, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1032, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1064, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1128, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1256, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(1512, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(2024, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(3048, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(5096, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(9192, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(17384, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(33768, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(66536, rpc.getRetryDelay(0));
    ++rpc.attempt;
    assertEquals(132072, rpc.getRetryDelay(0));
  }

  @Test
  public void cancelTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.setIdAndClient(1, regionclient);
    rpc.enqueueTimeout(regionclient);
    assertNotNull(rpc.timeoutHandle());
    verify(regionclient, never()).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException ex) { }
    
    final Timeout timeout = rpc.timeoutHandle();
    rpc.cancelTimeout();
    assertNull(rpc.timeoutHandle());
    verify(timeout, times(1)).cancel();
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException ex) { }
  }
  
  @Test
  public void cancelTimeoutNeverSet() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    assertNull(rpc.timeoutHandle());
    verify(regionclient, never()).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException ex) { }
    
    rpc.cancelTimeout();
    assertNull(rpc.timeoutHandle());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException ex) { }
  }
}
