/*
 * Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
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
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;


import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.TimerTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentMatcher;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

public final class TestNSREs extends BaseTestHBaseClient {
  private GetRequest[] dummy_gets;
  private GetRequest trigger;
  private Counter num_nsres;
  private ConcurrentSkipListMap<byte[], ArrayList<HBaseRpc>> got_nsre;
  private ArrayList<KeyValue> row;
  /** Static mock of GetRequest, held open by MockProbe() for the test. */
  private MockedStatic<GetRequest> mockedGetRequest;

  @After
  public void afterNSRE() {
    if (mockedGetRequest != null) {
      mockedGetRequest.close();
      mockedGetRequest = null;
    }
  }

  @Before
  public void beforeNSRE() throws Exception {
    row = new ArrayList<KeyValue>(1);
    row.add(KV);
    num_nsres = Whitebox.getInternalState(client, "num_nsres");
    num_nsre_rpcs = Whitebox.getInternalState(client, "num_nsre_rpcs");
    got_nsre = Whitebox.getInternalState(client, "got_nsre");
    // The base class mock-constructs HashedWheelTimer, so client.timer is an
    // inert mock whose newTimeout() never runs the scheduled task.  The NSRE
    // probe retry is driven through that timer, so inject the immediate-firing
    // FakeTimer for the tests (simpleNSRE/doubleNSRE) that rely on the probe
    // actually being scheduled.  Tests that need different timer behaviour
    // (a mock timer to suppress retries, FakeTaskTimer, or a fresh FakeTimer in
    // setupMultiNSRE) overwrite this field themselves after this method runs.
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, timer);
  }
  
  @Test
  public void simpleGet() throws Exception {
    // Just a simple test, no tricks, no problems, to verify we can
    // successfully mock out a complete get.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  @Test
  public void simpleNSRE() throws Exception {
    // Attempt to get a row, get an NSRE back, do a META lookup,
    // find the new location, try again, succeed.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    // First access triggers an NSRE.
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        // We completely stub out the RegionClient, which normally does this.
        client.handleNSRE(get, get.getRegion().name(),
                          new NotServingRegionException("test", get),
                          REMOTE_ADDRESS);
        return null;
      }
    }).when(regionclient).sendRpc(get);
    // So now we do a meta lookup.
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(), anyBytes()))
      .thenAnswer(newDeferred(metaRow()));
    // This is the client where the region moved to after the NSRE.
    final RegionClient newregionclient = mock(RegionClient.class);
    doReturn(newregionclient).when(client).newClient(anyString(), anyInt());
    when(newregionclient.isAlive()).thenReturn(true);
    // Answer the "exists" probe we use to check if the NSRE is still there.
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        exist.getDeferred().callback(true);
        return null;
      }
    }).when(newregionclient).sendRpc(any(GetRequest.class));
    // Answer our actual get request.
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(newregionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  @Test
  public void doubleNSRE() throws Exception {
    // Attempt to get a row, get an NSRE back, do a META lookup,
    // find the new location, get another NSRE that directs us to another
    // region, do another META lookup, try again, succeed.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    // First access triggers an NSRE.
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        // We completely stub out the RegionClient, which normally does this.
        client.handleNSRE(get, get.getRegion().name(),
                          new NotServingRegionException("test 1", get),
                          REMOTE_ADDRESS);
        return null;
      }
    }).when(regionclient).sendRpc(get);
    // So now we do a meta lookup.
    when(metaclient.isAlive()).thenReturn(true);  // [1]
    // The lookup tells us that now this key is in another daughter region.
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(), anyBytes()))
      .thenAnswer(newDeferred(metaRow(KEY, HBaseClient.EMPTY_ARRAY)));  // [2]
    // This is the client of the daughter region.
    final RegionClient newregionclient = mock(RegionClient.class);
    doReturn(newregionclient).when(client).newClient(anyString(), anyInt());
    when(newregionclient.isAlive()).thenReturn(true);
    // Make the exist probe fail with another NSRE.
    doAnswer(new Answer<Object>() {
      private byte attempt = 0;

      @SuppressWarnings("fallthrough")
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        switch (attempt++) {
          case 0:  // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                              new NotServingRegionException("test 2", exist),
                              REMOTE_ADDRESS);
            break;
          case 1:  // Second attempt succeeds.
          case 2:  // First probe succeeds.
            exist.getDeferred().callback(true);
            break;
          default:
            throw new AssertionError("Shouldn't be here");
        }
        return null;
      }
    }).when(newregionclient).sendRpc(any(GetRequest.class));
    // Do a second meta lookup (behavior already set at [1]).
    // The second lookup returns the same daughter region (re-use [2]).

    // Answer our actual get request.
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(newregionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  @Test
  public void alreadyNSREdRegion() throws Exception {
    // This test is to reproduce the retrial of RPC that was to a Region which
    // is known as NSRE at the time of initial sending. When a RPC is assigned
    // to a region that is already known as NSRE at the time to initial sent
    // (sendRpcToRegion), handleNSRE will be called, but at the same time in
    // the code a RetryRpc() callback is attached which will resend the RPC
    // even after the RPC succeeded when the NSRE is finished. This will be
    // mainly problematic with AtomicIncrementRequests.

    // mainGet is the RPC that will exhibit the above said behaviour of
    // resending it twice both the times returning success.
    final GetRequest mainGet = new GetRequest(TABLE, KEY);
    // triggerGet RPC is the RPC that will be used to trigger the NSRE for the
    // region, so the behaviour of RegionClient for this RPC would be to
    // return NSRE the first time and then for second time it will be called
    // back with result
    final GetRequest triggerGet = new GetRequest(TABLE, KEY);
    // Since the both the RPCs are same, the below is the result for them
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    // Always this region client will be always returns true
    when(regionclient.isAlive()).thenReturn(true);

    // The region client's behaviour for the triggerRpc, as mentioned above
    // this RPC is mainly used to invalidate the region cache of the client
    // so this will make the knownToBeNSREd to return true for the region
    doAnswer(new Answer<Object>() {
      private int attempt = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GetRequest triggerGet = (GetRequest)args[0];
        switch (attempt++) {
          case 0:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(triggerGet, triggerGet.getRegion().name(),
                new NotServingRegionException("Trigger NSRE", triggerGet),
                REMOTE_ADDRESS);
            break;
          case 1:
            // trigger the callback with the result
            triggerGet.callback(row);
            break;
          default:
            throw new AssertionError("Can Never Happen");
        }
        return null;
      }
    }).when(regionclient).sendRpc(eq(triggerGet));


    // Now since the handleNSRE function, this will create a probe RPC and
    // will invalidate the region cache before retry of the probe RPC.  So we
    // will configure the meta_client to return this region for the look up
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(),
        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    // This will make sure that whenever region lookup happens the same
    // region client, on which we have stubbed the calls
    doReturn(regionclient).when(client).newClient(anyString(), anyInt());


    // Now we write the NSRE logic for the probe and hence we defined the
    // argument matcher for things other than the original get Request and
    // trigger get Request
    doAnswer(new Answer<Object>() {
      private int attempt = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest)args[0];
        switch (attempt++) {
          case 0:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                new NotServingRegionException("exist 1", exist),
                REMOTE_ADDRESS);
            break;
          case 1:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                new NotServingRegionException("exist 2", exist),
                REMOTE_ADDRESS);
            break;
          case 2:
            // NSRE cleared here, start the callback chain for exist RPC
            exist.callback(null);
            break;
          default:
            // This should never happen
            throw new AssertionError("Never Happens");
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat((ArgumentMatcher<HBaseRpc>)that -> that != mainGet && that != triggerGet));

    // Now the class stubbing for the mainGet RPC, whenever the call
    // is made for this RPC we just start the callback chain of the RPC.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest getMain = (GetRequest)args[0];
        // stubbing out the entire decode method in the region client
        getMain.callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(eq(mainGet));

    // Now we swap the timer in client with our taskTimer which helps in making
    // the mainGet request during the period of wait for probe, in which case
    // the code path for the alreadyNSREd region will kick in.
    FakeTaskTimer taskTimer = new FakeTaskTimer();
    Field timerField2 = client.getClass().getDeclaredField("timer");
    timerField2.setAccessible(true);
    HashedWheelTimer originalTimer = (HashedWheelTimer)timerField2.get(client);
    Field timerField1 = client.getClass().getDeclaredField("timer");
    timerField1.setAccessible(true);
    timerField1.set(client, taskTimer);


    // Code for execution of the test start the execution of triggerGet, this
    // will create the probe RPC now the region will be in a NSREd state
    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(triggerGet);
    // now execute the mainGet
    Deferred<ArrayList<KeyValue>> mainRpcDeferred = client.get(mainGet);
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, originalTimer);
    // now start the task that was paused
    boolean execute = taskTimer.continuePausedTask();
    assertTrue(execute);

    // Check the return result is same for trigger
    assertSame(row, triggerRpcDeferred.joinUninterruptibly());
    // For the trigger the RPC is sent only twice
    // once for the initial trigger for the NSRE
    // other is the final time when NSRE is cleared
    verify(regionclient, times(2)).sendRpc(triggerGet);


    // Check the return result is same for main
    assertSame(row, mainRpcDeferred.joinUninterruptibly());
    // Number of times this RPC is sent to regionServer
    verify(regionclient, times(1)).sendRpc(mainGet);
  }

  @Test
  public void probeRpcTooManyRetriesCallBack() throws Exception {
    // This test is demonstrate that when probe RPC expires with the number of
    // tries (it will happen in around approx 10 sec) its call back chain will
    // start executing, in which case all the RPCs in that are in the NSRE
    // list will be called sendRpcToRegion assuming that NSRE is cleared. But
    // in this path if the first few RPC's response returns with region being
    // NSREd before the other RPC's client.sendRpc is triggered (this is quite
    // possible if above 1000 RPCs are waiting on NSRE because these RPCs may
    // be waiting on meta region lookup and before this clears up, the first
    // few RPCs may have returned NSRE Exception from the region server) all
    // the remaining RPCs will go through knownToBeNSREd codepath in which
    // retryRpc callback will be added to these RPC's deferred in which case
    // all of them will be resent again after the clearing of NSRE. This can
    // be disastrous in the case of probe RPC getting expired a few number of
    // times.


    // Number of times the probe RPC should expire.
    final int probe_expire_count = 5;

    // dummyGet[]: These are the getRequests for which the RetryRpc()
    // callback will be attached
    final GetRequest[] dummyGet = {new GetRequest(TABLE, KEY),
        new GetRequest(TABLE, KEY),
        new GetRequest(TABLE, KEY)};

    // triggerGet: This RPC is used to trigger the initial NSRE and also used
    // to pause the probe execution by the FakeTaskTimer
    final GetRequest triggerGet = new GetRequest(TABLE, KEY);
    // Since the both the RPCs are same, the below is the result for them
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    // The Timers which will be swapped to simulate the pause for probe RPC
    final FakeTaskTimer taskTimer = new FakeTaskTimer();
    Field timerField2 = client.getClass().getDeclaredField("timer");
    timerField2.setAccessible(true);
    HashedWheelTimer originalTimer = (HashedWheelTimer)timerField2.get(client);

    // Always this region client will be always returns true.
    when(regionclient.isAlive()).thenReturn(true);

    // Now since the handleNSRE function, this will create a probe RPC and
    // will invalidate the region cache before retry of the probe RPC.  So we
    // will configure the meta_client to return this region for the look up.
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(),
        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    // This will make sure that whenever region lookup happens the same
    // region client, on which we have stubbed the calls.
    doReturn(regionclient).when(client).newClient(anyString(), anyInt());

    // behaviour for the triggerGet
    doAnswer(new Answer<Object>() {
      private int attempt = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GetRequest triggerGet = (GetRequest)args[0];
        attempt++;
        if (attempt <= probe_expire_count + 1) {
          Field timerField = client.getClass().getDeclaredField("timer");
          timerField.setAccessible(true);
          timerField.set(client, taskTimer);
          // We stub out the RegionClient, which normally does this.
          client.handleNSRE(triggerGet, triggerGet.getRegion().name(),
              new NotServingRegionException("Trigger NSRE", triggerGet),
              REMOTE_ADDRESS);
        } else if (attempt == probe_expire_count + 2) {
          // this is the case where NSRE is cleared
          // trigger the callback with the result
          triggerGet.callback(row);
        } else {
          throw new AssertionError("Can Never Happen");
        }
        return null;
      }
    }).when(regionclient).sendRpc(eq(triggerGet));


    // Now we write the NSRE logic for the probe and hence we defined the
    // argument matcher for things other than the original GetRequest and
    // trigger GetRequest.
    doAnswer(new Answer<Object>() {
      private int attempt = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest)args[0];
        attempt++;
        if (attempt < (probe_expire_count * 10 + 4)) {
          // We stub out the RegionClient, which normally does this.
          client.handleNSRE(exist, exist.getRegion().name(),
              new NotServingRegionException("exist 1", exist),
              REMOTE_ADDRESS);
        } else if (attempt == (probe_expire_count * 10 + 4)) {
          // NSRE on the region is cleared here
          exist.callback(null);
        } else {
          // This should never happen
          throw new AssertionError("Never Happens");
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat((ArgumentMatcher<HBaseRpc>)that -> that != dummyGet[0] && that != triggerGet
        && that != dummyGet[1] && that != dummyGet[2]));


    // Now the class stubbing for the dummyGet RPC, whenever the call
    // is made for this RPC we just start the callback chain of the RPC.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest dummyGet = (GetRequest)args[0];
        // stubbing out the entire decode method in the region client
        dummyGet.callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(argThat((ArgumentMatcher<HBaseRpc>)that -> (that == dummyGet[0]
        || that == dummyGet[1]
        || that == dummyGet[2])));


    // Main test code starts here
    // Start the execution of triggerGet, this will create the probe RPC
    // now the region will be in a NSREd state
    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(triggerGet);

    // execute the dummyRpcs now
    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>>[] dummyRpcDeferred = new Deferred[]{
        client.get(dummyGet[0]),
        client.get(dummyGet[1]),
        client.get(dummyGet[2])};

    Field timerField1 = client.getClass().getDeclaredField("timer");
    timerField1.setAccessible(true);
    timerField1.set(client, originalTimer);
    int taskTimerPauses = 0;
    while (taskTimer.continuePausedTask()) {
      taskTimerPauses++;
      Field timerField = client.getClass().getDeclaredField("timer");
      timerField.setAccessible(true);
      timerField.set(client, originalTimer);
    }

    // See the mock of regionclient.sendRpc method for this RPC for the
    // explanation for this
    verify(regionclient, times(probe_expire_count + 2)).sendRpc(triggerGet);
    // TaskTimer will be paused probe_expire_count + 1 times
    assertEquals(probe_expire_count + 1, taskTimerPauses);

    // Output of the triggerRpc
    assertSame(row, triggerRpcDeferred.joinUninterruptibly());

    for (int i = 0;i < 3;i++) {
      // Check the output is same
      assertSame(row, dummyRpcDeferred[i].join());
      // Check the number of times RPC is sent to region client this will be
      // equals to probe_expire_count for each RetryRpc during failure
      //  + 1 after the NSRE is cleared
      verify(regionclient, times(1)).sendRpc(dummyGet[i]);
    }
  }

  @Test
  public void recoverOnTrigger() throws Exception {
    final int trigger_retries = 5;
    // probes all fail but the trigger will succeed at one point
    final FakeTimer timer = setupMultiNSRE(trigger_retries,
        client.getConfig().getInt("hbase.client.retries.number") + 2, false);

    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(trigger);

    // execute the dummyRpcs now
    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>>[] dummyRpcDeferred = new Deferred[]{
        client.get(dummy_gets[0]),
        client.get(dummy_gets[1]),
        client.get(dummy_gets[2])};

    for (int i = 0;i < 3;i++) {
      // Check the output is same
      assertSame(row, dummyRpcDeferred[i].join());
      // Check the number of times RPC is sent to region client this will be
      // equals to probe_expire_count for each RetryRpc during failure
      //  + 1 after the NSRE is cleared
      verify(regionclient, times(1)).sendRpc(dummy_gets[i]);
    }

    assertSame(row, triggerRpcDeferred.join());
    assertEquals(105, timer.tasks.size());

//    Long last = 400L;
//    int attempt = 1;
//    for (Map.Entry<TimerTask, Long> task : timer.tasks) {
//      assertEquals(last, task.getValue());
//      if (last >= 2024) {
//        last = 400L;
//        attempt = 0;
//      } else if (last < 1000) {
//        last += 200;
//      } else {
//        last = (long)1000 + (1 << attempt);
//      }
//      attempt++;
//    }
    
    verify(client, times(55)).invalidateRegionCache(
        region.name(), false, null);
    verify(client, times(119)).sendRpcToRegion((HBaseRpc)any());
    verify(client, times(55)).handleNSRE((HBaseRpc)any(), (byte[])any(),
        (RecoverableException)any(), (String)any());
    Field got_nsreField = client.getClass().getDeclaredField("got_nsre");
    got_nsreField.setAccessible(true);
    ConcurrentSkipListMap got_nsre = (ConcurrentSkipListMap)got_nsreField.get(client);
    assertEquals(0, got_nsre.size());
  }

  @Test
  public void recoverOnProbe() throws Exception {
    final int trigger_retries = 2;

    // probes all fail but the trigger will succeed at one point
    final FakeTimer timer = setupMultiNSRE(trigger_retries, 2, false);

    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(trigger);

    // execute the dummyRpcs now
    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>>[] dummyRpcDeferred = new Deferred[]{
        client.get(dummy_gets[0]),
        client.get(dummy_gets[1]),
        client.get(dummy_gets[2])};

    for (int i = 0;i < 3;i++) {
      // Check the output is same
      assertSame(row, dummyRpcDeferred[i].join());
      // Check the number of times RPC is sent to region client this will be
      // equals to probe_expire_count for each RetryRpc during failure
      //  + 1 after the NSRE is cleared
      verify(regionclient, times(1)).sendRpc(dummy_gets[i]);
    }

    assertSame(row, triggerRpcDeferred.join());
    assertEquals(trigger_retries, timer.tasks.size());

    Long last = 400L;
    for (Map.Entry<TimerTask, Long> task : timer.tasks) {
      assertEquals(last, task.getValue());
    }

    verify(client, times(2)).invalidateRegionCache(
        region.name(), false, null);
    verify(client, times(10)).sendRpcToRegion((HBaseRpc)any());
    verify(client, times(2)).handleNSRE((HBaseRpc)any(), (byte[])any(),
        (RecoverableException)any(), (String)any());
    Field got_nsreField = client.getClass().getDeclaredField("got_nsre");
    got_nsreField.setAccessible(true);
    ConcurrentSkipListMap got_nsre = (ConcurrentSkipListMap)got_nsreField.get(client);
    assertEquals(0, got_nsre.size());
  }

  @Test
  public void tooManyAttempts() throws Exception {
    Field read_rpc_retriesField = client.getClass().getDeclaredField("read_rpc_retries");
    read_rpc_retriesField.setAccessible(true);
    read_rpc_retriesField.set(client, 2);

    // probes all fail but the trigger will succeed at one point
    final FakeTimer timer = setupMultiNSRE(4, 4, true);

    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(trigger);

    // execute the dummyRpcs now
    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<KeyValue>>[] dummyRpcDeferred = new Deferred[]{
        client.get(dummy_gets[0]),
        client.get(dummy_gets[1]),
        client.get(dummy_gets[2])};

    for (int i = 0;i < 3;i++) {
      NonRecoverableException nre = null;
      try {
        dummyRpcDeferred[i].join();
      } catch (NonRecoverableException e) {
        nre = e;
      }
      assertNotNull(nre);
      verify(regionclient, times(3)).sendRpc(dummy_gets[i]);
    }

    NonRecoverableException nre = null;
    try {
      triggerRpcDeferred.join();
    } catch (NonRecoverableException e) {
      nre = e;
    }
    assertNotNull(nre);

    assertEquals(60, timer.tasks.size());

//    Long last = 400L;
//    for (Map.Entry<TimerTask, Long> task : timer.tasks) {
//      assertEquals(last, task.getValue());
//      if (last >= 800) {
//        last = 400L;
//      } else if (last < 1000) {
//        last += 200;
//      }
//    }
    
    verify(client, times(36)).invalidateRegionCache(
        region.name(), false, null);
    verify(client, times(84)).sendRpcToRegion((HBaseRpc)any());
    verify(client, times(36)).handleNSRE((HBaseRpc)any(), (byte[])any(),
        (RecoverableException)any(), (String)any());
    Field got_nsreField = client.getClass().getDeclaredField("got_nsre");
    got_nsreField.setAccessible(true);
    ConcurrentSkipListMap got_nsre = (ConcurrentSkipListMap)got_nsreField.get(client);
    assertEquals(0, got_nsre.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void handleNSRENullRPC() throws Exception {
    final GetRequest get = new GetRequest(TABLE, KEY);
    client.handleNSRE(null, region.name(), 
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);
  }
  
  @Test (expected = NullPointerException.class)
  public void handleNSRENullRegion() throws Exception {
    final GetRequest get = new GetRequest(TABLE, KEY);
    client.handleNSRE(get, null, 
        new NotServingRegionException("Fail", trigger),
        REMOTE_ADDRESS);
  }

  // apparently this is OK so just perform a basic validation
  @Test
  public void handleNSRENullException() throws Exception {
    setupMultiNSRE(1, 1, false);
    final GetRequest get = new GetRequest(TABLE, KEY);
    client.handleNSRE(get, region.name(), null,
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), false, null);
    verify(client, times(3)).sendRpcToRegion((HBaseRpc)any());
    verify(client, times(1)).handleNSRE((HBaseRpc)any(), (byte[])any(),
        (RecoverableException)any(),
        eq(REMOTE_ADDRESS));
    Field got_nsreField = client.getClass().getDeclaredField("got_nsre");
    got_nsreField.setAccessible(true);
    ConcurrentSkipListMap got_nsre = (ConcurrentSkipListMap)got_nsreField.get(client);
    assertEquals(0, got_nsre.size());
  }

  @Test
  public void handleNSRE1stTime() throws Exception {
    final HBaseRpc probe = MockProbe();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(1, num_nsres.get());
    assertEquals(1, num_nsre_rpcs.get());

    final Map.Entry<byte[], ArrayList<HBaseRpc>> entry =
        got_nsre.entrySet().iterator().next();
    assertArrayEquals(region.name(), entry.getKey());
    assertEquals(2, entry.getValue().size());
    assertSame(probe, entry.getValue().get(0));
    assertSame(get, entry.getValue().get(1));
  }

  @Test
  public void handleNSRE2ndTime() throws Exception {
    final HBaseRpc probe = MockProbe();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final GetRequest get2 = new GetRequest(TABLE, KEY);

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);
    client.handleNSRE(get2, region.name(),
        new NotServingRegionException("Fail", get2),
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(1, num_nsres.get());
    assertEquals(2, num_nsre_rpcs.get());

    final Map.Entry<byte[], ArrayList<HBaseRpc>> entry =
        got_nsre.entrySet().iterator().next();
    assertArrayEquals(region.name(), entry.getKey());
    assertEquals(3, entry.getValue().size());
    assertSame(probe, entry.getValue().get(0));
    assertSame(get, entry.getValue().get(1));
    assertSame(get2, entry.getValue().get(2));
  }

  // ?? What's the real purpose here?
  @Test
  public void handleNSRELowWatermark() throws Exception {
    Field nsre_low_watermarkField = client.getClass().getDeclaredField("nsre_low_watermark");
    nsre_low_watermarkField.setAccessible(true);
    nsre_low_watermarkField.set(client, (short)1);
    final HBaseRpc probe = MockProbe();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final GetRequest get2 = new GetRequest(TABLE, KEY);
    final GetRequest get3 = new GetRequest(TABLE, KEY);

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);
    client.handleNSRE(get2, region.name(),
        new NotServingRegionException("Fail", get2),
        REMOTE_ADDRESS);
    client.handleNSRE(get3, region.name(),
        new NotServingRegionException("Fail", get3),
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(1, num_nsres.get());
    assertEquals(3, num_nsre_rpcs.get());

    final Map.Entry<byte[], ArrayList<HBaseRpc>> entry =
        got_nsre.entrySet().iterator().next();
    assertArrayEquals(region.name(), entry.getKey());
    assertEquals(4, entry.getValue().size());
    assertSame(probe, entry.getValue().get(0));
    assertSame(get, entry.getValue().get(1));
    assertSame(get2, entry.getValue().get(2));
    assertSame(get3, entry.getValue().get(3));
  }

  @Test
  public void handleNSREHighWatermark() throws Exception {
    Field nsre_high_watermarkField = client.getClass().getDeclaredField("nsre_high_watermark");
    nsre_high_watermarkField.setAccessible(true);
    nsre_high_watermarkField.set(client, (short)2);
    final HBaseRpc probe = MockProbe();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final GetRequest get2 = new GetRequest(TABLE, KEY);
    final Deferred<Object> get2_deferred = get2.getDeferred();
    final GetRequest get3 = new GetRequest(TABLE, KEY);
    final Deferred<Object> get3_deferred = get3.getDeferred();

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);
    client.handleNSRE(get2, region.name(),
        new NotServingRegionException("Fail", get2),
        REMOTE_ADDRESS);
    client.handleNSRE(get3, region.name(),
        new NotServingRegionException("Fail", get3),
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(1, num_nsres.get());
    assertEquals(3, num_nsre_rpcs.get());

    final Map.Entry<byte[], ArrayList<HBaseRpc>> entry =
        got_nsre.entrySet().iterator().next();
    assertArrayEquals(region.name(), entry.getKey());
    assertEquals(2, entry.getValue().size());
    assertSame(probe, entry.getValue().get(0));
    assertSame(get, entry.getValue().get(1));

    NonRecoverableException ex = null;
    try {
      get2_deferred.join();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex instanceof PleaseThrottleException);
    assertNotNull(ex.getCause());
    assertTrue(ex.getCause() instanceof NotServingRegionException);

    ex = null;
    try {
      get3_deferred.join();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex instanceof PleaseThrottleException);
    assertNotNull(ex.getCause());
    assertTrue(ex.getCause() instanceof NotServingRegionException);
  }

  @Test
  public void handleNSREReProbe() throws Exception {
    Field nsre_high_watermarkField = client.getClass().getDeclaredField("nsre_high_watermark");
    nsre_high_watermarkField.setAccessible(true);
    nsre_high_watermarkField.set(client, (short)10000);
    final HBaseRpc probe = MockProbe();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final GetRequest get2 = new GetRequest(TABLE, KEY);

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);
    client.handleNSRE(get2, region.name(),
        new NotServingRegionException("Fail", get2),
        REMOTE_ADDRESS);
    client.handleNSRE(probe, region.name(),
        new NotServingRegionException("Fail", probe),
        REMOTE_ADDRESS);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(2, num_nsres.get());
    assertEquals(3, num_nsre_rpcs.get());

    final Map.Entry<byte[], ArrayList<HBaseRpc>> entry =
        got_nsre.entrySet().iterator().next();
    assertArrayEquals(region.name(), entry.getKey());
    assertEquals(3, entry.getValue().size());
    assertSame(probe, entry.getValue().get(0));
    assertSame(get, entry.getValue().get(1));
    assertSame(get2, entry.getValue().get(2));

  }

  /**
   * In this case we're making like we have retried the RPCs without actually
   * doing so. The client thinks this is a new NSRE so it will generate a probe
   * to see if the region comes up. Try storing one first
   */
  @Test
  public void handleNSRECannotRetryEmptyNSREMap() throws Exception {
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = get.getDeferred();
    get.attempt = (byte)(client.getConfig()
        .getInt("hbase.client.retries.number") + 2);

    assertEquals(0, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);

    NonRecoverableException ex = null;
    try {
      deferred.join();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex.getMessage().contains("Too many attempts"));
    assertNotNull(ex.getCause());
    assertTrue(ex.getCause() instanceof NotServingRegionException);

    verify(client, times(1)).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(1, num_nsres.get());
  }

  /**
   * TODO Investigate this. It seems to behave strangely. We pass in an RPC with
   * too many attempts and it rejects it with a please throttle because there is
   * a probe RPC on the same region.
   */
  @Test
  public void handleNSRECannotRetryRejectedWPleaseThrottle() throws Exception {
    final HBaseRpc exists = GetRequest.exists(TABLE, HBaseClient.PROBE_SUFFIX);
    final ArrayList<HBaseRpc> nsres = new ArrayList<HBaseRpc>(1);
    nsres.add(exists);
    got_nsre.put(region.name(), nsres);

    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, mock(HashedWheelTimer.class));
    final GetRequest get = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = get.getDeferred();
    get.attempt = (byte)(client.getConfig()
        .getInt("hbase.client.retries.number") + 2);

    assertEquals(1, got_nsre.size());
    assertEquals(0, num_nsres.get());

    client.handleNSRE(get, region.name(),
        new NotServingRegionException("Fail", get),
        REMOTE_ADDRESS);

    NonRecoverableException ex = null;
    try {
      deferred.join();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex instanceof PleaseThrottleException);
    assertNotNull(ex.getCause());
    assertTrue(ex.getCause() instanceof NotServingRegionException);

    verify(client, never()).invalidateRegionCache(
        region.name(), true, "seems to be splitting or closing it.");
    verify(client, never()).sendRpcToRegion((HBaseRpc)any());
    assertEquals(1, got_nsre.size());
    assertEquals(0, num_nsres.get());
  }

  private FakeTimer setupMultiNSRE(final int trigger_retries,
      final int probe_retries, final boolean nsre_dummies) throws Exception {
    final FakeTimer timer = new FakeTimer();
    Field timerField = client.getClass().getDeclaredField("timer");
    timerField.setAccessible(true);
    timerField.set(client, timer);
    Field rootregionField = client.getClass().getDeclaredField("rootregion");
    rootregionField.setAccessible(true);
    rootregionField.set(client, rootclient);

    when(regionclient.isAlive()).thenReturn(true);
    when(rootclient.isAlive()).thenReturn(true);
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(),
        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    when(rootclient.getClosestRowBefore((RegionInfo)any(), anyBytes(), anyBytes(),
        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    doReturn(regionclient).when(client).newClient(anyString(), anyInt());

    short id = 0;
    dummy_gets = new GetRequest[]{
        new GetRequest(TABLE, KEY, Bytes.fromShort(id++)),
        new GetRequest(TABLE, KEY, Bytes.fromShort(id++)),
        new GetRequest(TABLE, KEY, Bytes.fromShort(id++))
    };

    trigger = new GetRequest(TABLE, KEY);

    // TRIGGER
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {

        Object[] args = invocation.getArguments();
        GetRequest triggerGet = (GetRequest)args[0];
        if (triggerGet.attempt <= trigger_retries) {
          client.handleNSRE(triggerGet, triggerGet.getRegion().name(),
              new NotServingRegionException("Trigger NSRE", triggerGet),
              REMOTE_ADDRESS);
        } else if (triggerGet.attempt > trigger_retries) {
          triggerGet.callback(row);
        } else {
          throw new AssertionError("Can Never Happen");
        }
        return null;
      }
    }).when(regionclient).sendRpc(eq(trigger));

    // PROBE
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest)args[0];
        if (exist.attempt < probe_retries) {
          // We stub out the RegionClient, which normally does this.
          client.handleNSRE(exist, exist.getRegion().name(),
              new NotServingRegionException("exist 1", exist),
              REMOTE_ADDRESS);
        } else if (exist.attempt >= probe_retries) {
          // NSRE on the region is cleared here
          exist.callback(null);
        } else {
          // This should never happen
          throw new AssertionError("Never Happens");
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat((ArgumentMatcher<HBaseRpc>)that -> that != dummy_gets[0] && that != trigger
        && that != dummy_gets[1] && that != dummy_gets[2]));

    // DUMMY GETS
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {

        Object[] args = invocation.getArguments();
        final GetRequest dummyGet = (GetRequest)args[0];
        // stubbing out the entire decode method in the region client
        if (nsre_dummies) {
          client.handleNSRE(dummyGet, dummyGet.getRegion().name(),
              new NotServingRegionException("Dummy NSRE", dummyGet),
              REMOTE_ADDRESS);
        } else {
          dummyGet.callback(row);
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat((ArgumentMatcher<HBaseRpc>)that -> (that == dummy_gets[0]
        || that == dummy_gets[1]
        || that == dummy_gets[2])));

    return timer;
  }
  
  /**
   * Generates a mock {@code GetRequest.exists()} request for use in these tests 
   * @return An HBaseRPC to test with
   * @throws Exception If mocking failed.
   */
  private HBaseRpc MockProbe() throws Exception {
    final byte[] probe_key = (byte[])Whitebox.invokeMethod(HBaseClient.class,
        "probeKey", KEY);
    // Build a controlled mock probe so handleNSRE doesn't create a real
    // GetRequest whose Deferred never completes (which would hang the
    // FakeTimer's immediate-retry loop forever).
    final HBaseRpc exists = mock(HBaseRpc.class);
    exists.attempt = 0;
    when(exists.getDeferred()).thenReturn(new Deferred<Object>());
    when(exists.toString()).thenReturn("MockProbe");
    // Stub the static factory so production code obtains our controlled mock.
    // Keep all other GetRequest statics real (e.g. probeKey paths elsewhere).
    mockedGetRequest = Mockito.mockStatic(GetRequest.class,
        Mockito.CALLS_REAL_METHODS);
    mockedGetRequest.when(() -> GetRequest.exists(eq(TABLE), eq(probe_key)))
        .thenReturn(exists);
    return exists;
  }
}
