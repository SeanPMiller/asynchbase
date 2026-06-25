/*
 * Copyright (C) 2018 The Async HBase Authors.  All rights reserved.
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
package org.hbase.async.ratelimiter;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;


import com.google.common.util.concurrent.RateLimiter;
import org.hbase.async.AppendRequest;
import org.hbase.async.BaseTestHBaseClient.FakeTaskTimer;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.NotServingRegionException;
import org.hbase.async.PutRequest;
import org.hbase.async.RegionClient;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.ratelimiter.WriteRateLimiter.SIGNAL;
import org.jboss.netty.channel.Channels;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestWriteRateLimiter {

  private WriteRateLimiter limiter;
  private FakeTaskTimer timer;
  private LimitPolicy rate_policy;
  private LimitPolicy threshold_policy;
  private RegionClient regin_client;
  /** Static mock of RateLimiter held open for the duration of each test. */
  private MockedStatic<RateLimiter> mockedRateLimiter;
  /** Backing state for the deterministic RateLimiter mock returned by create. */
  private double current_rate;
  /**
   * One-shot permit that models Guava's RateLimiter.tryAcquire() behaviour
   * deterministically (no wall-clock dependency): a single permit becomes
   * available immediately after create()/setRate(), is granted by the first
   * tryAcquire(), and is then exhausted until the next setRate().  This is
   * exactly what the isHealthy() assertions in these tests rely on.
   */
  private boolean permit_available;

  @Before
  public void before() throws Exception {
    timer = new FakeTaskTimer();
    rate_policy = new RateLimitPolicyImpl();
    threshold_policy = new ThresholdLimitPolicyImpl();
    regin_client = mock(RegionClient.class);
    Mockito.when(regin_client.toString()).thenReturn("rc1");

    // Stub the static RateLimiter.create(double) so the production code under
    // test receives a deterministic, controllable RateLimiter mock.
    mockedRateLimiter = Mockito.mockStatic(RateLimiter.class);
    final RateLimiter limiter_mock = mock(RateLimiter.class);
    mockedRateLimiter.when(() -> RateLimiter.create(anyDouble()))
        .thenAnswer(new Answer<RateLimiter>() {
      @Override
      public RateLimiter answer(InvocationOnMock invocation) throws Throwable {
        current_rate = (Double) invocation.getArguments()[0];
        permit_available = true;
        return limiter_mock;
      }
    });
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        current_rate = (Double) invocation.getArguments()[0];
        permit_available = true;
        return null;
      }
    }).when(limiter_mock).setRate(anyDouble());
    when(limiter_mock.getRate()).thenAnswer(new Answer<Double>() {
      @Override
      public Double answer(InvocationOnMock invocation) throws Throwable {
        return current_rate;
      }
    });
    when(limiter_mock.tryAcquire()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        final boolean granted = permit_available;
        permit_available = false;
        return granted;
      }
    });
  }

  @After
  public void after() {
    if (mockedRateLimiter != null) {
      mockedRateLimiter.close();
    }
  }

  @Test
  public void ratePolicyRestrictTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    final int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }
  }
  
  @Test
  public void ratePolicyReleaseTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    final int max_iteration = 49;
    Double next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;
    limiter.overrideCurrentRate(WriteRateLimiter.RATE_MIN_THRESHOLD);
    limiter.resetOverridenHealthCheck();

    Double curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

      if (cycle < 2) {
        assertFalse(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  @Test
  public void ratePolicyFullCycleTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }

    max_iteration = 49;
    next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;

    curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  @Test
  public void thresholdPolicyRestrictTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, threshold_policy, timer, 60000);
    final int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }
  }

  @Test
  public void thresholdPolicyReleaseTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, threshold_policy, timer, 60000);
    final int max_iteration = 49;
    Double next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;
    limiter.overrideCurrentRate(WriteRateLimiter.RATE_MIN_THRESHOLD);
    limiter.resetOverridenHealthCheck();

    Double curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

      if (cycle < 2) {
        assertFalse(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  /** Simple mock RateLimiter to capture and modify calls */
  public static class MockRateLimiter {
    public RateLimiter limiter;
    public double current_rate = 0;
    public int acquire_attempts = 0;
    public boolean allow = true;
    /** Set when this helper opened (and therefore owns/closes) the static mock. */
    private final MockedStatic<RateLimiter> owned_static;

    /**
     * Opens its own {@code MockedStatic<RateLimiter>} and is responsible for
     * closing it via {@link #close()}.  Used by callers that don't manage the
     * static mock themselves.
     */
    public MockRateLimiter() {
      this(Mockito.mockStatic(RateLimiter.class), true);
    }

    /**
     * Wires {@code RateLimiter.create(double)} stubbing onto a static mock
     * owned by the caller (which is responsible for closing it).
     */
    public MockRateLimiter(final MockedStatic<RateLimiter> mockedRateLimiter) {
      this(mockedRateLimiter, false);
    }

    private MockRateLimiter(final MockedStatic<RateLimiter> mockedRateLimiter,
                            final boolean owns) {
      this.owned_static = owns ? mockedRateLimiter : null;
      limiter = mock(RateLimiter.class);

      // since the limiter may be nulled and created anew, make sure to reset
      // counters and flags.
      mockedRateLimiter.when(() -> RateLimiter.create(anyDouble()))
          .thenAnswer(new Answer<RateLimiter>() {
        @Override
        public RateLimiter answer(InvocationOnMock invocation) throws Throwable {
          current_rate = (Double)invocation.getArguments()[0];
          acquire_attempts = 0;
          allow = true;
          return limiter;
        }
      });

      when(limiter.tryAcquire()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          ++acquire_attempts;
          return allow;
        }
      });
      
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          current_rate = (Double)invocation.getArguments()[0];
          return null;
        }
      }).when(limiter).setRate(anyDouble());
      
      when(limiter.getRate()).thenAnswer(new Answer<Double>() {
        @Override
        public Double answer(InvocationOnMock invocation) throws Throwable {
          return current_rate;
        }
      });
    }

    /** Closes the static mock if this helper owns it. */
    public void close() {
      if (owned_static != null) {
        owned_static.close();
      }
    }
  }
}