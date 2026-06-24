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
package org.hbase.async.auth;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;


import org.apache.zookeeper.Shell;
import org.hbase.async.Config;
import org.hbase.async.auth.Login.TicketRenewalTask;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.TimerTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

public class TestLogin {
  private MockedStatic<Shell> mockedShell;
  private MockedStatic<System> mockedSystem;
  private MockedStatic<LoginContext> mockedLoginContext;
  private MockedStatic<Configuration> mockedConfiguration;
  private final static String CONTEXT_NAME = "Uberwald"; 
  
  private HashedWheelTimer timer;
  private Config config;
  private CallbackHandler callback;
  private AppConfigurationEntry[] app_config;
  private AppConfigurationEntry app_config_entry;
  private Map<String, Object> app_config_options;
  private LoginContext login_context;
  private Subject subject;
  private KerberosTicket ticket;
  private Set<KerberosTicket> tickets;
  private KerberosPrincipal server;
  private Date start_time;
  private Date end_time;

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    try (MockedConstruction<LoginContext> mockLoginContext = Mockito.mockConstruction(LoginContext.class)) {
      mockedShell = Mockito.mockStatic(Shell.class);
      mockedSystem = Mockito.mockStatic(System.class);
      mockedLoginContext = Mockito.mockStatic(LoginContext.class);
      mockedConfiguration = Mockito.mockStatic(Configuration.class);
      Field current_loginField = Login.class.getClass().getDeclaredField("current_login");
      current_loginField.setAccessible(true);
      current_loginField.set(Login.class, (Login)null);

      start_time = new Date(1388534400000L);
      end_time = new Date(1388538000000L);

      config = new Config();
      timer = mock(HashedWheelTimer.class);
      callback = mock(CallbackHandler.class);
      app_config_entry = mock(AppConfigurationEntry.class);
      app_config = new AppConfigurationEntry[]{app_config_entry};
      app_config_options = new HashMap<String, Object>(2);
      app_config_options.put("useTicketCache", "true");
      app_config_options.put("principal", "Vetinari");
      login_context = mock(LoginContext.class);
      subject = mock(Subject.class);
      ticket = Mockito.mock(KerberosTicket.class);
      server = mock(KerberosPrincipal.class);

      final Configuration app_conf = mock(Configuration.class);
      when(app_conf.getAppConfigurationEntry(anyString())).thenReturn(app_config);
      mockedConfiguration.when(Configuration::getConfiguration).thenReturn(app_conf);
      when(login_context.getSubject()).thenReturn(subject);

      tickets = new HashSet<KerberosTicket>();
      tickets.add(ticket);
      when(subject.getPrivateCredentials(any(Class.class))).thenReturn(tickets);

      when(ticket.getServer()).thenReturn(server);
      when(ticket.getStartTime()).thenReturn(start_time);
      when(ticket.getEndTime()).thenReturn(end_time);

      when(server.getName()).thenReturn("krbtgt/Lancre@Lancre");
      when(server.getRealm()).thenReturn("Lancre");
      mockedSystem.when(System::currentTimeMillis).thenReturn(1388534460000L);
    }
  }

  @After
  public void tearDownStaticMocks() {
    mockedConfiguration.closeOnDemand();
    mockedLoginContext.closeOnDemand();
    mockedSystem.closeOnDemand();
    mockedShell.closeOnDemand();
  }
  
  @Test
  public void initUserIfNeeded() throws Exception {
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    // no-ops
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, times(1)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void ctorWithKerberos() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    assertEquals(subject, login.getSubject());
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, times(1)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void ctorNotKerberos() throws Exception {
    when(subject.getPrivateCredentials(KerberosTicket.class))
      .thenReturn(Collections.<KerberosTicket>emptySet());
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    assertEquals(subject, login.getSubject());
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, never()).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test (expected = LoginException.class)
  public void ctorNullContextName() throws Exception {
    new Login(config, timer, null, callback);
  }
  
  @Test (expected = LoginException.class)
  public void ctorEmptyContextName() throws Exception {
    new Login(config, timer, "", callback);
  }
  
  @Test (expected = LoginException.class)
  public void ctorFailed() throws Exception {
    doThrow(new LoginException("Boo!")).when(login_context).login();
    new Login(config, timer, CONTEXT_NAME, callback);
  }
  
  @Test
  public void getRefreshDelay() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    // should be within 80% of the end time
    assertTrue(delay < end_time.getTime());
    assertTrue(delay >= (end_time.getTime() - start_time.getTime()) * 0.80);
  }
  
  @Test
  public void getRefreshDelayNoTicket() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", 
        (KerberosTicket)null);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }

  @Test
  public void getRefreshDelayCantRenew() throws Exception {
    when(ticket.getRenewTill()).thenReturn(end_time);
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Field using_ticket_cacheField = login.getClass().getDeclaredField("using_ticket_cache");
    using_ticket_cacheField.setAccessible(true);
    using_ticket_cacheField.set(login, true);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshPastExpiration() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    mockedSystem.when(System::currentTimeMillis).thenReturn(1388538060000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshWithinMinTime() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    mockedSystem.when(System::currentTimeMillis).thenReturn(1388537942000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(0, delay);
  }
  
  @Test
  public void getRefreshSuperShortExpiration() throws Exception {
    // I guess this prevents possible dos attacks if someone set the lifetime to
    // be less than a minute
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    start_time.setTime(1388537942000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshFlippedTicketTimes() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    // Friends don't let friend's KDC issue funky tickets like this
    end_time.setTime(1388534400000L);
    start_time.setTime(1388538000000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshSameTicketTimes() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    end_time.setTime(1388534400000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }

  @Test
  public void getTGT() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method getTGTMethod = login.getClass().getDeclaredMethod("getTGT");
    getTGTMethod.setAccessible(true);
    KerberosTicket tgt = (KerberosTicket)getTGTMethod.invoke(login);
    assertEquals(tgt, ticket);
  }

  @Test
  public void getTGTNoMatch() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    when(server.getName()).thenReturn("quirm");
    Method getTGTMethod = login.getClass().getDeclaredMethod("getTGT");
    getTGTMethod.setAccessible(true);
    KerberosTicket tgt = (KerberosTicket)getTGTMethod.invoke(login);
    assertNull(tgt);
  }

  @Test
  public void getTGTNoTickets() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    when(subject.getPrivateCredentials(KerberosTicket.class))
        .thenReturn(Collections.<KerberosTicket>emptySet());
    Method getTGTMethod = login.getClass().getDeclaredMethod("getTGT");
    getTGTMethod.setAccessible(true);
    KerberosTicket tgt = (KerberosTicket)getTGTMethod.invoke(login);
    assertNull(tgt);
  }

  @Test
  public void refreshTicketCache() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method refreshTicketCacheMethod = login.getClass().getDeclaredMethod("refreshTicketCache");
    refreshTicketCacheMethod.setAccessible(true);
    refreshTicketCacheMethod.invoke(login);
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }

  @Test
  public void refreshTicketCacheConfigPath() throws Exception {
    config.overrideConfig("asynchbase.security.auth.kinit",
        "/usr/local/bin/kinit");
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method refreshTicketCacheMethod = login.getClass().getDeclaredMethod("refreshTicketCache");
    refreshTicketCacheMethod.setAccessible(true);
    refreshTicketCacheMethod.invoke(login);
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/local/bin/kinit", "-R");
  }

  @Test(expected = RuntimeException.class)
  public void refreshTicketCacheIOException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    mockedShell.when(() -> Shell.execCommand(anyString(), anyString()))
        .thenThrow(new IOException("Boo!"));
    Method refreshTicketCacheMethod = login.getClass().getDeclaredMethod("refreshTicketCache");
    refreshTicketCacheMethod.setAccessible(true);
    refreshTicketCacheMethod.invoke(login);
  }

  @Test(expected = RuntimeException.class)
  public void refreshTicketCacheException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    mockedShell.when(() -> Shell.execCommand(anyString(), anyString()))
        .thenThrow(new Exception("Boo!"));
    Method refreshTicketCacheMethod = login.getClass().getDeclaredMethod("refreshTicketCache");
    refreshTicketCacheMethod.setAccessible(true);
    refreshTicketCacheMethod.invoke(login);
  }

  @Test
  public void refreshTicketCacheEmptyCommand() throws Exception {
    config.overrideConfig("asynchbase.security.auth.kinit", "");
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method refreshTicketCacheMethod = login.getClass().getDeclaredMethod("refreshTicketCache");
    refreshTicketCacheMethod.setAccessible(true);
    refreshTicketCacheMethod.invoke(login);
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }

  @Test
  public void reLogin() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method reLoginMethod = login.getClass().getDeclaredMethod("reLogin");
    reLoginMethod.setAccessible(true);
    reLoginMethod.invoke(login);
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
  }

  @Test
  public void reLoginNotKerberos() throws Exception {
    when(subject.getPrivateCredentials(KerberosTicket.class))
        .thenReturn(Collections.<KerberosTicket>emptySet());
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Method reLoginMethod = login.getClass().getDeclaredMethod("reLogin");
    reLoginMethod.setAccessible(true);
    reLoginMethod.invoke(login);
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
  }

  @Test(expected = LoginException.class)
  public void reLoginNotLoggedInYet() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Field login_contextField = login.getClass().getDeclaredField("login_context");
    login_contextField.setAccessible(true);
    login_contextField.set(login, (LoginContext)null);
    Method reLoginMethod = login.getClass().getDeclaredMethod("reLogin");
    reLoginMethod.setAccessible(true);
    reLoginMethod.invoke(login);
  }

  @Test(expected = LoginException.class)
  public void reLoginLoginFailed() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    doThrow(new LoginException("Boo!")).when(login_context).login();
    Method reLoginMethod = login.getClass().getDeclaredMethod("reLogin");
    reLoginMethod.setAccessible(true);
    reLoginMethod.invoke(login);
  }

  @Test
  public void ticketRenewalTask() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }

  @Test
  public void ticketRenewalTaskRefreshCache() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Field using_ticket_cacheField = login.getClass().getDeclaredField("using_ticket_cache");
    using_ticket_cacheField.setAccessible(true);
    using_ticket_cacheField.set(login, true);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(),
        eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }

  @Test
  public void ticketRenewalTaskLoginException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Field login_contextField = login.getClass().getDeclaredField("login_context");
    login_contextField.setAccessible(true);
    login_contextField.set(login, (LoginContext)null);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(),
        eq(TimeUnit.MILLISECONDS));
    // catch the default refresh rate
    verify(timer, times(1)).newTimeout((TimerTask)any(),
        eq(Login.MIN_TIME_BEFORE_RELOGIN), eq(TimeUnit.MILLISECONDS));
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
  
  @Test
  public void ticketRenewalTaskException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    doThrow(new RuntimeException("Boo!")).when(login_context).login();
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    // catch the default refresh rate
    verify(timer, times(1)).newTimeout((TimerTask)any(), 
        eq(Login.MIN_TIME_BEFORE_RELOGIN), eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
}
