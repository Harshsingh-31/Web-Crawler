package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final ProfilingState state;
  private final Object delegate;

  ProfilingMethodInterceptor(Clock clock, ProfilingState state, Object delegate) {
    this.clock = Objects.requireNonNull(clock);
    this.state = Objects.requireNonNull(state);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    if (method.equals(Object.class.getMethod("equals", Object.class))) {
      Object other = args[0];
      if (other == null) return false;
      if (Proxy.isProxyClass(other.getClass())) {
        InvocationHandler handler = Proxy.getInvocationHandler(other);
        if (handler instanceof ProfilingMethodInterceptor) {
          other = ((ProfilingMethodInterceptor) handler).delegate;
        }
      }
      return delegate.equals(other);
    }

    if (method.equals(Object.class.getMethod("hashCode"))) {
      return delegate.hashCode();
    }

    if (method.equals(Object.class.getMethod("toString"))) {
      return delegate.toString();
    }

    boolean profiled = method.isAnnotationPresent(Profiled.class);
    Instant start = null;
    if (profiled) {
      start = clock.instant();
    }

    try {
      return method.invoke(delegate, args == null ? new Object[0] : args);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Access not allowed to method: " + method.getName(), e);
    } catch (InvocationTargetException e) {
      throw e.getTargetException(); // rethrow actual cause
    } finally {
      if (profiled && start != null) {
        Instant end = clock.instant();
        state.record(delegate.getClass(), method, Duration.between(start, end));
      }
    }
  }
}
