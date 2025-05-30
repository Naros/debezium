/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities related to threads and threading.
 *
 * @author Randall Hauch
 */
public class Threads {

    private static final String DEBEZIUM_THREAD_NAME_PREFIX = "debezium-";
    private static final Logger LOGGER = LoggerFactory.getLogger(Threads.class);

    /**
     * Measures the amount time that has elapsed since the last {@link #reset() reset}.
     */
    public interface TimeSince {
        /**
         * Reset the elapsed time to 0.
         */
        void reset();

        /**
         * Get the time that has elapsed since the last call to {@link #reset() reset}.
         *
         * @return the number of milliseconds
         */
        long elapsedTime();
    }

    /**
     * Expires after defined time period.
     *
     */
    public interface Timer {

        /**
         * @return true if current time is greater than start time plus requested time period
         */
        boolean expired();

        Duration remaining();
    }

    /**
     * Obtain a {@link TimeSince} that uses the given clock to record the time elapsed.
     *
     * @param clock the clock; may not be null
     * @return the {@link TimeSince} object; never null
     */
    public static TimeSince timeSince(Clock clock) {
        return new TimeSince() {
            private long lastTimeInMillis;

            @Override
            public void reset() {
                lastTimeInMillis = clock.currentTimeInMillis();
            }

            @Override
            public long elapsedTime() {
                long elapsed = clock.currentTimeInMillis() - lastTimeInMillis;
                return Math.max(elapsed, 0L);
            }
        };
    }

    /**
     * Obtain a {@link Timer} that uses the given clock to indicate that a pre-defined time period expired.
     *
     * @param clock the clock; may not be null
     * @param time a time interval to expire
     * @return the {@link Timer} object; never null
     */
    public static Timer timer(Clock clock, Duration time) {
        final TimeSince start = timeSince(clock);
        start.reset();

        return new Timer() {

            @Override
            public boolean expired() {
                return start.elapsedTime() > time.toMillis();
            }

            @Override
            public Duration remaining() {
                return time.minus(start.elapsedTime(), ChronoUnit.MILLIS);
            }
        };
    }

    /**
     * Create a thread that will interrupt the calling thread when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     *
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread interruptAfterTimeout(String threadName,
                                               long timeout, TimeUnit timeoutUnit,
                                               TimeSince elapsedTimer) {
        Thread threadToInterrupt = Thread.currentThread();
        return interruptAfterTimeout(threadName, timeout, timeoutUnit, elapsedTimer, threadToInterrupt);
    }

    /**
     * Create a thread that will interrupt the given thread when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     *
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param threadToInterrupt the thread that should be interrupted upon timeout; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread interruptAfterTimeout(String threadName,
                                               long timeout, TimeUnit timeoutUnit,
                                               TimeSince elapsedTimer, Thread threadToInterrupt) {
        return timeout(threadName, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS,
                elapsedTimer::elapsedTime, elapsedTimer::reset,
                () -> threadToInterrupt.interrupt());
    }

    /**
     * Create a thread that will call the supplied function when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * <p>
     * The thread checks the elapsed time every 100 milliseconds.
     *
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 TimeSince elapsedTimer, Runnable uponTimeout) {
        return timeout(threadName, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS,
                elapsedTimer::elapsedTime, elapsedTimer::reset,
                uponTimeout);
    }

    /**
     * Create a thread that will call the supplied function when the {@link TimeSince elapsed time} has exceeded the
     * specified amount. The supplied {@link TimeSince} object will be {@link TimeSince#reset() reset} when the
     * new thread is started, and should also be {@link TimeSince#reset() reset} any time the elapsed time should be reset to 0.
     * <p>
     * The thread checks the elapsed time every 100 milliseconds.
     *
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param sleepInterval the amount of time for the new thread to sleep after checking the elapsed time; must be positive
     * @param sleepUnit the unit for {@code sleepInterval}; may not be null
     * @param elapsedTimer the component used to measure the elapsed time; may not be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 long sleepInterval, TimeUnit sleepUnit,
                                 TimeSince elapsedTimer, Runnable uponTimeout) {
        return timeout(threadName, timeout, timeoutUnit, sleepInterval, sleepUnit,
                elapsedTimer::elapsedTime, elapsedTimer::reset,
                uponTimeout);
    }

    /**
     * Create a thread that will call the supplied function when the elapsed time has exceeded the
     * specified amount.
     *
     * @param threadName the name of the new thread; may not be null
     * @param timeout the maximum amount of time that can elapse before the thread is interrupted; must be positive
     * @param timeoutUnit the unit for {@code timeout}; may not be null
     * @param sleepInterval the amount of time for the new thread to sleep after checking the elapsed time; must be positive
     * @param sleepUnit the unit for {@code sleepInterval}; may not be null
     * @param elapsedTime the function that returns the total elapsed time; may not be null
     * @param uponStart the function that will be called when the returned thread is {@link Thread#start() started}; may be null
     * @param uponTimeout the function to be called when the maximum amount of time has elapsed; may not be null
     * @return the new thread that has not yet been {@link Thread#start() started}; never null
     */
    public static Thread timeout(String threadName,
                                 long timeout, TimeUnit timeoutUnit,
                                 long sleepInterval, TimeUnit sleepUnit,
                                 LongSupplier elapsedTime,
                                 Runnable uponStart, Runnable uponTimeout) {
        final long timeoutInMillis = timeoutUnit.toMillis(timeout);
        final long sleepTimeInMillis = sleepUnit.toMillis(sleepInterval);
        Runnable r = () -> {
            if (uponStart != null) {
                uponStart.run();
            }
            while (elapsedTime.getAsLong() < timeoutInMillis) {
                try {
                    Thread.sleep(sleepTimeInMillis);
                }
                catch (InterruptedException e) {
                    // awoke from sleep
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            // Otherwise we've timed out ...
            uponTimeout.run();
        };
        return new Thread(r, DEBEZIUM_THREAD_NAME_PREFIX + "-timeout-" + threadName);
    }

    private Threads() {
    }

    /**
     * Returns a thread factory that creates threads conforming to Debezium thread naming
     * pattern {@code debezium-<component class>-<component-id>-<thread-name>}.
     *
     * @param component - the source connector or sink change consumer class
     * @param componentId - the identifier to differentiate between component instances
     * @param name - the name of the thread
     * @param indexed - true if the thread name should be appended with an index
     * @param daemon - true if the thread should be a daemon thread
     * @return the thread factory setting the correct name
     */
    public static ThreadFactory threadFactory(Class<?> component, String componentId, String name, boolean indexed, boolean daemon) {
        return threadFactory(component, componentId, name, indexed, daemon, null);
    }

    /**
     * Returns a thread factory that creates threads conforming to Debezium thread naming
     * pattern {@code debezium-<component class>-<component-id>-<thread-name>}.
     *
     * @param component - the source or sink component class
     * @param componentId - the identifier to differentiate between componentId instances
     * @param name - the name of the thread
     * @param indexed - true if the thread name should be appended with an index
     * @param daemon - true if the thread should be a daemon thread
     * @param callback - a callback called on every thread created
     * @return the thread factory setting the correct name
     */
    public static ThreadFactory threadFactory(Class<?> component, String componentId, String name, boolean indexed, boolean daemon,
                                              Consumer<Thread> callback) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Requested thread factory for component {}, id = {} named = {}", component.getSimpleName(), componentId, name);
        }

        return new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                StringBuilder threadName = new StringBuilder(DEBEZIUM_THREAD_NAME_PREFIX)
                        .append(component.getSimpleName().toLowerCase())
                        .append('-')
                        .append(componentId)
                        .append('-')
                        .append(name);
                if (indexed) {
                    threadName.append('-').append(index.getAndIncrement());
                }
                LOGGER.info("Creating thread {}", threadName);
                final Thread t = new Thread(r, threadName.toString());
                t.setDaemon(daemon);
                if (callback != null) {
                    callback.accept(t);
                }
                return t;
            }
        };
    }

    public static ExecutorService newSingleThreadExecutor(Class<?> component, String componentId, String name, boolean daemon) {
        return Executors.newSingleThreadExecutor(threadFactory(component, componentId, name, false, daemon));
    }

    public static ExecutorService newFixedThreadPool(Class<?> component, String componentId, String name, int threadCount) {
        return Executors.newFixedThreadPool(threadCount, threadFactory(component, componentId, name, true, false));
    }

    public static ExecutorService newSingleThreadExecutor(Class<?> component, String componentId, String name) {
        return newSingleThreadExecutor(component, componentId, name, false);
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(Class<?> component, String componentId, String name, boolean daemon) {
        return Executors.newSingleThreadScheduledExecutor(threadFactory(component, componentId, name, false, daemon));
    }

    /**
     * Runs an operation with a timeout using a single-threaded executor.
     *
     * @param componentClass the class of the component using this method
     * @param operation the operation to run
     * @param timeout the timeout duration
     * @param componentName the name of the component
     * @param operationName the name of the operation being executed with timeout
     * @throws Exception if the operation fails or times out
     */
    public static void runWithTimeout(Class<?> componentClass, Runnable operation, Duration timeout, String componentName, String operationName) throws Exception {
        ExecutorService executor = newSingleThreadExecutor(componentClass, componentName, operationName);
        Future<?> future = executor.submit(operation);
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            LOGGER.error("Operation {} timed out after {} ms", operationName, timeout.toMillis());
            future.cancel(true);
            throw e;
        }
        catch (ExecutionException e) {
            LOGGER.error("Operation {} failed", operationName, e);
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
        finally {
            executor.shutdownNow();
        }
    }
}
