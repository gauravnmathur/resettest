/*-
 * ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 * Autonomic Proprietary 1.0
 * ——————————————————————————————————————————————————————————————————————————————
 * Copyright (C) 2018 Autonomic, LLC - All rights reserved
 * ——————————————————————————————————————————————————————————————————————————————
 * Proprietary and confidential.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Autonomic, LLC and its suppliers, if any.  The intellectual and technical
 * concepts contained herein are proprietary to Autonomic, LLC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and are
 * protected by trade secret or copyright law. Dissemination of this information
 * or reproduction of this material is strictly forbidden unless prior written
 * permission is obtained from Autonomic, LLC.
 * 
 * Unauthorized copy of this file, via any medium is strictly prohibited.
 * ______________________________________________________________________________
 */
package com.autonomic.iam.client.grpc;

import static com.autonomic.iam.client.grpc.Helpers.hasStatus;
import static com.autonomic.iam.client.grpc.Helpers.isChannelDead;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container for state around a retriable call.
 * @param <T> Stub type which is passed to the function
 * @param <R> Return type of the function which is being retried
 */
public class RetrySession<T extends AbstractStub<T>, R> {
    private static final Logger logger = LoggerFactory.getLogger(RetrySession.class);

    interface Time {
        default void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Stats {
        public final boolean exceededMaxAttempts;
        public final int totalAttempts;
        public final int deadConnectionRetries;
        public final int otherRetries;

        public Stats(boolean exceededMaxAttempts,
                     int totalAttempts,
                     int deadConnectionRetries,
                     int otherRetries) {
            this.exceededMaxAttempts = exceededMaxAttempts;
            this.totalAttempts = totalAttempts;
            this.deadConnectionRetries = deadConnectionRetries;
            this.otherRetries = otherRetries;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("exceededMaxAttempts", exceededMaxAttempts)
                    .add("totalAttempts", totalAttempts)
                    .add("deadConnectionRetries", deadConnectionRetries)
                    .add("otherRetries", otherRetries)
                    .toString();
        }
    }


    @VisibleForTesting
    static Time time = new Time() {};

    private static final StatusRuntimeException DEFAULT_EXCEPTION =
            Code.UNKNOWN.toStatus().withDescription("default exception").asRuntimeException();

    // TODO: these should be configurable in the future
    private static final int DEFAULT_WAIT_TIME_MS = 50;
    private static final int DEFAULT_MAX_WAIT_TIME_MS = 5000;
    private static final int DEFAULT_INCREASE_FACTOR = 2;
    private static final double DEFAULT_JITTER_RATIO = 0.25; // % of wait time which is subject to random perturbation
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private static final int DEFAULT_RPC_DEADLINE_MS = 5000;

    private static final Random random = new Random();

    private final int maxRetries;
    private final int rpcDeadlineMs;

    private int currentAttemptNumber = 0;
    private int deadConnectionsRetries = 0;
    private int otherRetries = 0;

    private StubManager<T> stubManager;
    private Function<T,R> callable;

    public RetrySession(StubManager<T> stubManager,
                        Function<T, R> callable) {
        this(stubManager, callable, DEFAULT_RPC_DEADLINE_MS, DEFAULT_MAX_ATTEMPTS);
    }

    public RetrySession(StubManager<T> stubManager,
                        Function<T, R> callable,
                        int rpcDeadlineMillis,
                        int maxRetries) {
        this.stubManager = stubManager;
        this.callable = callable;

        this.rpcDeadlineMs = rpcDeadlineMillis;
        this.maxRetries = maxRetries;
    }

    public R get() {
        if (currentAttemptNumber > 0) {
            // sessions are not reusable.
            throw new IllegalStateException("retry session has already been consumed");
        }

        StatusRuntimeException exception = DEFAULT_EXCEPTION;
        while (currentAttemptNumber < maxRetries) {
            currentAttemptNumber += 1;

            T stub = stubManager.getStub();
            try {
                return callable.apply(stub.withDeadlineAfter(rpcDeadlineMs, TimeUnit.MILLISECONDS));
            } catch (StatusRuntimeException e) {
                exception = e;
            }

            if (isDeadChannel(exception)) {
                logger.trace("session {} discovered dead channel from exception:", this, exception);

                stubManager.handleTermination(stub);
                deadConnectionsRetries += 1;
                waitForNextRetry();

            } else if (isRetriable(exception)) {
                logger.trace("session {} discovered retry-able exception:", this, exception);

                otherRetries += 1;
                waitForNextRetry();

            } else {
                logger.trace("session {} throwing non-retry-able exception:", this, exception);

                throw exception;
            }
        }
        logger.trace("session {} throwing exception after retry limit reached", this, exception);

        throw exception;
    }

    public Stats getStats() {
        return new Stats(currentAttemptNumber >= maxRetries,
                         currentAttemptNumber,
                         deadConnectionsRetries,
                         otherRetries);
    }

    void waitForNextRetry() {
        long waitTimeMs = getWaitTime(DEFAULT_WAIT_TIME_MS,
                                      DEFAULT_INCREASE_FACTOR,
                                      DEFAULT_MAX_WAIT_TIME_MS,
                                      currentAttemptNumber,
                                      DEFAULT_JITTER_RATIO);
        time.sleep(waitTimeMs);
    }

    static boolean isRetriable(StatusRuntimeException sre) {
        return hasStatus(sre, Code.DEADLINE_EXCEEDED) ||
                (hasStatus(sre, Code.UNAVAILABLE) && !(sre.getCause() instanceof IOException));
    }

    static boolean isDeadChannel(StatusRuntimeException sre) {
        return isChannelDead(sre);
    }

    /**
     * @param waitTimeBaseIntervalMs
     * @param waitTimeIncreaseFactor
     * @param currentAttempt 1 is the first attempt
     * @param jitter
     * @return
     */
    static long getWaitTime(int waitTimeBaseIntervalMs,
                            int waitTimeIncreaseFactor,
                            int maxWaitTimeMs,
                            int currentAttempt,
                            double jitter) {
        checkArgument(currentAttempt > 0);
        checkArgument(waitTimeIncreaseFactor > 0);
        checkArgument(waitTimeBaseIntervalMs > 0);
        checkArgument(jitter >= 0 && jitter < 1.0);

        int baseWaitTime = (int) (Math.pow(waitTimeIncreaseFactor, currentAttempt - 1) * waitTimeBaseIntervalMs);
        int cappedWaitTime = Math.min(baseWaitTime, maxWaitTimeMs);
        if (jitter > 0.0) {
            int jitterWindow = (int) (jitter * cappedWaitTime);
            return cappedWaitTime - (jitterWindow - random.nextInt(jitterWindow * 2));
        } else {
            return cappedWaitTime;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", hashCode())
                .add("stats", getStats().toString())
                .toString();
    }
}
