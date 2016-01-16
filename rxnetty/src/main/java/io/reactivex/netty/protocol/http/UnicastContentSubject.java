/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivex.netty.protocol.http;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.internal.operators.BufferUntilSubscriber;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;
import rx.subjects.Subject;
import rx.subscriptions.Subscriptions;

/**
 * A {@link Subject} implementation to be used by {@link HttpClientResponse} and {@link HttpServerRequest}.
 *
 * <h2>Unicast</h2>
 * This implementation allows a single subscription as this buffers the content till subscription to solve the issue
 * described in <a href="https://github.com/Netflix/RxNetty/issues/206">this github issue</a>.
 * If we allow multiple subscription and still maintain it as a cold observable i.e. which re-runs the stream on every
 * subscription, we loose the control on the scope of this {@link Observable} as any code can hold the reference to
 * this {@link Observable} at any point in time and hence subscribe to it at any time. This will eventually increase
 * memory consumption as we will hold on to the content buffers for more time than required.
 *
 * <h2>Multicast option</h2>
 * If at all it is required to allow multiple subscriptions to this {@link Subject} one should use a
 * {@link Observable#publish()} operator.
 *
 * <h2>Buffering</h2>
 * This subject will buffer all content till the first (one and only) subscriber arrives.
 * In cases, when there are no subscriptions, this buffer may be held till infinity and hence can cause a memory leak in
 * case of netty's {@link ByteBuf} which needs to be released explicitly.
 * In order to avoid this leak, this subject provides a "no subscription timeout" which disposes this subject (calling
 * {@link #disposeIfNotSubscribed()} if it does not get a subscription in the configured timeout duration.
 *
 * The buffer is only utilized if there are any items emitted to this subject before a subscription arrives. After a
 * subscription arrives, this subject becomes a pass through i.e. it does not buffer before sending the notifications.
 *
 * @author Nitesh Kant
 */
public final class UnicastContentSubject<T> extends Subject<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(UnicastContentSubject.class);

    private final State<T> state;
    private volatile boolean noTimeout;
    private volatile Observable<Long> timeoutScheduler;

    private UnicastContentSubject(State<T> state) {
        super(new OnSubscribeAction<T>(state));
        this.state = state;
        timeoutScheduler = Observable.empty(); // No timeout.
        noTimeout = true;
        LOG.info("Created with no timeout, state={}", state, new Exception("trace"));
    }

    private UnicastContentSubject(final State<T> state, long noSubscriptionTimeout, TimeUnit timeUnit,
                                  Scheduler scheduler) {
        super(new OnSubscribeAction<T>(state));
        this.state = state;
        timeoutScheduler = Observable.interval(noSubscriptionTimeout, timeUnit, scheduler).take(1); // Started when content arrives.
        noTimeout = false;
        LOG.info("Created with timeout={} unit={} state={}", noSubscriptionTimeout, timeUnit, state);
    }

    /**
     * Creates a new {@link UnicastContentSubject} without a no subscription timeout.
     * <b>This can cause a memory leak in case no one ever subscribes to this subject.</b> See
     * {@link UnicastContentSubject} for details.
     *
     * @param onUnsubscribe An action to be invoked when the sole subscriber to this {@link Subject} unsubscribes.
     * @param <T> The type emitted and received by this subject.
     *
     * @return The new instance of {@link UnicastContentSubject}
     */
    public static <T> UnicastContentSubject<T> createWithoutNoSubscriptionTimeout(Action0 onUnsubscribe) {
        State<T> state = new State<T>(onUnsubscribe);
        return new UnicastContentSubject<T>(state);
    }

    /**
     * Creates a new {@link UnicastContentSubject} without a no subscription timeout.
     * <b>This can cause a memory leak in case no one ever subscribes to this subject.</b> See
     * {@link UnicastContentSubject} for details.
     *
     * @param <T> The type emitted and received by this subject.
     *
     * @return The new instance of {@link UnicastContentSubject}
     */
    public static <T> UnicastContentSubject<T> createWithoutNoSubscriptionTimeout() {
        return createWithoutNoSubscriptionTimeout(null);
    }

    public static <T> UnicastContentSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit) {
        return create(noSubscriptionTimeout, timeUnit, (Action0)null);
    }

    public static <T> UnicastContentSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
                                                      Action0 onUnsubscribe) {
        return create(noSubscriptionTimeout, timeUnit, Schedulers.computation(), onUnsubscribe);
    }

    public static <T> UnicastContentSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
                                                      Scheduler timeoutScheduler) {
        return create(noSubscriptionTimeout, timeUnit, timeoutScheduler, null);
    }

    public static <T> UnicastContentSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
                                                      Scheduler timeoutScheduler, Action0 onUnsubscribe) {
        State<T> state = new State<T>(onUnsubscribe);
        return new UnicastContentSubject<T>(state, noSubscriptionTimeout, timeUnit, timeoutScheduler);
    }

    /**
     * This will eagerly dispose this {@link Subject} without waiting for the no subscription timeout period,
     * if configured.
     *
     * This must be invoked when the caller is sure that no one will subscribe to this subject. Any subscriber after
     * this call will receive an error that the subject is disposed.
     *
     * @return {@code true} if the subject was disposed by this call (if and only if there was no subscription).
     */
    public boolean disposeIfNotSubscribed() {
        LOG.info("disposeIfNotSubscribed called state={} state.state={}", state, state.state);
        if (state.casState(State.STATES.UNSUBSCRIBED, State.STATES.DISPOSED)) {
            state.bufferedObservable.subscribe(Subscribers.create(new Action1<Object>() {
                @Override
                public void call(Object b) {
                    if (b instanceof ByteBuf) {
                        ByteBuf bb = (ByteBuf) b;
                        String mem = "";
                        if (bb.hasMemoryAddress()) {
                            mem = Long.toString(bb.memoryAddress());
                        }
                        LOG.info("disposeIfNotSubscribed disposing of buf={}:{}", b, mem);
                        bb.touch("disposeIfNotSubscribed state.state = " + state.state);
                    }
                }
            }));
            return true;
        }
        return false;
    }

    public void updateTimeoutIfNotScheduled(long noSubscriptionTimeout, TimeUnit timeUnit) {
        LOG.info("updatetimeoutIfNotScheduled called state={} state.timeoutScheduled={}", state, state.timeoutScheduled, new Exception("trace"));
        if (0 == state.timeoutScheduled) {
            timeoutScheduler = Observable.interval(noSubscriptionTimeout, timeUnit).take(1);
            noTimeout = false;
        }
    }

    /** The common state. */
    private static final class State<T> {

        private final Action0 onUnsubscribe;
        private volatile Subscription releaseSubscription;
        private volatile String lastMem;

        private State(Action0 onUnsubscribe) {
            this.onUnsubscribe = onUnsubscribe;
            final BufferUntilSubscriber<T> bufferedSubject = BufferUntilSubscriber.create();
            bufferedObservable = bufferedSubject.lift(new AutoReleaseByteBufOperator<T>()); // Always auto-release
            bufferedObserver = bufferedSubject;
        }

        /**
         * Following are the only possible state transitions:
         * UNSUBSCRIBED -> SUBSCRIBED
         * UNSUBSCRIBED -> DISPOSED
         */
        private enum STATES {
            UNSUBSCRIBED /*Initial*/, SUBSCRIBED /*Terminal state*/, DISPOSED/*Terminal state*/
        }

        private volatile int state = STATES.UNSUBSCRIBED.ordinal(); /*Values are the ordinals of STATES enum*/

        private final Observer<T> bufferedObserver;
        private final Observable<T> bufferedObservable;

        @SuppressWarnings("unused")private volatile int timeoutScheduled; // Boolean

        /** Field updater for state. */
        @SuppressWarnings("rawtypes")
        private static final AtomicIntegerFieldUpdater<State> STATE_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(State.class, "state");

        /** Field updater for timeoutScheduled. */
        @SuppressWarnings("rawtypes")
        private static final AtomicIntegerFieldUpdater<State> TIMEOUT_SCHEDULED_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(State.class, "timeoutScheduled");

        public boolean casState(STATES expected, STATES next) {
            LOG.info("casState state={} expect={} next={} lastMem={}", this, expected, next, lastMem);
            return STATE_UPDATER.compareAndSet(this, expected.ordinal(), next.ordinal());
        }

        public boolean casTimeoutScheduled() {
            LOG.info("casTimeoutScheduled state={} lastMem={}", this, lastMem);
            return TIMEOUT_SCHEDULED_UPDATER.compareAndSet(this, 0, 1);
        }

        public void setReleaseSubscription(final Subscription releaseSubscription) {
            LOG.info("setReleaseSubscription: state={} lastMem={}", this, lastMem);
            this.releaseSubscription = releaseSubscription;
        }

        public void unsubscribeReleaseSubscription() {
            LOG.info("unsubscribeReleaseSubscription: state={} lastMem={}", this, lastMem);
            if(releaseSubscription != null) {
                releaseSubscription.unsubscribe();
            }
        }
    }

    private static final class OnSubscribeAction<T> implements OnSubscribe<T> {

        private final State<T> state;

        public OnSubscribeAction(State<T> state) {
            this.state = state;
        }

        @Override
        public void call(final Subscriber<? super T> subscriber) {
            LOG.info("OnSubscribeAction called state.state={} lastMem={}", state.state, state.lastMem);
            if (state.casState(State.STATES.UNSUBSCRIBED, State.STATES.SUBSCRIBED)) {

                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        if (null != state.onUnsubscribe) {
                            state.onUnsubscribe.call();
                        }
                    }
                }));

                state.bufferedObservable.unsafeSubscribe(subscriber);
                state.unsubscribeReleaseSubscription();

            } else if(State.STATES.SUBSCRIBED.ordinal() == state.state) {
                subscriber.onError(new IllegalStateException("Content can only have one subscription. Use Observable.publish() if you want to multicast."));
            } else if(State.STATES.DISPOSED.ordinal() == state.state) {
                subscriber.onError(new IllegalStateException("Content stream is already disposed."));
            }
        }
    }

    private static class AutoReleaseByteBufOperator<I> implements Operator<I, I> {
        @Override
        public Subscriber<? super I> call(final Subscriber<? super I> subscriber) {
            LOG.info("Added new AutoReleaseByteBufOperator");
            return new Subscriber<I>() {
                @Override
                public void onCompleted() {
                    LOG.info("AutoReleaseByteBufOperator onComplete called");
                    subscriber.onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    LOG.info("AutoReleaseByteBufOperator onError called");
                    subscriber.onError(e);
                }

                @Override
                public void onNext(I t) {
                    LOG.info("AutoReleaseByteBufOperator onNext called, t={}:{}", t.toString(), memAddress(t));
                    try {
                        touch(t, "onNext is next");
                        subscriber.onNext(t);
                    } finally {
                        ReferenceCountUtil.release(t);
                    }
                }
            };
        }
    }

    @Override
    public void onCompleted() {
        state.bufferedObserver.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        state.bufferedObserver.onError(e);
    }

    @Override
    public void onNext(final T t) {
        state.lastMem = memAddress(t);
        LOG.info("onNext called state={} buf={}:{}", state, t.toString(), state.lastMem, new Exception("trace"));
        // Retain so that post-buffer, the ByteBuf does not get released.
        // Release will be done after reading from the subject.
        ReferenceCountUtil.retain(t);
        BufferUntilSubscriber<T> bufferedSubject = (BufferUntilSubscriber<T>) state.bufferedObserver;
        touch(t, "bufferedObserver is next and bufferedObserver.hasObservers()= "
                 + bufferedSubject.hasObservers()
                 + " and this.state.state = " + state.state
                 + " and state.timeoutScheduled = " + state.timeoutScheduled
                 + " and noTimeout = " + noTimeout
                 + " and state.releaseSubscription = " + state.releaseSubscription
                 + " and buf =" + t.toString() + ":" + state.lastMem
                 + " and state =" + state.toString());
        state.bufferedObserver.onNext(t);

        // Schedule timeout once and when not subscribed yet.
        if (state.casTimeoutScheduled() && state.state == State.STATES.UNSUBSCRIBED.ordinal()) {
            LOG.info("scheduling release state={} buf={}:{}", state, t, state.lastMem, new Exception("trace"));
            // Schedule timeout after the first content arrives.
            state.setReleaseSubscription(timeoutScheduler.subscribe(new Action1<Long>() {
                @Override
                public void call(Long aLong) {
                    LOG.info("timeout for state={} buf={}:{}", state, t, memAddress(t));
                    disposeIfNotSubscribed();
                }
            }));
        }
    }

    @Override
    public boolean hasObservers() {
        return state.state == State.STATES.SUBSCRIBED.ordinal();
    }

    public static String memAddress(Object t) {
        if (t instanceof ByteBuf) {
            ByteBuf b = (ByteBuf) t;
            if (b.hasMemoryAddress()) {
                return Long.toString(b.memoryAddress());
            }
        }
        return "";
    }
    public static void touch(Object t, String hint) {
        if (t instanceof ByteBuf) {
            ((ByteBuf) t).touch(hint);
        }
    }
}
