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
package io.reactivex.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.netty.metrics.MetricEventsSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A bridge between a {@link Connection} instance and the associated {@link Channel}.
 *
 * All operations on {@link Connection} will pass through this bridge to an appropriate action on the {@link Channel}
 *
 * <h2>Lazy {@link Connection#getInput()} subscription</h2>
 *
 * Lazy subscriptions are allowed on {@link Connection#getInput()} if and only if the channel is configured to
 * not read data automatically (i.e. {@link ChannelOption#AUTO_READ} is set to {@code false}). Otherwise,
 * if {@link Connection#getInput()} is subscribed lazily, the subscriber always recieves an error. The content
 * in this case is disposed upon reading.
 *
 * <h2>Backpressure</h2>
 *
 * Rx backpressure is built-in into this handler by using {@link CollaboratedReadInputSubscriber} for all
 * {@link Connection#getInput()} subscriptions.
 *
 * @param <R> Type read from the connection held by this handler.
 * @param <W> Type written to the connection held by this handler.
 */
public abstract class AbstractConnectionToChannelBridge<R, W> extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConnectionToChannelBridge.class);

    private static final IllegalStateException ONLY_ONE_CONN_SUB_ALLOWED =
            new IllegalStateException("Only one subscriber allowed for connection observable.");

    private static final IllegalStateException ONLY_ONE_CONN_INPUT_SUB_ALLOWED =
            new IllegalStateException("Only one subscriber allowed for connection input.");
    private static final IllegalStateException LAZY_CONN_INPUT_SUB =
            new IllegalStateException("Channel is set to auto-read but the subscription was lazy.");

    private static final byte[] BYTE_ARR_TO_FIND_W_TYPE = new byte[0];

    @SuppressWarnings("rawtypes")
    private final MetricEventsSubject eventsSubject;
    private final ChannelMetricEventProvider metricEventProvider;
    private final BytesWriteInterceptor bytesWriteInterceptor;
    private final boolean convertStringToBB;
    private final boolean convertByteArrToBB;
    private Subscriber<? super Connection<R, W>> newConnectionSub;
    private CollaboratedReadInputSubscriber<? super R> connInputSub;
    private boolean raiseErrorOnInputSubscription;
    private boolean connectionEmitted;

    protected AbstractConnectionToChannelBridge(MetricEventsSubject<?> eventsSubject,
                                                ChannelMetricEventProvider metricEventProvider) {
        this.eventsSubject = eventsSubject;
        this.metricEventProvider = metricEventProvider;
        bytesWriteInterceptor = new BytesWriteInterceptor();
        //TypeParameterMatcher matcher = TypeParameterMatcher.find(this, AbstractConnectionToChannelBridge.class, "W");
        convertStringToBB = false;
        convertByteArrToBB = false;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.pipeline().addFirst(bytesWriteInterceptor);
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        ctx.pipeline().remove(bytesWriteInterceptor);
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (!connectionEmitted) {
            createNewConnection(ctx.channel());
            connectionEmitted = true;
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (isValidToEmit(connInputSub)) {
            connInputSub.onCompleted();
        }

        super.channelUnregistered(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ConnectionSubscriberEvent) {
            @SuppressWarnings("unchecked")
            final ConnectionSubscriberEvent<R, W> connectionSubscriberEvent = (ConnectionSubscriberEvent<R, W>) evt;

            newConnectionSubscriber(connectionSubscriberEvent);
        } else if (evt instanceof ConnectionInputSubscriberEvent) {
            @SuppressWarnings("unchecked")
            ConnectionInputSubscriberEvent<R, W> event = (ConnectionInputSubscriberEvent<R, W>) evt;

            newConnectionInputSubscriber(ctx.channel(), event.getSubscriber());
        } else if (evt instanceof ConnectionInputSubscriberResetEvent) {
            resetConnectionInputSubscriber();
        } else if (evt == DrainInputSubscriberBuffer.INSTANCE) {
            drainInputSubscriberBuffer();
        }

        super.userEventTriggered(ctx, evt);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (isValidToEmit(connInputSub)) {
            try {
                connInputSub.onNext((R) msg);
            } catch (ClassCastException e) {
                ReferenceCountUtil.release(msg); // Since, this was not sent to the subscriber, release the msg.
                connInputSub.onError(e);
            }
        } else {
            ReferenceCountUtil.release(msg); // No consumer of the message, so discard.
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (!ctx.channel().config().isAutoRead() && connInputSub.shouldReadMore()) {
            ctx.read();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!connectionEmitted && isValidToEmit(newConnectionSub)) {
            newConnectionSub.onError(cause);
        } else if (isValidToEmit(connInputSub)) {
            connInputSub.onError(cause);
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Intercepts a write on the channel. The following message types are handled:
     *
     * <ul>
     <li>String: If the pipeline is not configured to write a String, this converts the string to a {@link ByteBuf} and
     then writes it on the channel.</li>
     <li>byte[]: If the pipeline is not configured to write a byte[], this converts the byte[] to a {@link ByteBuf} and
     then writes it on the channel.</li>
     <li>Observable: Subscribes to the {@link Observable} and writes all items, requesting the next item if an only if
     the channel is writable as indicated by {@link Channel#isWritable()}</li>
     </ul>
     *
     * @param ctx Channel handler context.
     * @param msg Message to write.
     * @param promise Promise for the completion of write.
     *
     * @throws Exception If there is an error handling this write.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (convertStringToBB && msg instanceof String) {
            /*If the charset is important, the user must convert the string to BB directly.*/
            byte[] msgAsBytes = ((String) msg).getBytes();
            ctx.write(ctx.alloc().buffer(msgAsBytes.length).writeBytes(msgAsBytes), promise);
        } else if(convertByteArrToBB && msg instanceof byte[]) {
            byte[] msgAsBytes = (byte[]) msg;
            ctx.write(ctx.alloc().buffer(msgAsBytes.length).writeBytes(msgAsBytes), promise);
        } else if (msg instanceof Observable) {
            @SuppressWarnings("unchecked")
            Observable<W> observable = (Observable<W>) msg;
            final WriteStreamSubscriber<W> subscriber = new WriteStreamSubscriber<W>(ctx, promise);
            bytesWriteInterceptor.addSubscriber(subscriber);
            observable.subscribe(subscriber);
        } else {
            ctx.write(msg, promise);
        }
    }

    private void createNewConnection(Channel channel) {
        if (null != newConnectionSub && !newConnectionSub.isUnsubscribed()) {
            Connection<R, W> connection = new ConnectionImpl<>(channel, eventsSubject, metricEventProvider);
            newConnectionSub.onNext(connection);
            connectionEmitted = true;
            checkEagerSubscriptionIfConfigured(connection, channel);
            newConnectionSub.onCompleted();
        } else {
            channel.close(); // Closing the connection if not sent to a subscriber.
        }
    }

    protected boolean isValidToEmit(Subscriber<?> subscriber) {
        return null != subscriber && !subscriber.isUnsubscribed();
    }

    protected void checkEagerSubscriptionIfConfigured(Connection<R, W> connection, Channel channel) {
        if (channel.config().isAutoRead() && null == connInputSub) {
            // If the channel is set to auto-read and there is no eager subscription then, we should raise errors
            // when a subscriber arrives.
            raiseErrorOnInputSubscription = true;
            final Subscriber<? super R> discardAll = ConnectionInputSubscriberEvent.discardAllInput(connection)
                                                                                   .getSubscriber();
            connInputSub = new CollaboratedReadInputSubscriber<R>(channel, discardAll) { };
        }
    }

    protected Subscriber<? super Connection<R, W>> getNewConnectionSub() {
        return newConnectionSub;
    }

    protected boolean isConnectionEmitted() {
        return connectionEmitted;
    }

    private void drainInputSubscriberBuffer() {
        /*
         * Drain unconditionally (even if unsubscribed) or else it will leak any ByteBuf contained in the buffer.
         * discard() checks if the subscriber is unsubscribed, then does not emit the notification.
         */
        connInputSub.drain();
    }

    private void resetConnectionInputSubscriber() {
        if (isValidToEmit(connInputSub)) {
            connInputSub.onCompleted();
        }
        connInputSub = null; // A subsequent event should set it to the desired subscriber.
    }

    private void newConnectionInputSubscriber(final Channel channel, final Subscriber<? super R> subscriber) {
        if (null != connInputSub) {
            subscriber.onError(ONLY_ONE_CONN_INPUT_SUB_ALLOWED);
        } else if (raiseErrorOnInputSubscription) {
            subscriber.onError(LAZY_CONN_INPUT_SUB);
        } else {
            connInputSub = new CollaboratedReadInputSubscriber<R>(channel, subscriber) { };
        }
    }

    private void newConnectionSubscriber(ConnectionSubscriberEvent<R, W> event) {
        if (null == newConnectionSub) {
            newConnectionSub = event.getSubscriber();
        } else {
            event.getSubscriber().onError(ONLY_ONE_CONN_SUB_ALLOWED);
        }
    }

    static final class DrainInputSubscriberBuffer {

        public static final DrainInputSubscriberBuffer INSTANCE = new DrainInputSubscriberBuffer();

        private DrainInputSubscriberBuffer() {
            // No state, no instance.
        }

    }

    /**
     * Regulates write->request more->write process on the channel.
     *
     * Why is this a separate handler?
     * The sole purpose of this handler is to request more items from each of the Observable streams producing items to
     * write. It is important to request more items only when the current item is written on the channel i.e. added to
     * the ChannelOutboundBuffer. If we request more from outside the pipeline (from WriteStreamSubscriber.onNext())
     * then it may so happen that the onNext is not from within this eventloop and hence instead of being written to
     * the channel, is added to the task queue of the EventLoop. Requesting more items in such a case, would mean we
     * keep adding the writes to the eventloop queue and not on the channel buffer. This would mean that the channel
     * writability would not truly indicate the buffer.
     */
    private final class BytesWriteInterceptor extends ChannelDuplexHandler {

        /*
         * Since, unsubscribes can happen on a different thread, this has to be thread-safe.
         */
        private final ConcurrentLinkedQueue<WriteStreamSubscriber<W>> subscribers = new ConcurrentLinkedQueue<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ctx.write(msg, promise);
            requestMoreIfWritable(ctx);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            if (ctx.channel().isWritable()) {
                requestMoreIfWritable(ctx);
            }
            super.channelWritabilityChanged(ctx);
        }

        public void addSubscriber(final WriteStreamSubscriber<W> streamSubscriber) {
            streamSubscriber.add(Subscriptions.create(new Action0() {
                @Override
                public void call() {
                    /*Remove the subscriber on unsubscribe.*/
                    subscribers.remove(streamSubscriber);
                }
            }));

            subscribers.add(streamSubscriber);
        }

        private void requestMoreIfWritable(ChannelHandlerContext ctx) {
            /**
             * Predicting which subscriber is going to give the next item isn't possible, so all subscribers are
             * requested an item. This means that we buffer a max of one item per subscriber.
             */
            for (WriteStreamSubscriber<W> subscriber: subscribers) {
                if (!subscriber.isUnsubscribed() && ctx.channel().isWritable()) {
                    subscriber.requestMore(1);
                }
            }
        }
    }

    /**
     * Backpressure enabled subscriber to an Observable written on this channel. This connects the promise for writing
     * the Observable to all the promises created per write (per onNext).
     *
     * @param <W> Type of Objects received by this subscriber.
     */
    private static final class WriteStreamSubscriber<W> extends Subscriber<W> {

        private final ChannelHandlerContext ctx;
        private final ChannelPromise overarchingWritePromise;
        private final Object guard = new Object();
        private boolean isDone; /*Guarded by guard*/
        private boolean isPromiseCompletedOnWriteComplete; /*Guarded by guard. Only transition is false->true*/

        private int listeningTo;

        private WriteStreamSubscriber(ChannelHandlerContext ctx, ChannelPromise promise) {
            this.ctx = ctx;
            overarchingWritePromise = promise;
        }

        @Override
        public void onStart() {
            request(1); /*Only request one item at a time. Every write, requests one more, if channel is writable.*/
        }

        @Override
        public void onCompleted() {
            onTermination();
        }

        @Override
        public void onError(Throwable e) {
            onTermination();
        }

        @Override
        public void onNext(W nextItem) {

            final ChannelFuture channelFuture = ctx.write(nextItem);
            synchronized (guard) {
                listeningTo++;
            }

            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    if (overarchingWritePromise.isDone()) {
                        /*
                         * Overarching promise will be done if and only if there was an error or all futures have
                         * completed. In both cases, this callback is useless, hence return from here.
                         * IOW, if we are here, it can be two cases:
                         *
                         * - There has already been a write that has failed. So, the promise is done with failure.
                         * - There was a write that arrived after termination of the Observable.
                         *
                         * Two above isn't possible as per Rx contract.
                         * One above is possible but is not of any consequence w.r.t this listener as this listener does
                         * not give callbacks to specific writes
                         */
                        return;
                    }

                    /**
                     * The intent here is to NOT give listener callbacks via promise completion within the sync block.
                     * So, a co-ordination b/w the thread sending Observable terminal event and thread sending write
                     * completion event is required.
                     * The only work to be done in the Observable terminal event thread is to whether the
                     * overarchingWritePromise is to be completed or not.
                     * The errors are not buffered, so the overarchingWritePromise is completed in this callback w/o
                     * knowing whether any more writes will arive or not.
                     * This co-oridantion is done via the flag isPromiseCompletedOnWriteComplete
                     */
                    synchronized (guard) {
                        listeningTo--;
                        if (0 == listeningTo && isDone) {
                            /**
                             * If the listening count is 0 and no more items will arive, this thread wins the race of
                             * completing the overarchingWritePromise
                             */
                            isPromiseCompletedOnWriteComplete = true;
                        }
                    }

                    /**
                     * Exceptions are not buffered but completion is only sent when there are no more items to be
                     * received for write.
                     */
                    if (!future.isSuccess()) {
                        overarchingWritePromise.tryFailure(future.cause());
                    } else if (isPromiseCompletedOnWriteComplete) { /*Once set to true, never goes back to false.*/
                        /*Complete only when no more items will arrive and all writes are completed*/
                        overarchingWritePromise.trySuccess();
                    }
                }
            });
        }

        private void onTermination() {
            int _listeningTo;
            boolean _shouldCompletePromise;

            /**
             * The intent here is to NOT give listener callbacks via promise completion within the sync block.
             * So, a co-ordination b/w the thread sending Observable terminal event and thread sending write
             * completion event is required.
             * The only work to be done in the Observable terminal event thread is to whether the
             * overarchingWritePromise is to be completed or not.
             * The errors are not buffered, so the overarchingWritePromise is completed in this callback w/o
             * knowing whether any more writes will arive or not.
             * This co-oridantion is done via the flag isPromiseCompletedOnWriteComplete
             */
            synchronized (guard) {
                isDone = true;
                _listeningTo = listeningTo;
                /**
                 * Flag to indicate whether the write complete thread won the race and will complete the
                 * overarchingWritePromise
                 */
                _shouldCompletePromise = 0 == _listeningTo && !isPromiseCompletedOnWriteComplete;
            }

            if (_shouldCompletePromise) {
                /* The co-ordination is for */
                overarchingWritePromise.trySuccess();
            }
        }

        private void requestMore(long more) {
            request(more);
        }
    }
}
