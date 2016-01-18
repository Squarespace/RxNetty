/*
 * Copyright 2014 Netflix, Inc.
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
package io.reactivex.netty.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.netty.channel.NewRxConnectionEvent;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.client.ConnectionReuseEvent;
import rx.Observer;

/**
 * An adapter that converts a message generated by netty's pipeline to an Observable event. <br/>
 *
 * @author Nitesh Kant
 */
public class ObservableAdapter extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ObservableAdapter.class);

    @SuppressWarnings("rawtypes")
    /*Nullable*/ private Observer bridgedObserver; /*This actually is an Rx Subject*/

    private boolean autoReleaseBuffers;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        LOG.info("handlerAdded this={} channel={}", this, ctx.channel());
        Boolean autoRelease = ctx.channel().attr(ObservableConnection.AUTO_RELEASE_BUFFERS).get();
        autoReleaseBuffers = null == autoRelease || autoRelease;
        super.handlerAdded(ctx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOG.info("channelRead this={} channel={} msg={}", this, ctx.channel(), msg);
        if (null != bridgedObserver) {
            try {
                bridgedObserver.onNext(msg);
            } catch (ClassCastException cce) {
                bridgedObserver.onError(new RuntimeException("Mismatched message type.", cce));
            } finally {
                if (autoReleaseBuffers) {
                    ReferenceCountUtil.release(msg);
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.info("exceptionCaught this={} channel={}", this, ctx.channel(), cause);
        if (null != bridgedObserver) {
            bridgedObserver.onError(cause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("channelInactive this={} channel={}", this, ctx.channel());
        if (null != bridgedObserver) {
            bridgedObserver.onCompleted();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        LOG.info("userEventTriggered this={} channel={} event={}", this, ctx.channel(), event);
        if (event instanceof NewRxConnectionEvent) {
            NewRxConnectionEvent rxConnectionEvent = (NewRxConnectionEvent) event;
            bridgedObserver = rxConnectionEvent.getConnectedObserver();
        } else if (event instanceof ConnectionReuseEvent) {
            ConnectionReuseEvent reuseEvent = (ConnectionReuseEvent) event;
            bridgedObserver = reuseEvent.getConnectedObserver();
        }

        super.userEventTriggered(ctx, event);
    }
}
