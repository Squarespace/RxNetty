package io.reactivex.netty.examples.http;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.client.RxClient;
import io.reactivex.netty.examples.http.helloworld.HelloWorldServer;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.server.HttpServer;
import rx.Observable;

public class ByteBufMemLeakTest {

  private HttpServer<ByteBuf, ByteBuf> server;

  @Before
  public void setUp() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    server = new HelloWorldServer(0).createServerChunked();
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.shutdown();
  }

  @Test
  public void testMemLeak() throws Exception {
    RxClient.ClientConfig config = new HttpClient.HttpClientConfig.Builder()
        .readTimeout(500, TimeUnit.MILLISECONDS)
        .responseSubscriptionTimeout(1, TimeUnit.MILLISECONDS)
        .build();
    HttpClient<ByteBuf, ByteBuf> client =
        RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("localhost", server.getServerPort())
        .config(config)
        .build();
    HttpClientRequest<ByteBuf> req = HttpClientRequest.createGet("/hello");

    int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    ExecutorService exec = Executors.newFixedThreadPool(nThreads);

    for (int i = 0; i < nThreads; i++) {
      exec.submit(() -> {
        while (true) {
          try {
            Observable.merge(
                Observable.timer(250, TimeUnit.MILLISECONDS)
                    .flatMap((l) -> Observable.error(new TimeoutException("test"))),
                client.submit(req))
                .map((next) -> {
                  try {
                    Thread.sleep(10000);
                  } catch (Exception e) {
                    //
                  }
                  return next;
                })
                .toBlocking()
                .single();
          } catch (Exception e) {
            //
          }
        }
      });
    }

    TimeUnit.HOURS.sleep(1);
  }
}
