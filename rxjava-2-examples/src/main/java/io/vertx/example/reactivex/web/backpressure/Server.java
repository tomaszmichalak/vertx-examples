package io.vertx.example.reactivex.web.backpressure;

import io.netty.handler.codec.http.HttpStatusClass;
import io.reactivex.Flowable;
import io.vertx.core.Vertx;
import io.vertx.core.http.RequestOptions;
import io.vertx.example.util.Runner;
import io.vertx.reactivex.RxHelper;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import io.vertx.reactivex.core.http.HttpClientResponse;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.core.http.HttpServerRequest;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.BodyHandler;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author tomasz.michalak
 */
public class Server extends AbstractVerticle {

  private static final int NUMBER_OF_REQUESTS = 1000;

  // Convenience method so you can run it in your IDE
  public static void main(String[] args) throws InterruptedException {
    Runner.runExample(io.vertx.example.reactivex.web.backpressure.Server.class);
  }

  @Override
  public void start() throws Exception {

    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route().handler(req -> req.response().putHeader("content-type", "text/html")
      .end("<html><body><h1>Hello from vert.x!</h1></body></html>"));

    HttpServer server = vertx.createHttpServer();
    HttpClient httpClient = vertx.createHttpClient();

    server.requestStream()
      .toFlowable()
      .map(HttpServerRequest::pause)
      .onBackpressureDrop(req -> req.response().setStatusCode(503).end())
      .observeOn(RxHelper.scheduler(vertx.getDelegate()))
      .subscribe(req -> {
        req.resume();
        router.accept(req);
      });
    server.rxListen(8080).subscribe(res -> {
      AtomicInteger success = new AtomicInteger(0);
      AtomicInteger fail = new AtomicInteger(0);
      Flowable.fromIterable(requests(httpClient))
        .flatMap(r -> r)
        .subscribeOn(RxHelper.blockingScheduler(Vertx.vertx()))
        .subscribe(
          resp -> {
            if (HttpStatusClass.SUCCESS.contains(resp.statusCode())) {
              success.incrementAndGet();
            } else {
              fail.incrementAndGet();
            }
          },
          Throwable::printStackTrace,
          () -> System.out.println(
            "All requests completed [" + NUMBER_OF_REQUESTS + "], success [" + success.get()
              + "], fail [" + fail.get() + "]"));
    });
  }

  private List<Flowable<HttpClientResponse>> requests(HttpClient httpClient) {
    return Stream.generate(() -> request(httpClient)).limit(NUMBER_OF_REQUESTS)
      .collect(Collectors.toList());
  }

  private Flowable<HttpClientResponse> request(HttpClient httpClient) {
    HttpClientRequest req = httpClient.get(
      new RequestOptions()
        .setHost("localhost")
        .setPort(8080)
        .setURI("/"));
    return req
      .toFlowable()
      .doOnSubscribe(subscription -> req.end());
  }

}
