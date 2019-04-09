package com.xiaoji.duan.aak;

import java.util.ArrayList;
import java.util.List;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class MainVerticle extends AbstractVerticle {

	private MongoClient mongodb = null;
	private AmqpBridge bridge = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		JsonObject config = new JsonObject();
		config.put("host", config().getString("mongo.host", "mongodb"));
		config.put("port", config().getInteger("mongo.port", 27017));
		config.put("keepAlive", config().getBoolean("mongo.keepalive", true));
		mongodb = MongoClient.createShared(vertx, config);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});

		connectStompServer();
	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"), config().getInteger("stomp.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						subscribeTrigger(config().getString("amq.app.id", "aak"));
					}
				});

	}

	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");
		JsonObject data = received.body().getJsonObject("body", new JsonObject());
		
		String operation = data.getJsonObject("context", new JsonObject()).getString("operation", data.getString("operation", "save"));
		String saprefix = data.getJsonObject("context", new JsonObject()).getString("saprefix", data.getString("saprefix", "none"));
		String collection = data.getJsonObject("context", new JsonObject()).getString("collection", data.getString("collection", "none"));
		JsonObject query = data.getJsonObject("context", new JsonObject()).getJsonObject("query", data.getJsonObject("query", new JsonObject()));

		String next = data.getJsonObject("context", new JsonObject()).getString("next", "none");

		if ("none".equals(saprefix) || "none".equals(collection)) {
			JsonObject body = data.getJsonObject("body", new JsonObject());
			
			if (!body.isEmpty()) {

				saprefix = body.getString("saprefix", "none");
				collection = body.getString("collection", "none");

				if (!"none".equals(saprefix) && !"none".equals(collection)) {
					data = body;
				}
			}
		}
		
		if ("save".equals(operation.toLowerCase())) {
			mongodb.insert(saprefix + "." + collection, data, res -> {});
		}
		
		if (!"none".equals(next.toLowerCase()) && "fetch".equals(operation.toLowerCase())) {
			mongodb.find(saprefix + "." + collection, query, res -> {
				if (res.succeeded()) {
					List<JsonObject> result = res.result();
					
					if (result == null)
						result = new ArrayList();
					
					JsonObject nextctx = new JsonObject()
							.put("context", new JsonObject()
									.put("stored", result));
					
					MessageProducer<JsonObject> producer = bridge.createProducer(next);
					producer.send(new JsonObject().put("body", nextctx));
					System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
				} else {
					JsonObject nextctx = new JsonObject()
							.put("context", new JsonObject()
									.put("stored", new JsonObject()));
					
					MessageProducer<JsonObject> producer = bridge.createProducer(next);
					producer.send(new JsonObject().put("body", nextctx));
					System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
				}
			});
		}
	}
}
