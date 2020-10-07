import {
  Lock,
  NatsServer,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/mod.ts";
import {
  delay,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/internal_mod.ts";
import {
  connect,
  ErrorCode,
  JSONCodec,
  StringCodec,
  createInbox,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts";
import { assertEquals } from "https://deno.land/std@0.63.0/testing/asserts.ts";

import { jsmClient, RetentionPolicy, StorageType } from "../src/mod.ts";
import { ns as nanos } from "../src/types.ts";

const jsopts = {
  // debug: true,
  // trace: true,
  jetstream: {
    max_memory_store: 1024 * 1024,
    max_file_store: 1,
    store_dir: "/tmp",
  },
};

Deno.test("manual - stream creation", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jc = JSONCodec();
  const orders = jc.encode({
    name: "ORDERS",
    subjects: ["orders.*"],
    retention: "limits",
    max_consumers: -1,
    max_msgs: -1,
    max_bytes: -1,
    max_age: 0,
    storage: "memory",
    replicas: 1,
    no_ack: false,
    discard: "old",
    duplicate_window: 100000,
  });
  const m = await nc.request("$JS.API.STREAM.CREATE.ORDERS", orders);
  const r = jc.decode(m.data);
  assertEquals(r.error, undefined);
  if (r.error) console.error(`error creating stream: ${r.error}`);
  await nc.close();
  await ns.stop();
});

Deno.test("manual - consumer creation", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);
  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });

  const jc = JSONCodec();
  const cons = jc.encode({
    stream_name: "ORDERS",
    config: {
      durable_name: "NEW",
      filter_subject: "orders.new",
      deliver_policy: "all",
      ack_policy: "explicit",
      ack_wait: 30 * 1000 * 1000000,
      replay_policy: "instant",
    },
  });
  const m = await nc.request(
    "$JS.API.CONSUMER.DURABLE.CREATE.ORDERS.NEW",
    cons,
  );
  const r = jc.decode(m.data);
  assertEquals(r.error, undefined);
  if (r.error) console.error(`error creating stream: ${r.error}`);
  await nc.close();
  await ns.stop();
});

Deno.test("manual - pull consumer", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    { durable_name: "NEW", filter_subject: "orders.new" },
  );

  nc.publish("orders.new", sc.encode("1"));
  nc.publish("orders.new", sc.encode("2"));
  nc.publish("orders.new", sc.encode("3"));

  while (true) {
    try {
      const m = await nc.request(
        "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
        undefined,
        { timeout: 1000, noMux: true },
      );
      console.log(m.subject, sc.decode(m.data));
      m.respond(sc.encode("+ACK"));
    } catch (err) {
      if (err.code === ErrorCode.TIMEOUT) {
        break;
      }
    }
  }
  await nc.close();
  await ns.stop();
});

Deno.test("manual - create push consumer", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);
  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });

  const jc = JSONCodec();
  const cons = jc.encode({
    stream_name: "ORDERS",
    config: {
      durable_name: "NEW",
      deliver_subject: "new.orders",
      filter_subject: "orders.new",
      deliver_policy: "all",
      ack_policy: "explicit",
      ack_wait: 30 * 1000 * 1000000,
      replay_policy: "instant",
    },
  });
  const m = await nc.request(
    "$JS.API.CONSUMER.DURABLE.CREATE.ORDERS.NEW",
    cons,
  );
  const r = jc.decode(m.data);
  assertEquals(r.error, undefined);
  if (r.error) console.error(`error creating consumer: ${r.error}`);
  await nc.close();
  await ns.stop();
});

Deno.test("manual - push consumer", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const lock = Lock(3);
  const jsm = jsmClient(nc);
  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create("ORDERS", {
    durable_name: "NEW",
    filter_subject: "orders.new",
    deliver_subject: "new.orders",
  });

  const sc = StringCodec();
  nc.publish("orders.new", sc.encode("1"));
  nc.publish("orders.new", sc.encode("2"));
  nc.publish("orders.new", sc.encode("3"));

  nc.subscribe("new.orders", {
    callback: (err, msg) => {
      lock.unlock();
      console.log(msg.subject, sc.decode(msg.data));
      msg.respond(sc.encode("+ACK"));
    },
  });

  await lock;
  await nc.close();
  await ns.stop();
});

Deno.test("manual - ephemeral stream", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });

  const jc = JSONCodec();
  const orders = jc.encode({
    name: "ORDERS",
    subjects: ["orders.*"],
    retention: "workqueue",
    max_consumers: -1,
    max_msgs: -1,
    max_bytes: -1,
    max_age: 0,
    storage: "memory",
    replicas: 1,
    no_ack: false,
    discard: "old",
    duplicate_window: 100000,
  });
  const m = await nc.request("$JS.API.STREAM.CREATE.ORDERS", orders);
  const r = jc.decode(m.data);
  assertEquals(r.error, undefined);
  if (r.error) console.error(`error creating stream: ${r.error}`);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - ephemeral consumer", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });

  const jsm = jsmClient(nc);
  await jsm.streams.create("ORDERS", {
    retention: RetentionPolicy.WorkQueue,
    subjects: ["orders.*"],
    storage: StorageType.Memory,
  });

  const sc = StringCodec();
  nc.publish("orders.new", sc.encode("1"));
  nc.publish("orders.new", sc.encode("2"));
  nc.publish("orders.new", sc.encode("3"));

  let s = await jsm.streams.info("ORDERS");
  assertEquals(s.state.messages, 3);

  const lock = Lock(3);

  nc.subscribe("new.orders", {
    callback: (err, msg) => {
      lock.unlock();
      console.log(msg.subject, sc.decode(msg.data));
      msg.respond(sc.encode("+ACK"));
    },
  });

  const jc = JSONCodec();
  const cons = jc.encode({
    stream_name: "ORDERS",
    config: {
      deliver_subject: "new.orders",
      filter_subject: "orders.new",
      deliver_policy: "all",
      ack_policy: "explicit",
      ack_wait: 30 * 1000 * 1000000,
      replay_policy: "instant",
    },
  });
  const m = await nc.request(
    "$JS.API.CONSUMER.CREATE.ORDERS",
    cons,
  );
  const r = jc.decode(m.data);
  assertEquals(r.error, undefined);
  if (r.error) console.error(`error creating consumer: ${r.error}`);

  await lock;

  s = await jsm.streams.info("ORDERS");
  assertEquals(s.state.messages, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - acks", () => {
  const sc = StringCodec();

  console.log(sc.decode(Uint8Array.of(43, 65, 67, 75)));
  console.log(sc.decode(Uint8Array.of(45, 78, 65, 75)));
  console.log(sc.decode(Uint8Array.of(43, 87, 80, 73)));
  console.log(sc.decode(Uint8Array.of(43, 78, 88, 84)));
  console.log(sc.decode(Uint8Array.of(43, 84, 69, 82, 77)));
});

Deno.test("manual - pull inflight acks expired", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(1000),
    },
  );

  nc.publish("orders.new", sc.encode("1"));

  let ci = await jsm.consumers.info("ORDERS", "NEW");

  const m = await nc.request(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { timeout: 1000, noMux: true },
  );
  // now we wait
  await delay(2000);

  ci = await jsm.consumers.info("ORDERS", "NEW");

  const m2 = await nc.request(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { timeout: 1000, noMux: true },
  );

  m.respond(sc.encode("+ACK"));

  await delay(1000);

  ci = await jsm.consumers.info("ORDERS", "NEW");
  assertEquals(ci.num_pending, 1);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - pull next", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(1000),
    },
  );

  nc.publish("orders.new", sc.encode("1"));
  nc.publish("orders.new", sc.encode("2"));
  nc.publish("orders.new", sc.encode("3"));

  const lock = Lock(3);
  const inbox = createInbox();
  nc.subscribe(inbox, {
    callback: (err, msg) => {
      if (msg.reply === "") {
        return;
      }
      console.log(msg.reply);
      msg.respond(sc.encode("+NXT"), { reply: inbox });
      lock.unlock();
    },
  });

  nc.publish(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { reply: inbox },
  );

  await lock;

  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("manual - with timeout", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(1000),
    },
  );

  nc.publish("orders.new", sc.encode("1"));
  nc.publish("orders.new", sc.encode("2"));
  nc.publish("orders.new", sc.encode("3"));

  const lock = Lock(6, 30000);
  const inbox = createInbox();
  let to = 0;
  function wd(millis: number) {
    if (to) {
      clearTimeout(to);
    }
    to = setTimeout(() => {
      lock.unlock();
      nc.publish(
        "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
        sc.encode("20"),
        { reply: inbox },
      );
      wd(millis);
    }, millis);
  }

  nc.subscribe(inbox, {
    callback: (err, msg) => {
      wd(5000);
      if (msg.reply === "") {
        return;
      }
      msg.respond(sc.encode("+NXT"), { reply: inbox });
      lock.unlock();
    },
  });

  wd(0);

  await lock;
  if (to) {
    clearTimeout(to);
  }

  await nc.flush();
  await nc.close();
  await ns.stop();
});
