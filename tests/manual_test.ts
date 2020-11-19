import {
  Lock,
  NatsServer,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/fix-79/tests/helpers/mod.ts";
import {
  DataBuffer,
  delay,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/fix-79/nats-base-client/internal_mod.ts";
import {
  connect,
  createInbox,
  Empty,
  ErrorCode,
  headers,
  JSONCodec,
  StringCodec,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/fix-79/src/mod.ts";
import { assertEquals } from "https://deno.land/std@0.63.0/testing/asserts.ts";

import {
  AckPolicy,
  DiscardPolicy,
  jsmClient,
  RetentionPolicy,
  StorageType,
} from "../src/mod.ts";
import { ns as nanos } from "../src/types.ts";
import { serverOpts } from "./util.ts";

Deno.test("manual - stream creation", async () => {
  const ns = await NatsServer.start(serverOpts());
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
  const ns = await NatsServer.start(serverOpts());
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
  const sc = JSONCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    { durable_name: "NEW", filter_subject: "orders.new" },
  );

  const a = [false, false, false];
  const max = 3;
  for (let i = 0; i < max; i++) {
    nc.publish("orders.new", sc.encode(i));
  }

  while (true) {
    try {
      const m = await nc.request(
        "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
        undefined,
        { timeout: 1000, noMux: true },
      );
      a[sc.decode(m.data) as number] = true;
      m.respond(sc.encode("+ACK"));
    } catch (err) {
      if (err.code === ErrorCode.TIMEOUT) {
        break;
      }
    }
  }

  for (let i = 0; i < max; i++) {
    assertEquals(a[i], true);
  }

  await nc.close();
  await ns.stop();
});

Deno.test("manual - create push consumer", async () => {
  const ns = await NatsServer.start(serverOpts());
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
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const lock = Lock(3);
  const jsm = jsmClient(nc);
  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create("ORDERS", {
    durable_name: "NEW",
    filter_subject: "orders.new",
    deliver_subject: "new.orders",
  });

  const a = [false, false, false];
  const max = 3;
  const jc = JSONCodec();

  for (let i = 0; i < max; i++) {
    nc.publish("orders.new", jc.encode(i));
  }

  nc.subscribe("new.orders", {
    callback: (err, msg) => {
      lock.unlock();
      a[jc.decode(msg.data)] = true;
      msg.respond(jc.encode("+ACK"));
    },
  });
  await lock;

  for (let i = 0; i < max; i++) {
    assertEquals(a[i], true);
  }

  await nc.close();
  await ns.stop();
});

Deno.test("manual - ephemeral stream", async () => {
  const ns = await NatsServer.start(serverOpts());
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
  const ns = await NatsServer.start(serverOpts());
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

Deno.test("manual - pull inflight acks expired", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(500),
    },
  );

  nc.publish("orders.new", sc.encode("1"));

  let ci = await jsm.consumers.info("ORDERS", "NEW");
  assertEquals(ci.num_pending, 1);

  const m = await nc.request(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { timeout: 1000, noMux: true },
  );

  ci = await jsm.consumers.info("ORDERS", "NEW");
  assertEquals(ci.num_ack_pending, 1);
  // now we wait to have the ack expire
  await delay(2000);

  ci = await jsm.consumers.info("ORDERS", "NEW");
  // FIXME: this will fail because of a bug on resetting num_ack_pending
  // back to the count of numpending:
  // assertEquals(ci.num_pending, 1);
  console.log(ci)
  assertEquals(ci.num_ack_pending, 1);

  const m2 = await nc.request(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { timeout: 1000, noMux: true },
  );
  m.respond(sc.encode("+ACK"));

  ci = await jsm.consumers.info("ORDERS", "NEW");
  assertEquals(ci.num_pending, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - pull next", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
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

  const ci = await jsm.consumers.info("ORDERS", "NEW");
  assertEquals(ci.num_pending, 0);

  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("manual - with timeout", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
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

Deno.test("manual - nxt with options", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });
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

  const inbox = createInbox();
  const lock = Lock(4, 30000);
  let to = 0;

  nc.subscribe(inbox, {
    callback: (err, msg) => {
      if (err) {
        console.error(err);
        return;
      }
      if (msg.headers && msg.headers.hasError) {
        console.log(`no messages - ${msg.headers.status}`);
        lock.unlock();
        return;
      }
      if (!msg.reply) {
        return;
      }
      const opts = {
        batch: 1,
        no_wait: true,
      };
      msg.respond(sc.encode(`+NXT ${JSON.stringify(opts)}`), { reply: inbox });
      lock.unlock();
    },
  });

  nc.publish(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    Empty,
    { reply: inbox },
  );

  await lock;
  if (to) {
    clearTimeout(to);
  }

  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("manual - info", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });

  const jc = JSONCodec();
  const m = await nc.request("$JS.API.INFO");
  const info = jc.decode(m.data);

  console.dir(info);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - snapshot", async () => {
  const jc = JSONCodec();
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });

  const jsm = jsmClient(nc);
  let names = await jsm.streams.names();
  if (names.streams && names.streams.indexOf("ORDERS") > -1) {
    await jsm.streams.delete("ORDERS");
  }
  await jsm.streams.create("ORDERS", {
    storage: StorageType.File,
    subjects: ["orders.>"],
    retention: RetentionPolicy.WorkQueue,
  });

  await nc.request("orders.new", jc.encode(1));
  await nc.request("orders.new", jc.encode(2));
  await nc.request("orders.new", jc.encode(3));
  await nc.request("orders.new", jc.encode(4));
  await nc.request("orders.new", jc.encode(5));

  let info = await jsm.streams.info("ORDERS");
  assertEquals(info.state.last_seq, 5);

  const inbox = createInbox();
  const buffer = new DataBuffer();
  const sub = nc.subscribe(inbox);
  const done = (async () => {
    for await (const m of sub) {
      m.respond();
      if (m.data.length === 0) {
        break;
      }
      buffer.fill(m.data);
    }
  })();

  const r = await nc.request(
    "$JS.API.STREAM.SNAPSHOT.ORDERS",
    jc.encode({
      deliver_subject: inbox,
      no_consumers: true,
      jsck: true,
    }),
  );
  const ok = jc.decode(r.data);
  if (ok.numblks === 0) {
    await sub.unsubscribe();
  }
  assertEquals(ok.err, undefined);
  await done;

  await jsm.streams.delete("ORDERS");
  const m = await nc.request("$JS.API.STREAM.RESTORE.ORDERS");
  const rc = jc.decode(m.data);
  await nc.request(rc.deliver_subject, buffer.drain());
  const rr = await nc.request(rc.deliver_subject);
  info = await jsm.streams.info("ORDERS");
  assertEquals(info.state.last_seq, 5);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - consumer samples", async () => {
  const jc = JSONCodec();
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  let names = await jsm.streams.names();
  if (names.streams && names.streams.indexOf("ORDERS") > -1) {
    await jsm.streams.delete("ORDERS");
  }

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(1000),
      sample_freq: "100%",
    },
  );

  const N = 1000;
  for (let i = 0; i < N; i++) {
    nc.publish("orders.new", jc.encode(i));
  }

  const samples = nc.subscribe("$JS.EVENT.METRIC.CONSUMER.ACK.ORDERS.NEW");
  const done = (async () => {
    for await (const m of samples) {
      if (samples.getProcessed() === 1) {
        console.log(jc.decode(m.data));
      }
      if (samples.getProcessed() === N) {
        break;
      }
    }
  })();

  const inbox = createInbox();
  nc.subscribe(inbox, {
    callback: (err, msg) => {
      if (msg.reply === "") {
        return;
      }
      msg.respond(sc.encode("+NXT"), { reply: inbox });
    },
  });

  nc.publish(
    "$JS.API.CONSUMER.MSG.NEXT.ORDERS.NEW",
    undefined,
    { reply: inbox },
  );

  await done;

  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("manual - max deliveries", async () => {
  const jc = JSONCodec();
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  const sub = nc.subscribe("neworder");
  (async () => {
    for await (const m of sub) {
      const v = jc.decode(m.data);
      console.log(v);
      if (m.reply === "") {
        return;
      }

      console.log(m.headers);

      if (v === 2) {
        console.log("ignoring 2!");
        m.respond(sc.encode("-NAK"));
      } else {
        m.respond(sc.encode("+ACK"));
      }
    }
  })().then();

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  await jsm.consumers.create(
    "ORDERS",
    {
      ack_policy: AckPolicy.Explicit,
      deliver_subject: "neworder",
      durable_name: "NEW",
      filter_subject: "orders.new",
      ack_wait: nanos(1000),
      max_deliver: 3,
    },
  );

  const samples = nc.subscribe(
    "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.NEW",
  );
  const done = (async () => {
    for await (const m of samples) {
      console.log(jc.decode(m.data));
      break;
    }
  })();

  const N = 2;
  for (let i = 0; i < N; i++) {
    nc.publish("orders.new", jc.encode(i + 1));
  }

  await done;
  await nc.flush();
  await nc.close();
  await ns.stop();
});

Deno.test("manual - duplicates", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });
  const jsm = jsmClient(nc);

  await jsm.streams.create("ORDERS", { subjects: ["orders.*"] });
  const N = 15;
  const hdrs = headers();
  hdrs.set("Msg-Id", "1");
  for (let i = 0; i < N; i++) {
    nc.publish("orders.new", sc.encode("data"), { headers: hdrs });
  }
  await nc.flush();
  const info = await jsm.streams.info("ORDERS");
  console.log(info);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - create template", async () => {
  const jc = JSONCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });

  const config = {
    name: "ORDERS",
    max_streams: 100,
    config: {
      name: "",
      subjects: ["orders.>"],
      retention: RetentionPolicy.Limits,
      max_consumers: -1,
      max_msgs: -1,
      max_bytes: -1,
      max_age: 0,
      max_msg_size: -1,
      storage: StorageType.Memory,
      discard: DiscardPolicy.Old,
      num_replicas: 1,
    },
  };

  const cr = await nc.request(
    "$JS.API.STREAM.TEMPLATE.CREATE.ORDERS",
    jc.encode(config),
  );
  const r = jc.decode(cr.data);
  console.log(r);
  if (r.error) {
    throw new Error(r.error);
  }
  assertEquals(r.error, undefined);
  nc.publish("orders.new");

  const rr = await nc.request("$JS.API.STREAM.TEMPLATE.INFO.ORDERS");
  const rrr = jc.decode(rr.data);
  console.log(rrr);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - template info", async () => {
  const jc = JSONCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });
  const jsm = jsmClient(nc);

  await jsm.templates.create("ORDERS", 100, { subjects: ["orders.>"] });
  nc.publish("orders.new");
  const ir = await nc.request("$JS.API.STREAM.TEMPLATE.INFO.ORDERS");
  const ird = jc.decode(ir.data);
  console.log(ird);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - template names", async () => {
  const jc = JSONCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });
  const jsm = jsmClient(nc);

  await jsm.templates.create("ORDERS", 100, { subjects: ["orders.>"] });
  const ir = await nc.request("$JS.API.STREAM.TEMPLATE.NAMES");
  const names = jc.decode(ir.data);
  console.log(names);

  await nc.close();
  await ns.stop();
});

Deno.test("manual - template delete", async () => {
  const jc = JSONCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port, headers: true });
  const jsm = jsmClient(nc);

  await jsm.templates.create("ORDERS", 100, { subjects: ["orders.>"] });
  nc.publish("orders.new");

  const ir = await nc.request("$JS.API.STREAM.TEMPLATE.DELETE.ORDERS");
  const deleted = jc.decode(ir.data);
  console.log(deleted);

  await nc.close();
  await ns.stop();
});
