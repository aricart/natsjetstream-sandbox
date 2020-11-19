import {
  NatsServer,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/mod.ts";
import {
  connect,
  createInbox,
  JSONCodec,
  StringCodec,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts";
import {
  assert,
  assertEquals,
} from "https://deno.land/std@0.63.0/testing/asserts.ts";

import { defaultConsumer, jsmClient } from "../src/mod.ts";
import { PullConsumer, PushConsumer } from "../src/consumer.ts";
import { pullSubject } from "../src/jsm.ts";
import { serverOpts } from "./util.ts";

Deno.test("basics - info", async () => {
  const opts = serverOpts()
  const ns = await NatsServer.start(opts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);
  const d = await jsm.info();
  const limits = d.limits ? d.limits : undefined
  assert(limits)
  assertEquals(limits.max_memory, opts.jetstream.max_memory_store)
  assertEquals(limits.max_storage, opts.jetstream.max_file_store)
  await nc.close();
  await ns.stop();
});

Deno.test("basics - stream crud", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  let streams = await jsm.streams.list();
  assertEquals(streams.streams, []);

  let names = await jsm.streams.names();
  assertEquals(names.streams, null);

  const s = await jsm.streams.create("foo", { subjects: ["foo"] });
  assertEquals(s?.config?.name, "foo");

  names = await jsm.streams.names();
  assertEquals(names.streams, ["foo"]);

  streams = await jsm.streams.list();
  assertEquals(streams.streams.length, 1);

  const jc = JSONCodec();
  await nc.request(
    "foo",
    jc.encode({ key: "value" }),
    { noMux: true, timeout: 1000 },
  );

  const stream = await jsm.streams.info("foo");
  assertEquals(stream.config, streams.streams[0].config);
  assertEquals(s.config, stream.config);

  const m = await jsm.streams.get("foo", 1);
  const p = jc.decode(m.data);
  assertEquals({ key: "value" }, p);

  assert(await jsm.streams.delete("foo"));
  streams = await jsm.streams.list();
  assertEquals(streams.streams.length, 0);

  await nc.close();
  await ns.stop();
});

Deno.test("basics - manual consumer", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  const sc = StringCodec();

  const s = await jsm.streams.create("foo", { subjects: ["foo"] });
  assertEquals(s?.config?.name, "foo");

  let copts = defaultConsumer();
  copts.durable_name = "foodur";

  const c = await jsm.consumers.create("foo", copts);
  const subj = pullSubject("foo", "foodur");
  const inbox = createInbox();

  const sub = nc.subscribe(inbox, { max: 1 });
  const done = (async () => {
    for await (const m of sub) {}
  })();

  nc.publish(subj, undefined, { reply: inbox });

  nc.publish("foo", sc.encode("Hello"));

  await done;
  await nc.close();
  await ns.stop();
});

Deno.test("basics - push consumer", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  const sc = StringCodec();
  const s = await jsm.streams.create("foo", { subjects: ["foo"] });
  assertEquals(s?.config?.name, "foo");
  const opts = defaultConsumer(
    { deliver_subject: "myfoo", durable_name: "fddd" },
  );
  const ci = await jsm.consumers.create("foo", opts);
  const consumer = PushConsumer.fromConsumerInfo(nc, ci);
  const done = (async () => {
    for await (const jm of consumer) {
      if (consumer.getProcessed() === 3) {
        consumer.sub.unsubscribe();
        break;
      }
    }
  })();

  for (let i = 0; i < 3; i++) {
    nc.publish("foo", sc.encode(`Hello${i + 1}`));
  }

  await done;

  await nc.close();
  await ns.stop();
});

Deno.test("basics - pull consumer", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  const sc = StringCodec();
  const s = await jsm.streams.create("foo", { subjects: ["foo"] });

  for (let i = 0; i < 1000; i++) {
    nc.publish("foo", sc.encode(`Hello${i + 1}`));
  }
  await nc.flush();

  assertEquals(s?.config?.name, "foo");
  const opts = defaultConsumer({ durable_name: "xxx" });
  const ci = await jsm.consumers.create("foo", opts);

  const consumer = PullConsumer.fromConsumerInfo(nc, ci);
  const done = (async () => {
    for await (const jm of consumer) {
      // console.log(
      //   `[${jm.seq} / ${consumer.getPending()}] ${
      //     sc.decode(jm.data)
      //   } - reply: ${jm.reply}`,
      // );
      if (consumer.getPending() <= 20) {
        jm.ack();
        await consumer.next(80);
      } else {
        jm.next();
      }
      if (jm.seq === 1000) {
        break;
      }
    }
  })();

  await done;
  await nc.close();
  await ns.stop();
});

// test("stream info", async (t) => {
//   const nc = await createConnection(t);
//   const jsm = new JSM(nc);
//
//   const name = nuid.next();
//   await jsm.streams.create(name, { subjects: [`${name}.*`] });
//
//   let info = await jsm.streams.info(name);
//   t.is(info.state.messages, 0);
//
//   nc.publish(`${name}.a`);
//   nc.publish(`${name}.b`);
//
//   info = await jsm.streams.info(name);
//   t.is(info.state.messages, 2);
//
//   nc.close();
// });
//
// test("purge", async (t) => {
//   const nc = await createConnection(t);
//   const jsm = new JSM(nc);
//
//   const name = nuid.next();
//   await jsm.streams.create(name, { subjects: [`${name}.*`] });
//   nc.publish(`${name}.a`);
//   nc.publish(`${name}.b`);
//
//   let info = await jsm.streams.info(name);
//   t.is(info.state.messages, 2);
//
//   const ok = await jsm.streams.purge(name);
//   t.true(ok.success);
//   t.is(ok.purged, 2);
//
//   nc.close();
// });

Deno.test("basics - create template", async () => {
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);
  await jsm.templates.create("FOOS", 100, { subjects: ["foo.>"] });

  nc.publish("foo.one");

  const list = await jsm.templates.list();
  assertEquals(list.streams, ["FOOS"]);

  const info = await jsm.templates.info("FOOS");
  assertEquals(info.streams, ["foo_one"]);

  await jsm.templates.delete("FOOS");
  await nc.close();
  await ns.stop();
});
