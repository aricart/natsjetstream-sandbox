import { Lock, NatsServer, } from 'https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/mod.ts'
import { connect, createInbox, } from 'https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts'

import { jsmClient, StorageType } from '../src/mod.ts'
import { ns as nanos } from '../src/types.ts'

const jsopts = {
  // debug: true,
  // trace: true,
  jetstream: {
    max_file_store: 1024 * 1024 * 1024,
    max_memory_store: 1024 * 1024 * 1024,
    store_dir: "/tmp",
  },
};

Deno.test("load", async () => {
  const ns = await NatsServer.start(jsopts);
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create("data", { subjects: ["data"], storage: StorageType.File });
  await jsm.consumers.create(
    "data",
    {
      durable_name: "mydurable",
      ack_wait: nanos(1000),
    },
  );

  const M = 2500;
  const lock = Lock(2500);

  const inbox = createInbox();
  const sub = nc.subscribe(inbox, {
    callback: () => {
      lock.unlock();
    },
  });

  const N = 2048;
  const payload = new Uint8Array(N);
  for (let i = 0; i < N; i++) {
    payload[i] = "a".charCodeAt(0) + (i % 26);
  }

  const start = Date.now();
  for (let i = 0; i < M; i++) {
    nc.publish("data", payload, { reply: inbox });
  }

  await lock;
  const time = Date.now() - start;
  console.log("jetstream time: ", time);

  const info = await jsm.streams.info("data");
  console.log(info);

  await nc.flush();
  await nc.close();
  await ns.stop();
});
