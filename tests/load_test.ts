import {
  Lock,
  NatsServer,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/tests/helpers/mod.ts";
import {
  connect,
  createInbox,
  StringCodec,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/src/mod.ts";

import { jsmClient, StorageType } from "../src/mod.ts";
import { ns as nanos } from "../src/types.ts";
import { serverOpts } from "./util.ts";

Deno.test("load", async () => {
  const sc = StringCodec();
  const ns = await NatsServer.start(serverOpts());
  const nc = await connect({ port: ns.port });
  const jsm = jsmClient(nc);

  await jsm.streams.create(
    "data",
    { subjects: ["data"], storage: StorageType.File },
  );
  await jsm.consumers.create(
    "data",
    {
      durable_name: "mydurable",
      ack_wait: nanos(1000),
      sample_freq: "100",
    },
  );

  const M = 2500;
  let lock = Lock(M);

  const inbox = createInbox();
  const sub = nc.subscribe(inbox, {
    callback: () => {
      lock.unlock();
    },
  })

  const N = 500;
  const payload = new Uint8Array(N);
  for (let i = 0; i < N; i++) {
    payload[i] = "a".charCodeAt(0) + (i % 26);
  }

  for (let i = 0; i < M; i++) {
    nc.publish("data", payload, { reply: inbox });
  }

  await lock;

  await jsm.streams.info("data");

  lock = Lock(M);
  const consInbox = createInbox();
  nc.subscribe(consInbox, {
    callback: (err, msg) => {
      if (msg.reply === "") {
        return;
      }
      msg.respond(sc.encode("+NXT"), { reply: consInbox });
      lock.unlock();
    },
  });

  nc.publish(
    "$JS.API.CONSUMER.MSG.NEXT.data.mydurable",
    undefined,
    { reply: consInbox },
  );

  await lock;

  await nc.flush();
  await nc.close();
  await ns.stop();
});
