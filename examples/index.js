const NATS = require("nats");
const JSM = require("../src/jsmImpl.ts").JsmImpl;
const opts = require("../src/jsmImpl.ts").opts;

const nc = NATS.connect({ payload: NATS.Payload.Binary });
nc.on("err", (err) => {
  console.log(err);
});

nc.on("connect", async () => {
  console.debug("connected");

  const jsm = new JSM(nc);
  const names = await jsm.streams.names();
  if (names !== null && names.indexOf("visitor") !== -1) {
    let ok = await jsm.streams.delete("visitor");
    console.debug("delete visitor", ok);
  }

  let ok = await jsm.streams.create("visitor", { subjects: ["visitor.*"] });
  console.debug("create visitor streams", ok);

  let info = await jsm.streams.info("visitor");
  console.debug(
    "info",
    "visitor has",
    info.state.messages,
    "and",
    info.state.consumer_count,
    "consumers",
  );

  await jsm.streams.purge("visitor");

  ok = await jsm.consumers.create(
    "visitor",
    { durable_name: "me", deliver_subject: "visitor" },
  );
  console.log("create consumers", ok);

  info = await jsm.consumers.info("visitor", "me");
  console.log(
    "info",
    "consumers",
    info.name,
    "will deliver to",
    info.config.deliver_subject,
  );

  nc.subscribe("visitor", (_, msg) => {
    console.log("> visitor consumers", msg.subject, msg.data);
    nc.publish(`worker.${msg.subject.split(".")[1]}`, msg.data);
    if (msg.reply) {
      msg.respond("+OK");
    }
  });

  nc.publish("visitor.entered", "hi");
  nc.publish("visitor.exited", "bye");

  console.log("streams", await jsm.streams.list());
  console.log("visitor consumers", await jsm.consumers.list("visitor"));
});
