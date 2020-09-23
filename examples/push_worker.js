const NATS = require("nats");
const { JSM, Worker, opts } = require("./jsm");

const nc = NATS.connect();
nc.on("err", (err) => {
  console.log(err);
});

nc.on("connect", async () => {
  console.debug("connected");

  // cleanup the server
  const jsm = new JSM(nc);
  const streams = await jsm.streams.list();
  console.log(streams);
  if (streams.indexOf("jobs") !== -1) {
    await jsm.streams.delete("jobs");
  }

  // create a streams
  await jsm.streams.create("jobs", {
    subjects: ["job.*"],
    retention: opts.stream.RetentionPolicy.Limits,
  });

  // add some messages
  nc.publish("job.in", "a");
  nc.publish("job.in", "b");
  nc.publish("job.in", "c");

  let info = await jsm.streams.info("jobs");
  console.debug(`stream has ${info.state.messages} messages`);

  const w = new Worker(jsm, "jobsx", { durable_name: "w" });
  const msgs = await w.pushGenerator();
  for await (let { error, msg } of msgs) {
    if (error) {
      if (error.code !== NATS.ErrorCode.TIMEOUT_ERR) {
        console.log("!", error.code);
        msgs.return();
      }
      continue;
    }
    console.log(">", msg.subject, msg.data);
    w.ok(msg);
  }
});
