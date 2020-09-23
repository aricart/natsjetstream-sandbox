const NATS = require("nats");
const JSM = require("./jsm").JSM;
const opts = require("./jsm").opts;

const nc = NATS.connect();
nc.on("err", (err) => {
  console.log(err);
});

nc.on("connect", async () => {
  console.debug("connected");

  const jsm = new JSM(nc);
  if (await jsm.streams.list()) {
    const ok = await jsm.streams.delete("worker");
    console.debug("delete worker", ok);
  }

  let ok = await jsm.streams.create(
    "worker",
    { subjects: ["worker.*"], retention: opts.stream.RetentionPolicy.Limits },
  );
  console.debug("create worker streams", ok);

  let info = await jsm.streams.info("worker");
  console.debug("info", "worker has", info.state.messages);

  await jsm.streams.purge("worker");

  // pull consumers
  const pullid = await jsm.consumers.create(
    "worker",
    { durable_name: "worker" },
  );
  console.log("created pull consumers", pullid);
  info = await jsm.consumers.info("worker", "worker");
  console.debug("info", info.name, "created");

  // const subj = jsm.consumers.pullSubject('worker', 'worker')
  // nc.subscribe("wrk", (err, msg) => {
  //   if (err) {
  //     return
  //   }
  //   console.log('> pull consumers:', msg.subject, msg.data)
  //   jsm.consumers.next(msg)
  // })
  // nc.publishRequest(subj, 'wrk')

  setInterval(() => {
    const subj = jsm.consumers.pullSubject("worker", "worker");
    nc.request(
      subj,
      (err, msg) => {
        if (err) {
          return;
        }
        msg.respond();
        console.log("> pull consumers:", msg.subject, msg.data);
      },
      undefined,
      { timeout: 500, noMuxRequests: true },
    );
  }, 1000);

  nc.publish("worker.entered", "hi");
  nc.publish("worker.exited", "bye");

  console.log("streams", await jsm.streams.list());
  console.log("worker consumers", await jsm.consumers.list("worker"));
});
