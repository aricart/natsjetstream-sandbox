import type {
  Msg,
  MsgHdrs,
  NatsConnection,
  Subscription,
  SubscriptionOptions,
} from "./nats-base-client.ts";
import { createInbox, Empty, QueuedIterator, TE } from "./nats-base-client.ts";
import type { ConsumerInfo, DeliveryInfo, JsMsg } from "./types.ts";
import { pullSubject } from "./jsm.ts";

const ACK = Uint8Array.of(43, 65, 67, 75);
const NAK = Uint8Array.of(45, 78, 65, 75);
const WPI = Uint8Array.of(43, 87, 80, 73);
const NXT = Uint8Array.of(43, 78, 88, 84);
const TERM = Uint8Array.of(43, 84, 69, 82, 77);

export class JsMsgImpl implements JsMsg {
  msg: Msg;
  sub: Subscription;
  di?: DeliveryInfo;

  constructor(msg: Msg, subscriber: Subscriber) {
    this.msg = msg;
    this.sub = subscriber.sub;
  }

  get subject(): string {
    return this.msg.subject;
  }

  get sid(): number {
    return this.msg.sid;
  }

  get data(): Uint8Array {
    return this.msg.data;
  }

  get headers(): MsgHdrs | undefined {
    return this.msg.headers;
  }

  get info(): DeliveryInfo {
    // "$JS.ACK.<stream>.<consumer>.<redelivery_count><streamSeq><deliverySequence>.<timestamp>"

    if (!this.di) {
      const tokens = this.reply.split(".");
      const di = {} as DeliveryInfo;
      di.stream = tokens[2];
      di.consumer = tokens[3];
      di.rcount = parseInt(tokens[4], 10);
      di.sseq = parseInt(tokens[5], 10);
      di.dseq = parseInt(tokens[6], 10);
      di.ts = parseInt(tokens[7], 10) / 1000000;
      di.pending = parseInt(tokens[8], 10);
      this.di = di;
    }
    return this.di;
  }

  get redelivered(): boolean {
    return this.info.rcount > 1;
  }

  get reply(): string {
    return this.msg.reply!;
  }

  get seq(): number {
    return this.info.sseq;
  }

  ack() {
    this.msg.respond(ACK);
  }

  nak() {
    this.msg.respond(NAK);
  }

  working() {
    this.msg.respond(WPI);
  }

  next() {
    this.msg.respond(NXT, { reply: this.sub.getSubject() });
  }

  ignore() {
    this.msg.respond(TERM);
  }
}

export interface Subscriber {
  sub: Subscription;
}

export class PushConsumer extends QueuedIterator<JsMsg> implements Subscriber {
  nc: NatsConnection;
  sub!: Subscription;

  static fromConsumerInfo(
    nc: NatsConnection,
    info: ConsumerInfo,
  ): PushConsumer {
    return new PushConsumer(nc, info.config.deliver_subject || "");
  }

  constructor(nc: NatsConnection, deliverSubject: string) {
    super();
    if (!deliverSubject) throw new Error("deliverSubject is required");
    this.nc = nc;

    const opts = {} as SubscriptionOptions;
    opts.callback = (err, msg) => {
      err ? this.stop(err) : this.push(new JsMsgImpl(msg, this));
    };

    this.sub = this.nc.subscribe(deliverSubject, opts);
  }
}

export class PullConsumer extends QueuedIterator<JsMsg> implements Subscriber {
  nc: NatsConnection;
  pullSub: string;
  sub!: Subscription;

  static fromConsumerInfo(
    nc: NatsConnection,
    info: ConsumerInfo,
  ): PullConsumer {
    return new PullConsumer(
      nc,
      info.stream_name,
      info.config.durable_name || "",
    );
  }

  constructor(
    nc: NatsConnection,
    stream: string,
    durable: string,
  ) {
    super();
    if (!stream) throw new Error("stream is required");
    if (!durable) throw new Error("a durable consumer is required");

    this.pullSub = pullSubject(stream, durable);
    this.nc = nc;

    const opts = {} as SubscriptionOptions;
    opts.callback = (err, msg) => {
      if (msg.subject === this.sub.getSubject()) {
        return;
      }
      err ? this.stop(err) : this.push(new JsMsgImpl(msg, this));
    };
    this.sub = this.nc.subscribe(createInbox(), opts);
    this.next(20).then();
  }

  async next(n?: number): Promise<void> {
    const opts = { reply: this.sub.getSubject() };
    let payload = Empty;
    if (n) {
      payload = TE.encode(String(n));
    }
    this.nc.publish(this.pullSub, payload, opts);
    await this.nc.flush();
  }
}
