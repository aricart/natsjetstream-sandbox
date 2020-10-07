import {
  NatsConnection,
  JSONCodec,
} from "./nats-base-client.ts";
import {
  JSM,
  ConsumerAPI,
  defaultConsumer,
  ConsumerInfo,
  Consumers,
  ConsumerConfig,
  JetStreamError,
  JSResponse,
  Stream,
  StreamAPI,
  StreamConfig,
  defaultStream,
  StreamNames,
  Streams,
  ms,
  StreamMessage,
} from "./types.ts";

export function jsmClient(nc: NatsConnection): JSM {
  return new JsmImpl(nc);
}

export function pullSubject(stream: string, durable: string) {
  return `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`;
}

export class JsmImpl implements JSM {
  nc: NatsConnection;
  streams: StreamAPI;
  consumers: ConsumerAPIImpl;

  constructor(ns: NatsConnection) {
    this.nc = ns;
    this.streams = new StreamAPIImpl(this);
    this.consumers = new ConsumerAPIImpl(this);
  }

  async _request(subj: string, payload: any): Promise<object> {
    const c = JSONCodec();
    if (payload) {
      payload = c.encode(payload);
    }
    const m = await this.nc.request(subj, payload);
    const data = c.decode(m.data);

    const r = data as JSResponse;
    if (r.error) {
      throw new JetStreamError(r.error.code, r.error.description);
    }
    return data;
  }
}

export class ConsumerAPIImpl implements ConsumerAPI {
  jsm: JsmImpl;
  constructor(jsm: JsmImpl) {
    this.jsm = jsm;
  }

  _request(
    verb: string,
    stream: string = "",
    durable = "",
    payload: object = {},
  ): Promise<any> {
    if (!verb) {
      return Promise.reject(new Error("verb is required"));
    }
    if (verb !== "list" && !stream) {
      return Promise.reject(new Error("streams is required"));
    }
    if (verb === "create" && durable) {
      verb = "durable";
    }
    if (!durable) {
      durable = "";
    }

    const m = new Map<string, string>();
    m.set("create", `$JS.API.CONSUMER.CREATE.${stream}`);
    m.set("durable", `$JS.API.CONSUMER.DURABLE.CREATE.${stream}.${durable}`);
    m.set("delete", `$JS.API.CONSUMER.DELETE.${stream}.${durable}`);
    m.set("info", `$JS.API.CONSUMER.INFO.${stream}.${durable}`);
    m.set("list", `$JS.API.CONSUMER.LIST.${stream}`);
    m.set("names", `$JS.API.CONSUMER.NAMES.${stream}`);

    const subject = m.get(verb);
    if (!subject) {
      return Promise.reject(`bad verb ${verb}`);
    }

    return this.jsm._request(subject, payload);
  }

  async create(
    stream: string,
    opts: ConsumerConfig = {},
  ): Promise<ConsumerInfo> {
    opts = opts || {};
    const config = defaultConsumer(opts);
    const payload = { stream_name: stream, config: config };
    const ci = await this._request(
      config.durable_name ? "durable" : "create",
      stream,
      payload.config.durable_name,
      payload,
    );

    ci.created = ci.created ? ms(ci.created) : ci.created;
    return ci;
  }

  info(stream: string, durable = ""): Promise<ConsumerInfo> {
    return this._request("info", stream, durable);
  }

  list(stream: string): Promise<Consumers> {
    return this._request("list", stream);
  }
}

export class StreamAPIImpl implements StreamAPI {
  jsm: JsmImpl;
  constructor(jsm: JsmImpl) {
    this.jsm = jsm;
  }

  list(): Promise<Streams> {
    return this._request("list");
  }

  names(): Promise<StreamNames> {
    return this._request("names");
  }

  create(name: string, spec: StreamConfig = {}): Promise<Stream> {
    const payload = Object.assign({}, defaultStream, { name: name }, spec);
    return this._request("create", payload.name, payload);
  }

  async delete(name: string): Promise<boolean> {
    const response = await this._request("delete", name);
    return response.success;
  }

  async purge(name: string): Promise<boolean> {
    const response = await this._request("purge", name);
    return response.success;
  }

  async get(name: string, sequence: number): Promise<StreamMessage> {
    const d = await this._request("get", name, { seq: sequence });
    if (d.message?.data) {
      const te = new TextEncoder();
      d.message.data = te.encode(atob(d.message.data));
    }
    return d.message as StreamMessage;
  }

  info(name: string): Promise<Stream> {
    return this._request("info", name);
  }

  _request(verb: string, stream = "", payload?: any): Promise<any> {
    if (!verb) {
      return Promise.reject(new Error("verb is required"));
    }

    if (verb === "list") {
      stream = "";
    }

    const m = new Map<string, string>();
    m.set("create", `$JS.API.STREAM.CREATE.${stream}`);
    m.set("update", `$JS.API.STREAM.UPDATE.${stream}`);
    m.set("names", "$JS.API.STREAM.NAMES");
    m.set("list", "$JS.API.STREAM.LIST");
    m.set("info", `$JS.API.STREAM.INFO.${stream}`);
    m.set("delete", `$JS.API.STREAM.DELETE.${stream}`);
    m.set("purge", `$JS.API.STREAM.PURGE.${stream}`);
    m.set("get", `$JS.API.STREAM.MSG.GET.${stream}`);

    const subject = m.get(verb);
    if (!subject) {
      return Promise.reject(`bad verb ${verb}`);
    }

    return this.jsm._request(subject, payload);
  }
}
