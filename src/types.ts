import type {
  MsgHdrs,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/v1.0.0-7/nats-base-client/mod.ts";

export interface JSM {
  streams: StreamAPI;
  consumers: ConsumerAPI;
}

export interface ConsumerAPI {
  list(stream: string): Promise<Consumers>;
  create(stream: string, opts?: ConsumerConfig): Promise<ConsumerInfo>;
  info(stream: string, durable: string): Promise<ConsumerInfo>;
}

export interface StreamAPI {
  list(): Promise<Streams>;
  names(): Promise<StreamNames>;
  create(name: string, spec?: StreamConfig): Promise<Stream>;
  delete(name: string): Promise<boolean>;
  purge(name: string): Promise<boolean>;
  info(name: string): Promise<Stream>;
  get(name: string, sequence: number): Promise<any>;
}

export enum RetentionPolicy {
  Limits = "limits",
  Interest = "interest",
  WorkQueue = "workqueue",
}

export enum DiscardPolicy {
  Old = "old",
  New = "new",
}

export enum StorageType {
  File = "file",
  Memory = "memory",
}

export enum DeliverPolicy {
  All = "all",
  Last = "last",
  New = "new",
  ByStartSequence = "by_start_sequence",
  ByStartTime = "by_start_time",
}

export enum AckPolicy {
  None = "none",
  All = "all",
  Explicit = "explicit",
}

export enum ReplayPolicy {
  Instant = "instant",
  Original = "original",
}

export function defaultStream(opts: StreamConfig = {}): StreamConfig {
  return Object.assign({
    retention: RetentionPolicy.Limits,
    max_consumers: -1,
    max_msgs: -1,
    max_bytes: -1,
    max_age: 0,
    storage: StorageType.Memory,
    replicas: 1,
    no_ack: false,
  }, opts);
}

export interface StreamState {
  messages: number;
  bytes: number;
  first_seq: number;
  first_ts: number;
  last_seq: number;
  last_ts: string;
  consumer_count: number;
}

export interface StreamConfig {
  name?: string;
  subjects?: string[];
  retention?: RetentionPolicy;
  max_consumers?: number;
  max_msgs?: number;
  max_bytes?: number;
  discard?: DiscardPolicy;
  max_age?: number;
  max_msg_size?: number;
  storage?: StorageType;
  num_replicas?: number;
  duplicate_window?: number;
}

export interface JSResponse {
  type: string;
  error?: { description: string; code: number };
}

export interface StreamNames extends IterableResponse {
  streams: string[];
}

export interface StreamMessage {
  subject: string;
  seq: number;
  data: Uint8Array;
  time: string;
}

export interface IterableRequest {
  offset: number;
}

export interface Stream {
  config: StreamConfig;
  created: number;
  state: StreamState;
}

export interface IterableResponse extends IterableRequest {
  total: number;
  limit: number;
}

export interface Streams extends IterableResponse {
  streams: [Stream];
}

export function ns(millis: number) {
  return millis * 1000000;
}

export function ms(ns: number) {
  return ns / 1000000;
}

export interface ConsumerConfig {
  durable_name?: string;
  deliver_subject?: string;
  deliver_policy?: DeliverPolicy;
  opt_start_seq?: number;
  opt_start_time?: number;
  ack_policy?: AckPolicy;
  ack_wait?: number;
  max_deliver?: number;
  filter_subject?: string;
  replay_policy?: ReplayPolicy;
  sample_freq?: string;
  rate_limit_bps?: number;
}

export function defaultConsumer(opts: ConsumerConfig = {}): ConsumerConfig {
  return Object.assign({
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: ns(30 * 1000),
    replay_policy: ReplayPolicy.Instant,
  }, opts);
}

export interface Consumers extends IterableResponse {
  consumers: ConsumerInfo[];
}

export interface SequencePair {
  consumer_seq: number;
  stream_seq: number;
}

export interface ConsumerInfo {
  stream_name: string;
  name: string;
  config: ConsumerConfig;
  delivered: SequencePair;
  ack_floor: SequencePair;
  num_pending: number;
  num_redelivered: number;
  created: number;
}

export class JetStreamError extends Error {
  name: string;
  message: string;
  code: number;

  /**
   * @param {String} code
   * @param {String} message
   * @constructor
   *
   * @api private
   */
  constructor(code: number, message: string) {
    super(message);
    this.name = "JetStreamError";
    this.code = code;
    this.message = message;
  }
}

export interface DeliveryInfo {
  stream: string;
  consumer: string;
  rcount: number;
  sseq: number;
  dseq: number;
  ts: number;
}

export interface JsMsg {
  subject: string;
  sid: number;
  data: Uint8Array;
  headers?: MsgHdrs;
  reply: string;
  redelivered: boolean;
  info: DeliveryInfo;
  seq: number;

  ack(): void;
  nak(): void;
  working(): void;
  next(): void;
  ignore(): void;
}
