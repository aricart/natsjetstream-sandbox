import {
  nuid,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/internal_mod.ts";

import { join } from "https://deno.land/std@0.78.0/path/mod.ts";

export const jsopts = {
  // debug: true,
  // trace: true,
  jetstream: {
    max_file_store: 1024 * 1024,
    max_memory_store: 1024 * 1024,
    store_dir: "/tmp",
  },
};

export function serverOpts(opts = {}, randomStoreDir = true): any {
  const conf = Object.assign(opts, jsopts)
  if(randomStoreDir) {
    conf.jetstream.store_dir = join("/tmp", "jetstream", nuid.next())
  }
  Deno.mkdirSync(conf.jetstream.store_dir, {recursive: true})

  return jsopts;
}
