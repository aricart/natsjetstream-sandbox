const _ = require('underscore')
const NATS = require('nats')

const RetentionPolicy = Object.freeze({
  Limits: 'limits',
  Interest: 'interest',
  WorkQueue: 'workqueue'
})

const StorageType = Object.freeze({
  File: 'file',
  Memory: 'memory'
})

const DeliverPolicy = Object.freeze({
  All: 'all',
  Last: 'last',
  New: 'new',
  ByStartSequence: 'by_start_sequence',
  ByStartTime: 'by_start_time'
})

const AckPolicy = Object.freeze({
  None: 'none',
  All: 'all',
  Explicit: 'explicit'
})

const ReplayPolicy = Object.freeze({
  Instant: 'instant',
  Original: 'original'
})

const _streamTemplate = Object.freeze({
  retention: RetentionPolicy.Limits,
  max_consumers: -1,
  max_msgs: -1,
  max_bytes: -1,
  max_age: 0,
  storage: StorageType.Memory,
  replicas: 1,
  no_ack: false
})

// const streamTemplates = Object.freeze({
//   create: "$JS.API.STREAM.TEMPLATE.CREATE.{name}",
//   info: "$JS.API.STREAM.TEMPLATE.INFO.{name}",
//   delete: "$JS.API.STREAM.TEMPLATE.DELETE.{name}",
//   names: "$JS.API.STREAM.TEMPLATE.NAMES",
// })

function ns (millis) {
  return millis * 1000000
}

const _consumerTemplate = Object.freeze({
  deliver_policy: DeliverPolicy.All,
  ack_policy: AckPolicy.Explicit,
  ack_wait: ns(30 * 1000),
  replay_policy: ReplayPolicy.Instant
})

class Acker {
  ok (msg) {
    msg.respond('+OK')
  }

  nak (msg) {
    msg.respond('-NAK')
  }

  progress (msg) {
    msg.respond('+WPI')
  }

  next (msg) {
    msg.respond('+NXT')
  }
}

class Worker extends Acker {
  constructor (jsm, stream, opts) {
    super()
    this.jsm = jsm
    this.stream = stream
    this.opts = opts
  }

  pullGenerator () {
    const { jsm, stream, opts } = this
    if (!opts.durable_name) {
      return Promise.reject(new Error('`durable_name` option is required'))
    }
    opts.timeout = opts.timeout || 1000
    opts.noMuxRequests = true

    return new Promise((resolve, reject) => {
      jsm.consumers.info(stream, opts.durable_name)
        .catch(() => {
          // we have no consumer, create it
          return jsm.consumers.create(stream, opts)
        })
        .then(() => {
          const fn = async function * () {
            const subj = jsm.consumers.pullSubject(stream, opts.durable_name)
            while (true) {
              const t = await new Promise((resolve) => {
                jsm.nc.request(subj, (err, msg) => {
                  if (err) {
                    resolve({ error: err, msg: null })
                  }
                  resolve({ error: null, msg: msg })
                }, '', opts)
              })
              yield t
            }
          }
          resolve(fn())
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

class ConsumerAPI {
  constructor (jsm) {
    this.jsm = jsm
  }

  _request (verb, stream, durable, payload) {
    if (!verb) {
      return Promise.reject(new Error('verb is required'))
    }
    if (verb !== 'list' && !stream) {
      return Promise.reject(new Error('streams is required'))
    }
    if (verb === 'create' && durable) {
      verb = 'durable'
    }
    if (!durable) {
      durable = ''
    }
    const subjects = {
      create: `$JS.API.CONSUMER.CREATE.${stream}`,
      durable: `$JS.API.CONSUMER.DURABLE.CREATE.${stream}.${durable}`,
      delete: `$JS.API.CONSUMER.DELETE.${stream}.${durable}`,
      info: `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
      list: `$JS.API.CONSUMER.LIST.${stream}`,
      names: `$JS.API.CONSUMER.NAMES.${stream}`
    }

    return this.jsm._request(subjects[verb], payload)
  }

  create (stream, opts) {
    opts = opts || {}
    const config = _.extend({}, _consumerTemplate, opts)
    const payload = { stream_name: stream, config: config }
    return this._request('durable', stream, payload.config.durable_name, payload)
  }

  info (stream, optDurable) {
    return new Promise((resolve, reject) => {
      this._request('info', stream, optDurable)
        .then((v) => {
          resolve(v)
        })
        .catch((err) => {
          reject(err)
        })
    })
  }

  pullSubject (stream, durable) {
    return `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`
  }

  list (stream) {
    return new Promise((resolve, reject) => {
      this._request('list', stream)
        .then((v) => {
          resolve(v)
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

class StreamAPI {
  constructor (jsm) {
    this.jsm = jsm
  }

  _request (verb, stream, payload) {
    if (!verb) {
      return Promise.reject(new Error('verb is required'))
    }

    if (verb === 'list') {
      stream = ''
    }

    const subjects = {
      create: `$JS.API.STREAM.CREATE.${stream}`,
      update: `$JS.API.STREAM.UPDATE.${stream}`,
      names: '$JS.API.STREAM.NAMES',
      list: '$JS.API.STREAM.LIST',
      info: `$JS.API.STREAM.INFO.${stream}`,
      delete: `$JS.API.STREAM.DELETE.${stream}`,
      purge: `$JS.API.STREAM.PURGE.${stream}`,
      get: `$JS.API.STREAM.GET.${stream}`
    }

    return this.jsm._request(subjects[verb], payload)
  }

  create (name, spec) {
    const payload = _.extend({}, _streamTemplate, { name: name }, spec)
    return this._request('create', payload.name, payload)
  }

  delete (name) {
    return this._request('delete', name)
  }

  purge (name) {
    return this._request('purge', name)
  }

  list () {
    return new Promise((resolve, reject) => {
      this._request('list')
        .then((v) => {
          resolve(v)
        })
        .catch((err) => {
          reject(err)
        })
    })
  }

  names () {
    return new Promise((resolve, reject) => {
      this._request('names')
        .then((v) => {
          return resolve(v.streams)
        })
        .catch((err) => {
          reject(err)
        })
    })
  }

  info (name) {
    return new Promise((resolve, reject) => {
      this._request('info', name)
        .then((v) => {
          resolve(v)
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

class JSM {
  constructor (nc) {
    if (nc.options.payload !== NATS.Payload.Binary) {
      throw new Error('JSM requires Binary payloads')
    }
    this.nc = nc
    this.streams = new StreamAPI(this)
    this.consumers = new ConsumerAPI(this)
  }

  _request (subj, payload) {
    return new Promise((resolve, reject) => {
      if (payload) {
        payload = JSON.stringify(payload)
      }

      this.nc.request(subj, (err, msg) => {
        if (err) {
          reject(err)
          return
        }
        msg.data = JSON.parse(msg.data)
        if (msg.data.error) {
          console.error('request', subj, 'failed', msg.data.error)
          reject(new Error(msg.data.error.description))
        }
        resolve(msg.data)
      }, payload)
    })
  }
}

module.exports = {
  JSM: JSM,
  Worker: Worker,
  opts: {
    stream: {
      RetentionPolicy: RetentionPolicy,
      StorageType: StorageType
    },
    consumer: {
      DeliverPolicy: DeliverPolicy,
      AckPolicy: AckPolicy,
      ReplayPolicy: ReplayPolicy
    }
  }
}
