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

function ns (millis) {
  return millis * 1000000
}

const _consumerTemplate = Object.freeze({
  deliver_policy: DeliverPolicy.All,
  ack_policy: AckPolicy.Explicit,
  ack_wait: ns(30 * 1000),
  replay_policy: ReplayPolicy.Instant
})

class Consumer {
  constructor (jsm) {
    this.jsm = jsm
  }

  _request (verb, stream, durable, payload) {
    if (!verb) {
      return Promise.reject(new Error('verb is required'))
    }
    if (!stream) {
      return Promise.reject(new Error('stream name is required'))
    }
    if (verb !== 'CONSUMERS' && !durable) {
      return Promise.reject(new Error('stream name is required'))
    }
    let subj = ''
    switch (verb) {
      case 'CONSUMERS':
        subj = `$JS.STREAM.${stream}.CONSUMERS`
        break
      case 'EPHEMERAL':
        subj = '$JS.STREAM.%s.EPHEMERAL.CONSUMER.CREATE'
      default:
        subj = `$JS.STREAM.${stream}.CONSUMER.${durable}.${verb}`
    }

    return this.jsm._request(subj, payload ? JSON.stringify(payload) : '')
  }

  create (stream, durable, spec) {
    spec = spec || {}
    spec = _.extend(spec, _consumerTemplate, {durable_name: durable})
    const opts = { stream_name: stream, config: spec }
    console.info(opts)
    return this._request('CREATE', stream, durable, opts)
  }
}

class Stream {
  constructor (jsm) {
    this.jsm = jsm
  }

  _request (verb, stream, payload) {
    if (!verb) {
      return Promise.reject(new Error('verb is required'))
    }
    let subj = ''
    if (verb === 'LIST') {
      subj = `$JS.STREAM.${verb}`
    } else {
      if (!stream) {
        return Promise.reject(new Error('stream name is required'))
      }
      subj = `$JS.STREAM.${stream}.${verb}`
    }

    return this.jsm._request(subj, payload ? JSON.stringify(payload) : '')
  }

  create (spec) {
    const payload = _.extend(spec, _streamTemplate)
    return this._request('CREATE', spec.name, payload)
  }

  delete (name) {
    return this._request('DELETE', name)
  }

  purge (name) {
    return this._request('PURGE', name)
  }

  list () {
    return new Promise((resolve, reject) => {
      this._request('LIST')
        .then((v) => {
          return resolve(JSON.parse(v))
        })
        .catch((err) => {
          reject(err)
        })
    })
  }

  info (name) {
    return new Promise((resolve, reject) => {
      this._request('INFO', name)
        .then((v) => {
          return resolve(JSON.parse(v))
        })
        .catch((err) => {
          reject(err)
        })
    })
  }
}

class JSM {
  constructor (nc) {
    if (nc.options.payload !== NATS.Payload.String) {
      throw ('JSM requires string payloads')
    }
    this.nc = nc
    this.stream = new Stream(this)
    this.consumer = new Consumer(this)
  }

  _request (subj, payload) {
    return new Promise((resolve, reject) => {
      this.nc.request(subj, (err, msg) => {
        if (err) {
          reject(err)
          return
        }

        const jerr = this._isError(msg.data)
        if (jerr) {
          reject(new Error(jerr))
          return
        }
        return resolve(this._getData(msg.data))
      }, payload)
    })
  }

  _getData (m) {
    if (m === '+OK') {
      return true
    }
    if (m.indexOf('+OK \'') === 0) {
      return m.substring(6, m.length - 1)
    }
    return m
  }

  _isError (m) {
    if (m.indexOf('-ERR \'') === 0) {
      return m.substring(6, m.length - 1)
    }
  }
}

module.exports = {JSM: JSM}
