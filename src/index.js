const NATS = require('nats')
const JSM = require('./jsm').JSM
const opts = require('./jsm').opts

const nc = NATS.connect()
nc.on('err', (err) => {
  console.log(err)
})

nc.on('connect', async () => {
  console.debug('connected')

  const jsm = new JSM(nc)
  if (await jsm.stream.list()) {
    let ok = await jsm.stream.delete('visitor')
    console.debug('delete visitor', ok)

    ok = await jsm.stream.delete('worker')
    console.debug('delete worker', ok)
  }

  let ok = await jsm.stream.create('visitor', { subjects: ['visitor.*'] })
  console.debug('create visitor stream', ok)

  let info = await jsm.stream.info('visitor')
  console.debug('info', 'visitor has', info.state.messages, 'and', info.state.consumer_count, 'consumers')

  ok = await jsm.stream.create('worker', { subjects: ['worker.*'], retention: opts.stream.RetentionPolicy.WorkQueue })
  console.debug('create worker stream', ok)

  info = await jsm.stream.info('worker')
  console.debug('info', 'worker has', info.state.messages)

  await jsm.stream.purge('visitor')
  await jsm.stream.purge('worker')

  ok = await jsm.consumer.create('visitor', { durable_name: 'me', deliver_subject: 'visitor' })
  console.log('create consumer', ok)

  info = await jsm.consumer.info('visitor', 'me')
  console.log('info', 'consumer', info.name, 'will deliver to', info.config.deliver_subject)

  nc.subscribe('visitor', (_, msg) => {
    console.log('> visitor consumer', msg.subject, msg.data)
    nc.publish(`worker.${msg.subject.split('.')[1]}`, msg.data)
    if (msg.reply) {
      msg.respond()
    }
  })

  nc.subscribe('ephemeral', (_, msg) => {
    console.log('> ephemeral consumer:', msg.subject, msg.data)
    if (msg.reply) {
      msg.respond()
    }
  })

  // ephemeral consumer
  const ephid = await jsm.consumer.create('visitor', { deliver_subject: 'ephemeral' })
  console.log('create ephemeral consumer', ephid)
  info = await jsm.consumer.info('visitor', ephid)
  console.log('info', 'consumer', info.name, 'will deliver to', info.config.deliver_subject)

  // pull consumer
  const pullid = await jsm.consumer.create('worker', { durable_name: 'worker' })
  console.log('created pull consumer', pullid)
  info = await jsm.consumer.info('worker', 'worker')
  console.debug('info', info.name, 'created')

  // const subj = jsm.consumer.pullSubject('worker', 'worker')
  // nc.subscribe("wrk", (err, msg) => {
  //   if (err) {
  //     return
  //   }
  //   console.log('> pull consumer:', msg.subject, msg.data)
  //   jsm.consumer.next(msg)
  // })
  // nc.publishRequest(subj, 'wrk')

  setInterval(() => {
    const subj = jsm.consumer.pullSubject('worker', 'worker')
    nc.request(subj, (err, msg) => {
      if (err) {
        return
      }
      console.log('> pull consumer:', msg.subject, msg.data)
    }, undefined, { timeout: 500, noMuxRequests: true })
  }, 1000)

  nc.publish('visitor.entered', 'hi')
  nc.publish('visitor.exited', 'bye')

  console.log('streams', await jsm.stream.list())
  console.log('visitor consumers', await jsm.consumer.list('visitor'))
  console.log('worker consumers', await jsm.consumer.list('worker'))
})
