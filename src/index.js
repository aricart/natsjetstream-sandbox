const NATS = require('nats')
const JSM = require('./jsm')

const nc = NATS.connect()
nc.on('err', (err) => {
  console.log(err)
})

nc.on('connect', async () => {
  console.log('connected')

  const jsm = new JSM.JSM(nc)
  const streams = await jsm.stream.list()
  console.log('list', streams)

  if (streams) {
    const ok = await jsm.stream.delete('visitor')
    console.log('delete', ok)
  }

  let ok = await jsm.stream.create({ name: 'visitor', subjects: ['visitor.*'] })
  console.log('create', ok)

  let info = await jsm.stream.info('visitor')
  console.log('info', info)

  ok = await jsm.stream.purge('visitor')
  console.log('purge', ok)

  ok = await jsm.consumer.create('visitor', 'me', {deliver_subject: 'v'})
  console.log('create consumer', ok)

  nc.publish('visitor.entered', 'hello')
  nc.publish('visitor.exited', 'bye')

  info = await jsm.stream.info('visitor')
  console.log('info', info)

  nc.subscribe('v', (err, msg) => {
    console.log(msg.subject, msg.data)
    if(msg.reply) {
      msg.respond('')
      console.log('responded', msg.reply)
    }
  })
})
