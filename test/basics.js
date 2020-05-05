const test = require('ava')
const { startServer, stopServer } = require('./helpers/nats_server_control')
const NATS = require('nats')
const Payload = require('nats').Payload
const {JSM} = require('../src/jsm')
const nuid = require('nuid')

test.beforeEach(async (t) => {
  const server = await startServer(t)
  t.context = { server: server }
})

test.after.always((t) => {
  stopServer(t.context.server)
})

function createConnection(t, url) {
  return new Promise((resolve, reject) => {
    if (!url) {
      url = t.context.server.nats
    }
    const nc = NATS.connect(url, { payload: Payload.Binary })
    nc.on('error', (err) => {
      reject(err)
    })

    nc.on('connect', () => {
      resolve(nc)
    })
  })
}

test('create a stream', async (t) => {
  const nc = await createConnection(t)
  const jsm = new JSM(nc)
  let resp = await jsm.streams.list()
  t.is(resp.streams, null)

  const name = nuid.next()

  const v = await jsm.streams.create(name, {subjects: [`${name}.*`]})
  t.truthy(v.config)
  t.truthy(v.state)
  t.truthy(v.created)

  resp = await jsm.streams.list()
  t.is(resp.streams.length, 1)

  nc.close()
})

test('delete a stream', async (t) => {
  const nc = await createConnection(t)
  const jsm = new JSM(nc)
  let resp = await jsm.streams.list()
  t.is(resp.streams, null)

  const name = nuid.next()
  await jsm.streams.create(name, {subjects: [`${name}.*`]})

  resp = await jsm.streams.list()
  t.is(resp.streams.length, 1)

  const deleted = await jsm.streams.delete(name)
  t.true(deleted.success)

  resp = await jsm.streams.list()
  t.is(resp.streams, null)

  nc.close()
})

test('stream names', async (t) => {
  const nc = await createConnection(t)
  const jsm = new JSM(nc)

  const name = nuid.next()
  await jsm.streams.create(name, {subjects: [`${name}.*`]})

  let names = await jsm.streams.names()
  t.is(names.length, 1)
  t.is(names[0], name)

  nc.close()
})

test('stream info', async (t) => {
  const nc = await createConnection(t)
  const jsm = new JSM(nc)

  const name = nuid.next()
  await jsm.streams.create(name, {subjects: [`${name}.*`]})

  let info = await jsm.streams.info(name)
  t.is(info.state.messages, 0)

  nc.publish(`${name}.a`)
  nc.publish(`${name}.b`)

  info = await jsm.streams.info(name)
  t.is(info.state.messages, 2)

  nc.close()
})

test('purge', async (t) => {
  const nc = await createConnection(t)
  const jsm = new JSM(nc)

  const name = nuid.next()
  await jsm.streams.create(name, {subjects: [`${name}.*`]})
  nc.publish(`${name}.a`)
  nc.publish(`${name}.b`)

  let info = await jsm.streams.info(name)
  t.is(info.state.messages, 2)

  const ok = await jsm.streams.purge(name)
  t.true(ok.success)
  t.is(ok.purged, 2)


  nc.close()
})
