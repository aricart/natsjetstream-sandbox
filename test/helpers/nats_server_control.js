/*
 * Copyright 2018-2019 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

const { spawn } = require('child_process')
const net = require('net')
const path = require('path')
const fs = require('fs')
const os = require('os')
const { URL } = require('url')
const SERVER = (process.env.TRAVIS) ? 'nats-server/nats-server' : 'bin/nats-server'

function getPort (urlString) {
  const u = new URL(urlString)
  return parseInt(u.port, 10)
}

function startServer (t) {
  return new Promise((resolve, reject) => {
    const dn = fs.mkdtempSync(path.join(os.tmpdir(), '_jsm'))
    // t.log('test dir', dn)

    const sc = {
      ports_file_dir: dn,
      listen: '127.0.0.1:-1',
      jetstream: {
        max_memory_store: 1024 * 1024 * 100,
        max_file_store: 1,
        store_dir: path.join(dn, 'data')
      }
    }

    const conf = path.join(dn, 'server.conf')
    fs.writeFileSync(conf, jsonToNatsConf(sc))

    const server = spawn(SERVER, ['-c', conf, '-DV'])
    // server.stderr.on('data', function (data) {
    //     let lines = data.toString().split('\n');
    //     lines.forEach((m) => {
    //         t.log(m);
    //     });
    // });


    const start = Date.now()
    let wait = 0
    const maxWait = 5 * 1000 // 5 secs
    const delta = 250
    let socket
    let timer

    function resetSocket () {
      if (socket) {
        socket.removeAllListeners()
        socket.destroy()
        socket = null
      }
    }

    function finish (err) {
      resetSocket()
      if (timer) {
        clearInterval(timer)
        timer = null
      }
      if (err === undefined) {
        resolve(server)
        return
      }
      reject(err)
    }

    let count = 50
    new Promise((resolve, reject) => {
      const to = setInterval(() => {
        --count
        if (count === 0) {
          clearInterval(t)
          reject(new Error('Unable to find the pid'))
        }
        // @ts-ignore
        const portsFile = path.join(dn, `nats-server_${server.pid}.ports`)
        if (fs.existsSync(portsFile)) {
          const data = fs.readFileSync(portsFile).toString()
          const s = server
          const ports = JSON.parse(data)
          s.nats = ports.nats[0]
          s.port = getPort(server.nats)
          s.ports = ports
          clearInterval(to)
          resolve()
        }
      }, 150)
    }).then(() => {
      // Test for when socket is bound.
      timer = setInterval(() => {
        resetSocket()

        wait = Date.now() - start
        if (wait > maxWait) {
          finish(new Error(`Can't connect to server on port ${server.port}`))
        }

        // Try to connect to the correct port.
        socket = net.createConnection(server.port)

        // Success
        socket.on('connect', () => {
          if (server.pid === null) {
            // We connected but not to our server..
            finish(new Error(`Server already running on port: ${server.port}`))
          } else {
            finish()
          }
        })

        // Wait for next try..
        socket.on('error', (error) => {
          finish(new Error('Problem connecting to server on port: ' + server.port + ' (' + error + ')'))
        })
      }, delta)
    })
      .catch((err) => {
        reject(err)
      })

    // Other way to catch another server running.
    server.on('exit', (code, signal) => {
      if (code === 1) {
        finish(new Error('Server exited with bad code, already running? (' + code + ' / ' + signal + ')'))
      }
    })

    // Server does not exist..
    // @ts-ignore
    server.stderr.on('data', (data) => {
      if (/^execvp\(\)/.test(data.toString())) {
        if (timer) {
          clearInterval(timer)
        }
        finish(new Error('Can\'t find the ' + SERVER))
      }
    })
  })
}

function wait (server, done) {
  if (server.killed) {
    if (done) {
      done()
    }
  } else {
    setTimeout(function () {
      wait(server, done)
    }, 0)
  }
}

function stopServer (server, done) {
  if (server && !server.killed) {
    server.kill()
    wait(server, done)
  } else if (done) {
    done()
  }
}

function jsonToNatsConf (o, indent) {
  const pad = arguments[1] !== undefined ? arguments[1] + '  ' : ''
  const buf = []
  for (const k in o) {
    if (Object.hasOwnProperty.call(o, k)) {
      // @ts-ignore
      const v = o[k]
      if (Array.isArray(v)) {
        buf.push(pad + k + ' [')
        buf.push(jsonToNatsConf(v, pad))
        buf.push(pad + ' ]')
      } else if (typeof v === 'object') {
        // don't print a key if it is an array and it is an index
        const kn = Array.isArray(o) ? '' : k
        buf.push(pad + kn + ' {')
        buf.push(jsonToNatsConf(v, pad))
        buf.push(pad + ' }')
      } else {
        if (!Array.isArray(o)) {
          // @ts-ignore
          if (typeof v === 'string' && v.charAt(0) >= '0' && v.charAt(0) <= '9' && isNaN(v)) {
            buf.push(pad + k + ': "' + v + '"')
          } else {
            buf.push(pad + k + ': ' + v)
          }
        } else {
          buf.push(pad + v)
        }
      }
    }
  }
  return buf.join('\n')
}

exports.startServer = startServer
exports.stopServer = stopServer
