const NATS = require('nats')

const nc = NATS.connect()
nc.on('err', (err) => {
  console.log(err)
})



nc.on('connect', async () => {
  function *feeder() {
    while(true) {
      yield
      const v = yield
      yield v
    }
  }

  const j = feeder()

  console.debug('connected')

  async function *msgs() {
    while(true) {
      yield *feeder()
    }
  }

  const i = msgs()
  i.next()

  nc.subscribe('foo', (err, msg) => {
    console.log('got one')
    j.next()
    j.next({err, msg})
  })

  for await (let v of i) {
    console.log('>', v)
  }
})


// function foo(x, y) {
//   ajax(`http://someurl.1/?x=${x}&y=${y}`,
//     function(err, data) {
//       if (err) {
//         it.throw(err)
//       } else {
//         it.next(data)
//       }
//     })
//
//   function *main() {
//     try {
//       let text = yield foo(11,31);
//       console.log(text)
//     } catch(err) {
//       console.error(err)
//     }
//   }
//
//   var it = main();
//   it.next()
// }


// function *foo() {
//   var y = (yield)
//   return y
// }
//
// var it = foo(6)
// it.next();
// var res = it.next(7)
//
// console.log(res.value)
