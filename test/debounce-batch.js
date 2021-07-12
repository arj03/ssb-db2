const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const validate = require('ssb-validate')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-debounce-batch'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .call(null, {
    keys,
    path: dir,
  })

test('add many times', async (t) => {
  let state = validate.initial()
  const keys1 = ssbKeys.generate()
  const keys2 = ssbKeys.generate()

  // 1..99, inclusive
  for (let i = 1; i <= 99; ++i) {
    if (i % 2 === 0) {
      state = validate.appendNew(
        state,
        null,
        keys1,
        { type: 'post', text: 'a' + i },
        Date.now()
      )
    } else {
      state = validate.appendNew(
        state,
        null,
        keys2,
        { type: 'post', text: 'b' + i },
        Date.now()
      )
    }
  }

  await Promise.all(state.queue.map((kvt) => pify(sbot.add)(kvt.value)))
  t.pass('added messages by two authors')

  await pify(setTimeout)(1000)

  const msgs = await new Promise((resolve, reject) => {
    sbot.db.query(
      sbot.db.operators.toCallback((err, msgs) => {
        if (err) reject(err)
        else resolve(msgs)
      })
    )
  })

  t.equals(msgs.length, 99, 'there are 99 messages')
  const msgs1 = msgs.filter((msg) => msg.value.author === keys1.id)
  const msgs2 = msgs.filter((msg) => msg.value.author === keys2.id)
  t.equals(msgs1.length, 49, 'there are 49 messages by author1')
  t.equals(msgs2.length, 50, 'there are 50 messages by author2')

  state = validate.appendNew(
    state,
    null,
    keys1,
    { type: 'post', text: 'a' + 100 },
    Date.now()
  )
  const finalKVT = state.queue[state.queue.length - 1]
  const added = await pify(sbot.add)(finalKVT.value)
  t.deepEquals(added.value, finalKVT.value)

  await pify(sbot.close)(true)
  t.end()
})
