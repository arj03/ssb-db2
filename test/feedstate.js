const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const validate = require('ssb-validate')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-feedstate'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/feedstate'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    sbot.getFeedState(keys.id, (err, state) => {
      t.error(err)
      t.equal(state.id, postMsg.key)
      t.equal(state.sequence, postMsg.value.sequence)
      sbot.close(t.end)
    })
  })
})
