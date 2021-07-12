// add a bunch of message, add index see that it indexes correctly

// add messages after the index has been created, also test indexSync

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const pull = require('pull-stream')

const dir = '/tmp/ssb-db2-base-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('drain', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      const status = db.getStatus().value
      t.equal(status.log, 0, 'log in sync')
      t.equal(status.indexes['base'], 0, 'index in sync')
      t.end()
    })
  })
})

test('get', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.get(postMsg.key, (err, msg) => {
        t.equal(msg.content.text, post.text, 'correct msg')

        t.end()
      })
    })
  })
})

test('get latest', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.getLatest(keys.id, (err, status) => {
        t.error(err, 'no err')
        t.equal(status.sequence, postMsg.value.sequence)
        t.true(status.offset > 100)

        t.end()
      })
    })
  })
})

test('get all latest', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      pull(
        db.getAllLatest(),
        pull.collect((err, all) => {
          t.error(err, 'no err')
          t.equals(all.length, 1)
          const { key, value } = all[0]
          t.equal(key, keys.id)
          t.equal(value.sequence, postMsg.value.sequence)
          t.true(value.offset > 100)

          sbot.close(t.end)
        })
      )
    })
  })
})
