const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const bendy = require('ssb-bendy-butt')
const timestamp = require('monotonic-timestamp')

const dir = '/tmp/ssb-db2-private'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('publish encrypted message', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      t.end()
    })
  })
})

test('publish: auto encrypt message with recps', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }

  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      t.end()
    })
  })
})

test('publishAs classic', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }

  db.publishAs(keys, null, content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      t.end()
    })
  })
})

test('publishAs bendy butt', (t) => {
  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )

  db.addBox2DMKey(testkey)

  // fake some keys
  const mfKeys = ssbKeys.generate()
  mfKeys.public = mfKeys.id = mfKeys.id.replace(".ed25519", ".bbfeed-v1")
  const mainKeys = ssbKeys.generate()

  const content = {
    type: "metafeed/add",
    feedpurpose: "secret purpose",
    subfeed: mainKeys.id,
    metafeed: mfKeys.id,
    recps: [keys.id],
    tangles: {
      metafeed: {
        root: null,
        previous: null
      }
    }
  }

  db.publishAs(mfKeys, mainKeys, content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.true(privateMsg.value.content.endsWith(".box2"), 'box2 encoded')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.feedpurpose, 'secret purpose')
      sbot.close(t.end)
    })
  })
})

test('box2', (t) => {
  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )

  const dirBox2 = '/tmp/ssb-db2-private-box2'
  rimraf.sync(dirBox2)
  mkdirp.sync(dirBox2)

  const sbotBox2 = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys,
      path: dirBox2,
      db2: {
        alwaysbox2: true
      }
    })

  sbotBox2.db.addBox2DMKey(testkey)

  let content = { type: 'post', text: 'super secret', recps: [keys.id] }

  sbotBox2.db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.true(privateMsg.value.content.endsWith(".box2"), 'box2 encoded')
    sbotBox2.db.get(privateMsg.key, (err, msg) => {
      t.error(err, 'no err')
      t.equal(msg.content.text, 'super secret')

      // encrypt to another key

      const dirKeys2 = '/tmp/ssb-db2-private-box2-2'
      rimraf.sync(dirKeys2)
      mkdirp.sync(dirKeys2)

      const keys2 = ssbKeys.loadOrCreateSync(path.join(dirKeys2, 'secret'))

      const sbotKeys2 = SecretStack({ appKey: caps.shs })
        .use(require('../'))
        .call(null, {
          keys: keys2,
          path: dirKeys2,
          db2: {
            alwaysbox2: true
          }
        })

      let contentKeys2 = { type: 'post', text: 'keys2 secret', recps: [keys2.id] }

      sbotBox2.db.publish(contentKeys2, (err, privateKeys2Msg) => {
        sbotKeys2.db.add(privateMsg.value, (err) => {
          sbotKeys2.db.add(privateKeys2Msg.value, (err) => {
            t.error(err, 'no err')
            sbotKeys2.db.get(privateKeys2Msg.key, (err, msg) => {
              t.error(err, 'no err')
              t.equal(msg.content.text, 'keys2 secret')

              sbotKeys2.db.get(privateMsg.key, (err, msg) => {
                t.error(err, 'no err')
                t.true(privateMsg.value.content.endsWith(".box2"), 'box2 encoded')

                sbotBox2.close(() => sbotKeys2.close(t.end))
              })
            })
          })
        })
      })
    })
  })
})
