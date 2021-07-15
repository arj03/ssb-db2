const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const bendy = require('ssb-bendy-butt')
const timestamp = require('monotonic-timestamp')

function readyDir(dir) {
  rimraf.sync(dir)
  mkdirp.sync(dir)
  return dir
}

const dir = readyDir('/tmp/ssb-db2-box2-tribes')
const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
 .use(require('../')).call(null, {
   keys,
   path: dir,
   db2: {
     alwaysbox2: true
   }
 })
const db = sbot.db

const db1Dir = readyDir('/tmp/ssb-db2-box2-tribes-db1')
const db1Keys = ssbKeys.loadOrCreateSync(path.join(db1Dir, 'secret'))

const db1Sbot = SecretStack({ caps })
  .use(require('ssb-db'))
  .use(require('ssb-backlinks'))
  .use(require('ssb-query'))
  .use(require('ssb-tribes'))
  .call(null, {
    keys: db1Keys,
    path: db1Dir,
  })

test('box2 message can be read with tribes', (t) => {
  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )

  db.addBox2DMKey(testkey)

  let content = { type: 'post', text: 'super secret', recps: [keys.id, db1Keys.id] }
  
  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')

      db1Sbot.add(privateMsg.value, (err) => {
        db1Sbot.get({ id: privateMsg.key, private: true }, (err, db1Msg) => {
          t.equal(db1Msg.content.text, 'super secret')
          t.end()
        })
      })
    })
  })
})

test('second box2 message can be read with tribes', (t) => {
  let content = { type: 'post', text: 'super secret 2', recps: [keys.id, db1Keys.id] }
  
  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret 2')

      db1Sbot.add(privateMsg.value, (err) => {
        db1Sbot.get({ id: privateMsg.key, private: true }, (err, db1Msg) => {
          t.equal(db1Msg.content.text, 'super secret 2')
          t.end()
        })
      })
    })
  })
})

test('we can decrypt messages created with tribes', (t) => {
  let content = { type: 'post', text: 'super secret 3', recps: [keys.id, db1Keys.id] }
  
  db1Sbot.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.add(privateMsg.value, (err) => {
      db.get(privateMsg.key, (err, db2Msg) => {
        t.equal(db2Msg.content.text, 'super secret 3')
        t.end()
      })
    })
  })
})

test('we can decrypt messages created with tribes 2', (t) => {
  let content = { type: 'post', text: 'super secret 4', recps: [keys.id, db1Keys.id] }
  
  db1Sbot.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.add(privateMsg.value, (err) => {
      db.get(privateMsg.key, (err, db2Msg) => {
        t.equal(db2Msg.content.text, 'super secret 4')
        sbot.close(() => db1Sbot.close(t.end))
      })
    })
  })
})
