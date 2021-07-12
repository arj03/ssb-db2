const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const validate = require('ssb-validate')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-ebt'

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
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    sbot.getAtSequence([keys.id, 1], (err, msg) => {
      t.error(err)
      t.equal(msg.value.content.text, postMsg.value.content.text)
      t.end()
    })
  })
})

test('author sequence', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.onDrain('ebt', () => {
        db.getIndex('ebt').getMessageFromAuthorSequence(
          [keys.id, postMsg2.value.sequence],
          (err, msg) => {
            t.error(err, 'no err')
            t.equal(msg.value.content.text, post2.text, 'correct msg')

            t.end()
          }
        )
      })
    })
  })
})

test('vector clock', (t) => {
  sbot.getVectorClock((err, clock) => {
    t.error(err, 'no err')
    t.deepEquals(clock, { [keys.id]: 3 })

    t.end()
  })
})

test('Encrypted', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  let i = 0

  var remove = sbot.db.post((msg) => {
    if (i++ === 0) t.equal(msg.value.sequence, 3, 'we get existing')
    else {
      t.equal(msg.value.sequence, 4, 'post is called on publish')
      remove()
    }
  })

  db.publish(content, (err) => {
    t.error(err, 'no err')

    sbot.getAtSequence([keys.id, 4], (err, msg) => {
      t.error(err)
      t.equal(msg.value.content, content)
      t.end()
    })
  })
})

test('add', (t) => {
  let state = validate.initial()
  const keys2 = ssbKeys.generate()

  state = validate.appendNew(
    state,
    null,
    keys2,
    { type: 'post', text: 'testing sbot.add' },
    Date.now()
  )

  let i = 0

  var remove = sbot.db.post((msg) => {
    if (i++ === 0) t.equal(msg.value.author, keys.id, 'we get existing')
    else {
      t.equal(msg.value.author, keys2.id, 'post is called on add')
      remove()
    }
  })

  sbot.add(state.queue[0].value, (err, added) => {
    t.error(err)
    t.equal(added.value.content.text, 'testing sbot.add')
    t.end()
  })
})

test('teardown sbot', (t) => {
  sbot.close(t.end)
})
