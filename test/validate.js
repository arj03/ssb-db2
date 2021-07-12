const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const validate = require('ssb-validate')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-validate'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')
    t.end()
  })
})

test('Multiple', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const post2 = { type: 'post', text: 'Testing 2!' }

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')
      t.equal(postMsg2.value.content.text, post2.text, 'text correct')
      t.end()
    })
  })
})

test('Raw feed with unused type + ooo in batch', (t) => {
  let state = validate.initial()
  const keys = ssbKeys.generate()

  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test1' },
    Date.now()
  ) // ooo
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test2' },
    Date.now() + 1
  ) // missing
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test3' },
    Date.now() + 2
  ) // start
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'vote', vote: { link: '%something.sha256', value: 1 } },
    Date.now() + 3
  )
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test5' },
    Date.now() + 4
  )
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test6' },
    Date.now() + 5
  )

  const msgVals = state.queue.slice(2).map((msg) => msg.value)
  db.addOOOBatch(msgVals, (err) => {
    t.error(err, 'no err')

    db.addOOO(state.queue[0].value, (err, oooMsg) => {
      t.error(err, 'no err')
      t.equal(oooMsg.value.content.text, 'test1', 'text correct')

      t.end()
    })
  })
})

// we might get some messages from an earlier thread, and then get the
// latest 25 messages from the user
test('Add OOO with holes', (t) => {
  let state = validate.initial()
  const keys = ssbKeys.generate()

  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test1' },
    Date.now()
  ) // ooo
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test2' },
    Date.now() + 1
  ) // missing
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test3' },
    Date.now() + 2
  ) // start

  db.addOOO(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    db.addOOO(state.queue[2].value, (err, msg) => {
      t.error(err, 'no err')
      t.equal(msg.value.content.text, 'test3', 'text correct')
      t.end()
    })
  })
})

test('Add same message twice', (t) => {
  let state = validate.initial()
  const keys = ssbKeys.generate()

  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test1' },
    Date.now()
  )
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test2' },
    Date.now() + 1
  )

  db.add(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    db.add(state.queue[1].value, (err) => {
      t.error(err, 'no err')

      // validate makes sure we can't add the same message twice
      db.add(state.queue[1].value, (err) => {
        t.ok(err, 'Should fail to add')
        t.end()
      })
    })
  })
})

test('add fail case', (t) => {
  let state = validate.initial()
  const keys = ssbKeys.generate()

  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test1' },
    Date.now()
  )
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test2' },
    Date.now() + 1
  )
  state = validate.appendNew(
    state,
    null,
    keys,
    { type: 'post', text: 'test3' },
    Date.now() + 2
  )

  db.add(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    db.add(state.queue[2].value, (err) => {
      t.ok(err, 'Should fail to add')

      sbot.close(t.end)
    })
  })
})
