const test = require('tape')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const generateFixture = require('ssb-fixtures')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const ssbKeys = require('ssb-keys')
const multicb = require('multicb')
const pull = require('pull-stream')
const asyncFilter = require('pull-async-filter')
const validate = require('ssb-validate')
const fromEvent = require('pull-stream-util/from-event')
const DeferredPromise = require('p-defer')
const trammel = require('trammel')
const sleep = require('util').promisify(setTimeout)
const {
  and,
  where,
  type,
  author,
  key,
  votesFor,
  isPublic,
  isRoot,
  hasRoot,
  startFrom,
  paginate,
  descending,
  toCallback,
} = require('../operators')

const dir = '/tmp/ssb-db2-benchmark'
const dirAdd = '/tmp/ssb-db2-benchmark-add'
const dirPrivate = '/tmp/ssb-db2-benchmark-private'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const db2Path = path.join(dir, 'db2')
const indexesPath = path.join(dir, 'db2', 'indexes')
const reportPath = path.join(dir, 'benchmark.md')

rimraf.sync(dirAdd)
rimraf.sync(dirPrivate)

const skipCreate = process.argv[2] === 'noCreate'

if (!skipCreate) {
  rimraf.sync(dir)
  mkdirp.sync(dir)

  const SEED = 'sloop'
  const MESSAGES = 100000
  const AUTHORS = 2000

  test('generate fixture with flumelog-offset', (t) => {
    generateFixture({
      outputDir: dir,
      seed: SEED,
      messages: MESSAGES,
      authors: AUTHORS,
      slim: true,
    }).then(() => {
      t.pass(`seed = ${SEED}`)
      t.pass(`messages = ${MESSAGES}`)
      t.pass(`authors = ${AUTHORS}`)
      t.true(fs.existsSync(oldLogPath), 'log.offset was created')
      fs.appendFileSync(reportPath, '## Benchmark results\n\n')
      fs.appendFileSync(reportPath, '| Part | Duration |\n|---|---|\n')
      t.end()
    })
  })
}

let maxRAMrss = 0
let maxRAMheap = 0
function updateMaxRAM() {
  const memUsage = process.memoryUsage()
  maxRAMrss = Math.max(maxRAMrss, memUsage.rss)
  maxRAMheap = Math.max(maxRAMheap, memUsage.heapUsed)
}

function toMB(bytes) {
  return (bytes / 1000 / 1000).toFixed(2)
}

function reportMem() {
  const rss = toMB(maxRAMrss)
  const heap = toMB(maxRAMheap)
  return `${rss} MB = ${heap} MB + etc`
}

let keys, keys2
test('setup', (t) => {
  keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  keys2 = ssbKeys.loadOrCreateSync(path.join(dirPrivate, 'secret'))
  t.end()
})

test('add a bunch of messages', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dirAdd })

  let state = validate.initial()
  for (var i = 0; i < 1000; ++i) {
    state = validate.appendNew(
      state,
      null,
      keys,
      { type: 'tick', count: i },
      Date.now()
    )
  }

  const messages = state.queue.map((x) => x.value)

  const ended = DeferredPromise()
  const start = Date.now()

  pull(
    pull.values(messages),
    asyncFilter(sbot.db.add),
    pull.collect((err) => {
      const duration = Date.now() - start

      if (err) t.fail(err)

      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| add 1000 elements | ${duration}ms |\n`)

      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

const randos = [
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id
]

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

test('private', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dirPrivate })

  const recps = [...randos, keys.id]

  let contents = []
  for (var i = 0; i < 1000; ++i)
    contents.push({ type: 'tick', count: i, recps: shuffle(recps) })

  const ended = DeferredPromise()
  const start = Date.now()

  pull(
    pull.values(contents),
    pull.asyncMap(sbot.db.publish),
    pull.collect((err, msgs) => {
      const duration = Date.now() - start

      if (err) t.fail(err)

      t.pass(`box duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| add 1000 private box1 elements | ${duration}ms |\n`)

      sbot.db.onDrain('base', () => {
        let startQuery = Date.now()

        sbot.db.query(
          where(author(sbot.id)),
          toCallback((err, results) => {
            const durationQuery = Date.now() - startQuery
            t.pass(`unbox first run duration: ${durationQuery}ms`)
            fs.appendFileSync(reportPath, `| unbox 1000 private box1 elements first run | ${durationQuery}ms |\n`)

            startQuery = Date.now()

            sbot.db.query(
              where(author(sbot.id)),
              toCallback((err, results) => {
                const durationQuery2 = Date.now() - startQuery
                t.pass(`unbox second run duration: ${durationQuery2}ms`)
                fs.appendFileSync(reportPath, `| unbox 1000 private box1 elements second run | ${durationQuery2}ms |\n`)

                sbot.close(() => ended.resolve())
              })
            )
          })
        )
      })
    })
  )

  await ended.promise
})

test('private box2', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: keys2,
      path: dirPrivate,
      db2: {
        alwaysbox2: true
      }
    })

  const recps = [...randos, keys2.id]

  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  sbot.db.addBox2DMKey(testkey)

  let contents = []
  for (var i = 0; i < 1000; ++i)
    contents.push({ type: 'tick', count: i, recps: shuffle(recps) })

  const ended = DeferredPromise()
  const start = Date.now()

  pull(
    pull.values(contents),
    pull.asyncMap(sbot.db.publish),
    pull.collect((err, msgs) => {
      const duration = Date.now() - start

      if (err) t.fail(err)

      t.pass(`box duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| add 1000 private box2 elements | ${duration}ms |\n`)

      sbot.db.onDrain('base', () => {
        let startQuery = Date.now()

        sbot.db.query(
          where(author(sbot.id)),
          toCallback((err, results) => {
            const durationQuery = Date.now() - startQuery
            t.pass(`unbox duration first run: ${durationQuery}ms`)
            fs.appendFileSync(reportPath, `| unbox 1000 private box2 elements first run | ${durationQuery}ms |\n`)

            startQuery = Date.now()

            sbot.db.query(
              where(author(sbot.id)),
              toCallback((err, results) => {
                const durationQuery2 = Date.now() - startQuery
                t.pass(`unbox duration second run: ${durationQuery2}ms`)
                fs.appendFileSync(reportPath, `| unbox 1000 private box2 elements first run | ${durationQuery2}ms |\n`)

                sbot.close(() => ended.resolve())
              })
            )
          })
        )
      })
    })
  )

  await ended.promise
})

test('migrate (+db1)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  while (true) {
    const { current, target } = sbot.progress().indexes
    if (current === target) break
    else await sleep(500)
  }
  t.pass('ssb-db has finished indexing')

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Migrate (+db1) | ${duration}ms |\n`)
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (alone)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Migrate (alone) | ${duration}ms |\n`)
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (+db1 +db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Migrate (+db1 +db2) | ${duration}ms |\n`)
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (+db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Migrate (+db2) | ${duration}ms |\n`)
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate continuation (+db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  let sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress > 0.9),
    pull.take(1),
    pull.drain(async () => {
      sbot.db2migrate.stop()
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      await new Promise((resolve) => sbot.close(resolve))
      await sleep(500) // some silence
      t.pass('migrated 90%, will reset sbot')

      sbot = SecretStack({ appKey: caps.shs })
        .use(require('../'))
        .call(null, { keys, path: dir })

      global.gc()
      await sleep(500)
      updateMaxRAM() // will report later, just to make the report order pretty

      const start = Date.now()
      sbot.db2migrate.start()

      pull(
        fromEvent('ssb:db2:migrate:progress', sbot),
        pull.filter((progress) => progress === 1),
        pull.take(1),
        pull.drain(async () => {
          const duration = Date.now() - start
          t.pass(`duration: ${duration}ms`)
          fs.appendFileSync(
            reportPath,
            `| Migrate continuation (+db2) | ${duration}ms |\n`
          )
          await new Promise((resolve) => sbot.db.onDrain(resolve))
          sbot.close(() => ended.resolve())
        })
      )
    })
  )

  await ended.promise
})

test('Memory usage without indexes', (t) => {
  t.pass(`memory usage without indexes: ${reportMem()}`)
  fs.appendFileSync(
    reportPath,
    `| Memory usage without indexes | ${reportMem()} |\n`
  )
  t.end()
})

test('initial indexing', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  sbot.db.query(
    where(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      const duration = Date.now() - start
      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Initial indexing | ${duration}ms |\n`)
      updateMaxRAM()
      global.gc()
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('initial indexing maxcpu 86', async (t) => {
  rimraf.sync(indexesPath)
  t.pass('delete indexes folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir, db2: { maxCpu: 86 } })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  sbot.db.query(
    where(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      const duration = Date.now() - start
      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| Initial indexing maxCpu=86 | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      sbot.close(() => ended.resolve())
    })
  )

  await ended.promise
})

test('initial indexing compat', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  sbot.db.onDrain('base', () => {
    sbot.db.onDrain('ebt', () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| Initial indexing compat | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      sbot.close(() => ended.resolve())
    })
  })

  await ended.promise
})

test('Two indexes updating concurrently', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const done = multicb({ pluck: 1 })
  const start = Date.now()

  sbot.db.query(where(type('about')), toCallback(done()))
  sbot.db.query(where(and(type('about'), isPublic())), toCallback(done()))

  done((err) => {
    if (err) t.fail(err)
    const duration = Date.now() - start
    t.pass(`duration: ${duration}ms`)
    fs.appendFileSync(
      reportPath,
      `| Two indexes updating concurrently | ${duration}ms |\n`
    )
    updateMaxRAM()
    global.gc()
    sbot.close(() => ended.resolve())
  })

  await ended.promise
})

test.skip('ssb-threads and ssb-friends', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('ssb-friends'))
    .use(require('ssb-threads'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  pull(
    sbot.threads.publicSummary({ allowlist: ['post', 'contact'] }),
    pull.take(1),
    pull.collect(async (err, threads) => {
      const duration = Date.now() - start
      if (err) t.fail(err)
      if (threads.length !== 1) t.fail('missing results')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| ssb-threads and ssb-friends | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      await sleep(2000) // wait for jitdb indexes to save to disk
      sbot.close(() => {
        ended.resolve()
      })
    })
  )

  await ended.promise
})

test.skip('ssb-threads and ssb-friends again', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('ssb-friends'))
    .use(require('ssb-threads'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  pull(
    sbot.threads.publicSummary({ allowlist: ['post', 'contact'] }),
    pull.take(1),
    pull.collect(async (err, threads) => {
      const duration = Date.now() - start
      if (err) t.fail(err)
      if (threads.length !== 1) t.fail('missing results')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| ssb-threads and ssb-friends again | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      await sleep(2000) // wait for jitdb indexes to save to disk
      sbot.close(() => {
        ended.resolve()
      })
    })
  )

  await ended.promise
})

const KEY1 = '%Xwdpu9gRe8wl4i0ssKyU24oGYKXW75hjE5VCbEB9bmM=.sha25' // root post
const KEY2 = '%EpzOw6sOBb4RGtofVD43GnfImoiw6NzEEsraHsNXF1g=.sha25' // contact
const KEY3 = '%55wBq68+p45q7/OuPgL+TC07Ifx8ihEW93u/EZaYv6c=.sha256' // another post
const AUTHOR1 = '@ZngOKXHjrvG+cy7Gjx5pSFunUqcePfmDQQxoUlHFUdU=.ed2551'
const AUTHOR2 = '@58u/J9+5bOXeYRDCYQ9cJ7kklghIpQFPBYxlhKq1/qs=.ed2551'
const REBOOT = 'reboot'

const queries = {
  'key one initial': [where(key(KEY1))],

  'key two': [where(key(KEY2))],

  'key one again': [where(key(KEY1))],

  [REBOOT]: true,

  'reboot and key one again': [where(key(KEY1))],

  'latest root posts': [
    where(and(type('post'), isRoot(), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'latest posts': [
    where(and(type('post'), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'votes one initial': [where(votesFor(KEY1))],

  'votes again': [where(votesFor(KEY3))],

  hasRoot: [where(hasRoot(KEY1))],

  'hasRoot again': [where(hasRoot(KEY3))],

  'author one posts': [
    where(and(type('post'), author(AUTHOR1), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'author two posts': [
    where(and(type('post'), author(AUTHOR2), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts': [
    where(and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts again': [
    where(and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],
}

let sbot
test('setup', (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })
  t.end()
})

for (const title in queries) {
  test(title, (t) => {
    if (title === REBOOT) {
      sbot.close(() => {
        sbot = SecretStack({ appKey: caps.shs })
          .use(require('../'))
          .call(null, { keys, path: dir })
        t.end()
      })
      return
    }

    const start = Date.now()

    sbot.db.query(
      ...queries[title],
      toCallback((err) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(reportPath, `| ${title} | ${duration}ms |\n`)
        updateMaxRAM()
        t.end()
      })
    )
  })
}

test('maximum RAM used', (t) => {
  t.pass(`maximum memory usage: ${reportMem()}`)
  fs.appendFileSync(reportPath, `| Maximum memory usage | ${reportMem()} |\n`)
  t.end()
})

test('Indexes folder size', (t) => {
  trammel(indexesPath).then((size) => {
    t.pass(`indexes folder size: ${size}`)
    fs.appendFileSync(reportPath, `| Indexes folder size | ${size} |\n`)
    t.end()
  })
})

test('teardown', (t) => {
  sbot.close(t.end)
})
