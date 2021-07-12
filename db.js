const push = require('push-stream')
const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate') // TODO: remove this eventually
const validate2 =
  typeof localStorage === 'undefined' || localStorage === null
    ? require('ssb-validate2-rsjs')
    : require('ssb-validate2')
const bipf = require('bipf')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const Obv = require('obz')
const promisify = require('promisify-4loc')
const jitdbOperators = require('jitdb/operators')
const operators = require('./operators')
const JITDb = require('jitdb')
const Debug = require('debug')
const multicb = require('multicb')

const { indexesPath } = require('./defaults')
const { onceWhen } = require('./utils')
const DebouncingBatchAdd = require('./debounce-batch')
const Log = require('./log')
const Status = require('./status')
const makeBaseIndex = require('./indexes/base')
const KeysIndex = require('./indexes/keys')
const PrivateIndex = require('./indexes/private')

const {
  where,
  fromDB,
  key,
  author,
  deferred,
  toCallback,
  asOffsets,
} = operators

exports.name = 'db'

exports.version = '1.9.1'

exports.manifest = {
  get: 'async',
  add: 'async',
  publish: 'async',
  del: 'async',
  deleteFeed: 'async',
  addOOO: 'async',
  addBatch: 'async',
  addOOOBatch: 'async',
  getStatus: 'sync',

  // `query` should be `sync`, but secret-stack is automagically converting it
  // to async because of secret-stack/utils.js#hookOptionalCB. Eventually we
  // should include an option `synconly` in secret-stack that bypasses the hook,
  // but for now we leave the `query` API *implicitly* available in the plugin:

  // query: 'sync',
}

exports.init = function (sbot, config) {
  let self
  config = config || {}
  config.db2 = config.db2 || {}
  const indexes = {}
  const dir = config.path
  const privateIndex = PrivateIndex(dir, config.keys)
  const log = Log(dir, config, privateIndex)
  const jitdb = JITDb(log, indexesPath(dir))
  const status = Status(log, jitdb)
  const debug = Debug('ssb:db2')
  const post = Obv()
  const hmacKey = null
  const stateFeedsReady = Obv().set(false)
  const state = {}

  sbot.close.hook(function (fn, args) {
    close(() => {
      fn.apply(this, args)
    })
  })

  registerIndex(makeBaseIndex(privateIndex))
  registerIndex(KeysIndex)

  loadStateFeeds()

  function setStateFeedsReady(x) {
    stateFeedsReady.set(x)
  }

  function loadStateFeeds(cb) {
    // restore current state
    validate2.ready(() => {
      onDrain('base', () => {
        pull(
          indexes.base.getAllLatest(),
          paramap((latest, cb) => {
            getMsgByOffset(latest.value.offset, (err, kvt) => {
              if (err) cb(err)
              else cb(null, kvt)
            })
          }, 8),
          pull.collect((err, kvts) => {
            if (err) return console.error('loadStateFeeds failed: ' + err)
            for (const kvt of kvts) {
              updateState(kvt)
            }
            debug('getAllLatest is done setting up initial validate state')
            if (!stateFeedsReady.value) stateFeedsReady.set(true)
            if (cb) cb()
          })
        )
      })
    })
  }

  function updateState(kvt) {
    state[kvt.value.author] = kvt
  }

  // Crunch stats numbers to produce one number for the "indexing" progress
  status.obv((stats) => {
    const logSize = Math.max(1, stats.log) // 1 prevents division by zero
    const nums = Object.values(stats.indexes).concat(Object.values(stats.jit))
    const N = Math.max(1, nums.length) // 1 prevents division by zero
    const progress = Math.min(
      nums
        .map((offset) => Math.max(0, offset)) // avoid -1 numbers
        .map((offset) => offset / logSize) // this index's progress
        .reduce((acc, x) => acc + x, 0) / N, // avg = (sum of all progress) / N
      1 // never go above 1
    )
    sbot.emit('ssb:db2:indexing:progress', progress)
  })

  function guardAgainstDuplicateLogs(methodName) {
    if (sbot.db2migrate && sbot.db2migrate.doesOldLogExist()) {
      return new Error(
        'ssb-db2: refusing to ' +
          methodName +
          ' because the old log still exists. ' +
          'This is to protect your feed from forking ' +
          'into an irrecoverable state.'
      )
    }
  }

  function getHelper(id, onlyValue, cb) {
    self.query(
      where(key(id)),
      toCallback((err, results) => {
        if (err) return cb(err)
        else if (results.length)
          return cb(null, onlyValue ? results[0].value : results[0])
        else return cb(new Error('Key not found in database ' + id))
      })
    )
  }

  function get(id, cb) {
    getHelper(id, true, cb)
  }

  function getMsg(id, cb) {
    getHelper(id, false, cb)
  }

  function getMsgByOffset(offset, cb) {
    log.get(offset, (err, buf) => {
      if (err) return cb(err)
      cb(null, bipf.decode(buf, 0))
    })
  }

  function addOOOBatch(msgVals, cb) {
    const guard = guardAgainstDuplicateLogs('addOOOBatch()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        validate2.validateOOOBatch(hmacKey, msgVals, (err, keys) => {
          if (err) return cb(err)

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < msgVals.length; ++i)
            log.add(keys[i], msgVals[i], done())

          done(cb)
        })
      }
    )
  }

  function addBatch(msgVals, cb) {
    const guard = guardAgainstDuplicateLogs('addBatch()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const latestMsgVal =
          msgVals.length > 0 && state[msgVals[0].author]
            ? state[msgVals[0].author].value
            : null
        validate2.validateBatch(hmacKey, msgVals, latestMsgVal, (err, keys) => {
          if (err) return cb(err)

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < msgVals.length; ++i) {
            if (i === msgVals.length - 1) {
              // last KVT, let's update the latest state
              log.add(keys[i], msgVals[i], (err, kvt) => {
                if (!err && kvt) updateState(kvt)
                done()(err, kvt)
              })
            } else {
              log.add(keys[i], msgVals[i], done())
            }
          }

          done(cb)
        })
      }
    )
  }

  function addImmediately(msgVal, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const latestMsgVal = state[msgVal.author]
          ? state[msgVal.author].value
          : null
        validate2.validateSingle(hmacKey, msgVal, latestMsgVal, (err, key) => {
          if (err) return cb(err)
          updateState({ key, value: msgVal })
          log.add(key, msgVal, (err, data) => {
            if (err) return cb(err)
            post.set(data)
            cb(null, data)
          })
        })
      }
    )
  }

  const debouncePeriod = config.db2.addDebounce || 400
  const debouncer = new DebouncingBatchAdd(addBatch, debouncePeriod)

  function addOOO(msgVal, cb) {
    const guard = guardAgainstDuplicateLogs('addOOO()')
    if (guard) return cb(guard)

    validate2.validateOOOBatch(hmacKey, [msgVal], (err, keys) => {
      if (err) return cb(err)
      const key = keys[0]
      get(key, (err, data) => {
        if (data) return cb(null, data)
        log.add(key, msgVal, (err, data) => {
          if (err) return cb(err)
          post.set(data)
          cb(null, data)
        })
      })
    })
  }

  function publish(content, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (content.recps) content = ssbKeys.box(content, content.recps)
        const latestKVT = state[config.keys.id]
        const msgVal = validate.create(
          latestKVT ? { queue: [latestKVT] } : null,
          config.keys,
          null,
          content,
          Date.now()
        )
        const kvt = validate.toKeyValueTimestamp(msgVal)
        updateState(kvt)
        log.add(kvt.key, kvt.value, (err, data) => {
          post.set(data)
          cb(err, data)
        })
      }
    )
  }

  function del(msgId, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    self.query(
      where(key(msgId)),
      asOffsets(),
      toCallback((err, results) => {
        if (err) return cb(err)
        if (results.length === 0)
          return cb(
            new Error('cannot delete ' + msgId + ' because it was not found')
          )

        indexes['keys'].delMsg(msgId)
        log.del(results[0], cb)
      })
    )
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    jitdb.all(author(feedId), 0, false, true, (err, offsets) => {
      push(
        push.values(offsets),
        push.asyncMap((offset, cb) => {
          log.del(offset, cb)
        }),
        push.collect((err) => {
          if (err) cb(err)
          else {
            delete state[feedId]
            indexes.base.removeFeedFromLatest(feedId, cb)
          }
        })
      )
    })
  }

  function clearIndexes() {
    for (const indexName in indexes) indexes[indexName].remove(() => {})
  }

  function registerIndex(Index) {
    const index = new Index(log, dir)

    if (indexes[index.name]) throw 'Index already exists'

    index.offset((o) => status.updateIndex(index.name, o))

    indexes[index.name] = index
  }

  function updateIndexes() {
    const start = Date.now()

    const indexesArr = Object.values(indexes)

    const lowestOffset = Math.min(...indexesArr.map((idx) => idx.offset.value))
    debug(`lowest offset for all indexes is ${lowestOffset}`)

    log.stream({ gt: lowestOffset }).pipe({
      paused: false,
      write(record) {
        indexesArr.forEach((idx) => idx.onRecord(record, false))
      },
      end() {
        debug(`updateIndexes() scan time: ${Date.now() - start}ms`)
        const writeTasks = indexesArr.map((idx) =>
          promisify(idx.flush.bind(idx))()
        )
        Promise.all(writeTasks).then(() => {
          debug('updateIndexes() live streaming')
          log.stream({ gt: indexes['base'].offset.value, live: true }).pipe({
            paused: false,
            write(record) {
              indexesArr.forEach((idx) => idx.onRecord(record, true))
            },
          })
        })
      },
    })
  }

  function onDrain(indexName, cb) {
    if (!cb) {
      // default
      cb = indexName
      indexName = 'base'
    }

    // setTimeout to make sure extra indexes from secret-stack are also included
    setTimeout(() => {
      onIndexesStateLoaded(() => {
        log.onDrain(() => {
          const index = indexes[indexName]
          if (!index) return cb('Unknown index:' + indexName)

          status.updateLog()

          if (index.offset.value === log.since.value) {
            status.updateIndex(indexName, index.offset.value)
            cb()
          } else {
            const remove = index.offset(() => {
              if (index.offset.value === log.since.value) {
                remove()
                status.updateIndex(indexName, index.offset.value)
                cb()
              }
            })
          }
        })
      })
    })
  }

  function onIndexesStateLoaded(cb) {
    if (!onIndexesStateLoaded.promise) {
      const stateLoadedPromises = [privateIndex.stateLoaded]
      for (const indexName in indexes) {
        stateLoadedPromises.push(indexes[indexName].stateLoaded)
      }
      onIndexesStateLoaded.promise = Promise.all(stateLoadedPromises)
    }
    onIndexesStateLoaded.promise.then(cb)
  }

  // setTimeout to make sure extra indexes from secret-stack are also included
  const timer = setTimeout(() => {
    onIndexesStateLoaded(updateIndexes)
  })
  if (timer.unref) timer.unref()

  function close(cb) {
    const tasks = []
    for (const indexName in indexes) {
      const index = indexes[indexName]
      tasks.push(promisify(index.close.bind(index))())
    }
    return Promise.all(tasks)
      .then(() => promisify(log.close)())
      .then(cb)
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    // Before running the query, the log needs to be migrated/synced with the
    // old log and it should be 'drained'
    const waitUntilReady = deferred((meta, cb) => {
      if (sbot.db2migrate) {
        sbot.db2migrate.synchronized((isSynced) => {
          if (isSynced) log.onDrain(cb)
        })
      } else {
        log.onDrain(cb)
      }
    })

    if (first.meta) {
      return jitdbOperators.query(first, where(waitUntilReady), ...rest)
    } else {
      const ops = fromDB(jitdb)
      ops.meta.db = this
      return jitdbOperators.query(ops, where(waitUntilReady), first, ...rest)
    }
  }

  return (self = {
    // Public API:
    get,
    getMsg,
    query,
    del,
    deleteFeed,
    add: debouncer.add,
    publish,
    addOOO,
    addOOOBatch,
    getStatus: () => status.obv,
    operators,
    post,

    // needed primarily internally by other plugins in this project:
    addBatch,
    addImmediately,
    getLatest: indexes.base.getLatest.bind(indexes.base),
    getAllLatest: indexes.base.getAllLatest.bind(indexes.base),
    getLog: () => log,
    registerIndex,
    setStateFeedsReady,
    loadStateFeeds,
    getIndexes: () => indexes,
    getIndex: (index) => indexes[index],
    clearIndexes,
    onDrain,
    getJITDB: () => jitdb,
  })
}
