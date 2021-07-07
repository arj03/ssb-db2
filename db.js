const push = require('push-stream')
const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate')
const bendy = require('ssb-bendy-butt')
const Obv = require('obz')
const promisify = require('promisify-4loc')
const jitdbOperators = require('jitdb/operators')
const operators = require('./operators')
const JITDb = require('jitdb')
const { isFeed, isCloakedMsg: isGroup } = require('ssb-ref')
const Debug = require('debug')

const bfe = require('ssb-bfe')
const { box } = require('envelope-js')
const { SecretKey } = require('ssb-box2')
const KeyStore = require('./keystore')

const { indexesPath } = require('./defaults')
const { onceWhen } = require('./utils')
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
  publishAs: 'async',
  del: 'async',
  deleteFeed: 'async',
  addOOO: 'async',
  addOOOStrictOrder: 'async',
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
  const keystore = KeyStore(config)
  const privateIndex = PrivateIndex(dir, config, keystore)
  const log = Log(dir, config, privateIndex)
  const jitdb = JITDb(log, indexesPath(dir))
  const status = Status(log, jitdb)
  const debug = Debug('ssb:db2')
  const post = Obv()
  const hmac_key = null
  const stateFeedsReady = Obv().set(false)
  let state = validate.initial()

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
    onDrain('base', () => {
      indexes.base.getAllLatest((err, last) => {
        // copy to so we avoid weirdness, because this object
        // tracks the state coming in to the database.
        for (const k in last) {
          state.feeds[k] = {
            id: last[k].id,
            timestamp: last[k].timestamp,
            sequence: last[k].sequence,
            queue: [],
          }
        }
        debug('getAllLatest is done setting up initial validate state')
        if (!stateFeedsReady.value) stateFeedsReady.set(true)
        if (cb) cb()
      })
    })
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

  function add(msg, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        try {
          state = validate.append(state, hmac_key, msg)
          if (state.error) return cb(state.error)
          const kv = state.queue[state.queue.length - 1]
          log.add(kv.key, kv.value, (err, data) => {
            post.set(data)
            cb(err, data)
          })
        } catch (ex) {
          return cb(ex)
        }
      }
    )
  }

  function addOOO(msg, cb) {
    const guard = guardAgainstDuplicateLogs('addOOO()')
    if (guard) return cb(guard)

    try {
      let oooState = validate.initial()
      validate.appendOOO(oooState, hmac_key, msg)

      if (oooState.error) return cb(oooState.error)

      const kv = oooState.queue[oooState.queue.length - 1]
      get(kv.key, (err, data) => {
        if (data) cb(null, data)
        else
          log.add(kv.key, kv.value, (err, data) => {
            post.set(data)
            cb(err, data)
          })
      })
    } catch (ex) {
      return cb(ex)
    }
  }

  function addOOOStrictOrder(msg, strictOrderState, cb) {
    const guard = guardAgainstDuplicateLogs('addOOOStrictOrder()')
    if (guard) return cb(guard)

    const knownAuthor = msg.author in strictOrderState.feeds

    try {
      if (!knownAuthor)
        strictOrderState = validate.appendOOO(strictOrderState, hmac_key, msg)
      else strictOrderState = validate.append(strictOrderState, hmac_key, msg)

      if (strictOrderState.error) return cb(strictOrderState.error)

      const kv = strictOrderState.queue[strictOrderState.queue.length - 1]
      log.add(kv.key, kv.value, (err, data) => {
        post.set(data)
        cb(err, data)
      })
    } catch (ex) {
      return cb(ex)
    }
  }

  function box2Classic(content, previous) {
    if (content.recps.length > 16)
      throw new Error(
        `private-group spec allows maximum 16 slots, but you've tried to use ${content.recps.length}`
      )

    if (!content.recps.every(isFeed))
      throw new Error('only feeds are supported as recipients') // for now

    const recipientKeys = content.recps.reduce((acc, recp) => {
      if (recp === config.keys.id) return [...acc, ...keystore.ownDMKeys()]
      else return [...acc, keystore.sharedDMKey(recp)]
    }, [])

    const plaintext = Buffer.from(JSON.stringify(content), 'utf8')
    const msgKey = new SecretKey().toBuffer()
    const previousMessageId = bfe.encode(previous)

    const envelope = box(
      plaintext,
      keystore.TFKId,
      previousMessageId,
      msgKey,
      recipientKeys
    )

    return envelope.toString('base64') + '.box2'
  }

  function encryptContent(content) {
    if (content.recps.every(keystore.supportsBox2)) {
      const feedState = state.feeds[config.keys.id]
      return box2Classic(content, feedState ? feedState.id : null)
    } else return ssbKeys.box(content, content.recps)
  }

  function encryptBendyButt(
    encodedAuthor,
    encodedContent,
    encodedPrevious,
    recps
  ) {
    if (recps.length > 16)
      throw new Error(
        `private-group spec allows maximum 16 slots, but you've tried to use ${content.recps.length}`
      )

    if (!recps.every(isFeed))
      throw new Error('only feeds are supported as recipients') // for now

    const recipientKeys = recps.reduce((acc, recp) => {
      if (recp === config.keys.id) return [...acc, ...keystore.ownDMKeys()]
      else return [...acc, keystore.sharedDMKey(recp)]
    }, [])

    const msgKey = new SecretKey().toBuffer()

    // FIXME: consider error if no recipientKeys

    const envelope = box(
      encodedContent,
      encodedAuthor,
      encodedPrevious,
      msgKey,
      recipientKeys
    )

    // maybe just return envelope directly?
    return envelope.toString('base64') + '.box2'
  }

  function publish(content, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (content.recps) content = encryptContent(content)

        state.queue = []
        state = validate.appendNew(
          state,
          null,
          config.keys,
          content,
          Date.now()
        )

        const kv = state.queue[state.queue.length - 1]
        log.add(kv.key, kv.value, (err, data) => {
          post.set(data)
          cb(err, data)
        })
      }
    )
  }

  function publishAs(feedKeys, subfeedKeys, content, cb) {
    const guard = guardAgainstDuplicateLogs('publishAs()')
    if (guard) return cb(guard)

    if (feedKeys.id.endsWith('.ed25519')) {
      // classic
      onceWhen(
        stateFeedsReady,
        (ready) => ready === true,
        () => {
          if (content.recps) content = encryptContent(content)

          state.queue = []
          state = validate.appendNew(state, null, feedKeys, content, Date.now())

          const kv = state.queue[state.queue.length - 1]
          log.add(kv.key, kv.value, (err, data) => {
            post.set(data)
            cb(err, data)
          })
        }
      )
    } else if (feedKeys.id.endsWith('.bbfeed-v1')) {
      // bendy butt
      const feedState = state.feeds[feedKeys.id]
      const previous = feedState ? feedState.id : null
      const sequence = feedState ? feedState.sequence : 1
      const msg = bendy.create(
        content,
        feedKeys,
        subfeedKeys,
        previous,
        sequence,
        Date.now(),
        encryptBendyButt
      )

      // FIXME: validate

      const key = bendy.hash(msg)

      state.feeds[feedKeys.id] = {
        id: key,
        sequence: sequence + 1,
      }

      log.add(key, msg, (err, data) => {
        post.set(data)
        cb(err, data)
      })
    } else throw ('Unknown feed format', feedKeys)
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
            delete state.feeds[feedId]
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
    // API:
    get,
    getMsg,
    query,
    del,
    deleteFeed,
    add,
    publish,
    publishAs,
    addOOO,
    addOOOStrictOrder,
    getStatus: () => status.obv,
    operators,
    post,
    addBox2DMKey: keystore.addBox2DMKey,

    // needed primarily internally by other plugins in this project:
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
