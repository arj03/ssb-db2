const push = require('push-stream')
const hash = require('ssb-keys/util').hash
const validate = require('ssb-validate')
const keys = require('ssb-keys')
const Obv = require('obv')
const promisify = require('promisify-4loc')
const jitdbOperators = require('jitdb/operators')
const JITDb = require('jitdb')
const Debug = require('debug')

const { indexesPath } = require('./defaults')
const Log = require('./log')
const BaseIndex = require('./indexes/base')
const Private = require('./indexes/private')
const Migrate = require('./migrate')
// const Partial = require('./indexes/partial')

const { and, key, toCallback } = require('./operators')

function getId(msg) {
  return '%' + hash(JSON.stringify(msg, null, 2))
}

exports.init = function (sbot, dir, config) {
  const private = Private(dir, config.keys)
  const log = Log(dir, config, private)
  const jitdb = JITDb(log, indexesPath(dir))
  const baseIndex = BaseIndex(log, dir, private)
  const migrate = Migrate.init(sbot, config, log)
  //const contacts = fullIndex.contacts
  //const partial = Partial(dir)

  const debug = Debug('ssb:db2')

  const indexes = {
    base: baseIndex,
  }
  const post = Obv()
  const hmac_key = null
  let state = validate.initial()

  // restore current state
  baseIndex.getAllLatest((err, last) => {
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
  })

  function guardAgainstDuplicateLogs(methodName) {
    if (migrate.oldLogExists.value === true) {
      return new Error(
        'ssb-db2: refusing to ' +
          methodName +
          ' because the old log still exists. ' +
          'This is to protect your feed from forking ' +
          'into an irrecoverable state.'
      )
    }
  }

  function get(id, cb) {
    onIndexesStateLoaded(() => {
      query(
        and(key(id)),
        toCallback((err, results) => {
          if (err) return cb(err)
          else if (results.length) return cb(null, results[0].value)
          else return cb()
        })
      )
    })
  }

  function add(msg, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    const id = getId(msg)

    /*
      Beware:

      There is a race condition here if you add the same message quickly
      after another because baseIndex is lazy. The default js SSB
      implementation adds messages in order, so it doesn't really have
      this problem.
    */

    get(id, (err, data) => {
      if (data) cb(null, data)
      else log.add(id, msg, cb)
    })
  }

  function publish(msg, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    state.queue = []
    state = validate.appendNew(state, null, config.keys, msg, Date.now())
    add(state.queue[0].value, (err, data) => {
      post.set(data)
      cb(err, data)
    })
  }

  function del(key, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    // FIXME: this doesn't work anymore after changing base index
    baseIndex.keyToSeq(key, (err, seq) => {
      if (err) return cb(err)
      if (seq == null) return cb(new Error('seq is null!'))

      log.del(seq, cb)
    })
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    // FIXME: doesn't work, need test
    jitdb.onReady(() => {
      jitdb.query(
        {
          type: 'EQUAL',
          data: {
            seek: jitdb.seekAuthor,
            value: feedId,
            indexType: 'author',
          },
        },
        (err, results) => {
          push(
            push.values(results),
            push.asyncMap((msg, cb) => {
              del(msg.key, cb)
            }),
            push.collect((err) => {
              if (!err) {
                delete state.feeds[feedId]
                baseIndex.removeFeedFromLatest(feedId)
              }
              cb(err)
            })
          )
        }
      )
    })
  }

  function validateAndAddOOO(msg, cb) {
    const guard = guardAgainstDuplicateLogs('validateAndAddOOO()')
    if (guard) return cb(guard)

    try {
      let oooState = validate.initial()
      validate.appendOOO(oooState, hmac_key, msg)

      if (oooState.error) return cb(oooState.error)

      add(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function validateAndAdd(msg, cb) {
    const guard = guardAgainstDuplicateLogs('validateAndAdd()')
    if (guard) return cb(guard)

    const knownAuthor = msg.author in state.feeds

    try {
      if (!knownAuthor) state = validate.appendOOO(state, hmac_key, msg)
      else state = validate.append(state, hmac_key, msg)

      if (state.error) return cb(state.error)

      add(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function getStatus() {
    //const partialState = partial.getSync()
    //const graph = contacts.getGraphForFeedSync(config.keys.public)

    // partial
    /*
    let profilesSynced = 0
    let contactsSynced = 0
    let messagesSynced = 0
    let totalPartial = 0
    */

    // full
    let fullSynced = 0
    let totalFull = 0

    /*
    graph.following.forEach(relation => {
      if (partialState[relation] && partialState[relation]['full'])
        fullSynced += 1

      totalFull += 1
    })

    graph.extended.forEach(relation => {
      if (partialState[relation] && partialState[relation]['syncedProfile'])
        profilesSynced += 1
      if (partialState[relation] && partialState[relation]['syncedContacts'])
        contactsSynced += 1
      if (partialState[relation] && partialState[relation]['syncedMessages'])
        messagesSynced += 1

      totalPartial += 1
    })
    */

    const result = {
      log: log.since.value,
      indexes: {},
      /*
      partial: {
        totalPartial,
        profilesSynced,
        contactsSynced,
        messagesSynced,
        totalFull,
        fullSynced,
      }
      */
    }

    for (const indexName in indexes) {
      result.indexes[indexName] = indexes[indexName].seq.value
    }

    return result
  }

  function clearIndexes() {
    for (const indexName in indexes) indexes[indexName].remove(() => {})
  }

  function registerIndex(Index) {
    const index = Index(dir)

    if (indexes[index.name]) throw 'Index already exists'

    indexes[index.name] = index
  }

  function updateIndexes() {
    const start = Date.now()

    const indexesRun = Object.values(indexes)

    function liveStream() {
      debug('live streaming changes')
      log.stream({ gt: indexes['base'].seq.value, live: true }).pipe({
        paused: false,
        write: (data) => indexesRun.forEach((x) => x.onData(data, true)),
      })
    }

    const lowestSeq = Math.min(
      ...Object.values(indexes).map((x) => x.seq.value)
    )
    debug(`lowest seq for all indexes ${lowestSeq}`)

    log.stream({ gt: lowestSeq }).pipe({
      paused: false,
      write: (data) => indexesRun.forEach((x) => x.onData(data, false)),
      end: () => {
        const tasks = indexesRun.map((index) => promisify(index.writeBatch)())
        Promise.all(tasks).then(liveStream)

        debug(`index scan time: ${Date.now() - start}ms`)
      },
    })
  }

  function onDrain(indexName, cb) {
    if (!cb) {
      // default
      cb = indexName
      indexName = 'base'
    }

    onIndexesStateLoaded(() => {
      log.onDrain(() => {
        const index = indexes[indexName]
        if (!index) return cb('Unknown index:' + indexName)

        debug(`drain got log: ${index.seq.value}, index: ${index.seq.value}`)

        if (index.seq.value === log.since.value) {
          cb()
        } else {
          const remove = index.seq(() => {
            debug(
              `drain seq update got log: ${index.seq.value}, index: ${index.seq.value}`
            )
            if (index.seq.value === log.since.value) {
              remove()
              cb()
            }
          })
        }
      })
    })
  }

  function onIndexesStateLoaded(cb) {
    if (!onIndexesStateLoaded.promise) {
      const stateLoadedPromises = [private.stateLoaded]
      for (var index in indexes) {
        stateLoadedPromises.push(indexes[index].stateLoaded)
      }
      onIndexesStateLoaded.promise = Promise.all(stateLoadedPromises)
    }
    onIndexesStateLoaded.promise.then(cb)
  }

  // setTimeout here so we make that extra indexes are also included
  setTimeout(() => {
    onIndexesStateLoaded(updateIndexes)
  })

  function close(cb) {
    const tasks = []
    tasks.push(promisify(log.close)())
    for (const indexName in indexes) {
      tasks.push(promisify(indexes[indexName].close)())
    }
    return Promise.all(tasks).then(cb)
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    if (!first.meta) {
      const ops = jitdbOperators.fromDB(jitdb)
      ops.meta.db2 = this
      return jitdbOperators.query(ops, first, ...rest)
    } else {
      return jitdbOperators.query(first, ...rest)
    }
  }

  return {
    get,
    getSync: function (id, cb) {
      onDrain('base', () => {
        get(id, cb)
      })
    },
    add,
    publish,
    del,
    deleteFeed,
    validateAndAdd,
    validateAndAddOOO,
    getStatus,
    close,

    post,

    registerIndex,
    getIndexes: function () {
      return indexes
    },

    getLatest: baseIndex.getLatest,
    getAllLatest: baseIndex.getAllLatest,
    // EBT
    getMessageFromAuthorSequence: baseIndex.getMessageFromAuthorSequence,
    migrate,

    // FIXME: contacts & profiles

    jitdb,
    onDrain,
    query,

    // hack
    state,

    // debugging
    clearIndexes,

    // partial stuff
    //partial
  }
}
