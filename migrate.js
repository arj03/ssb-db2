const fs = require('fs')
const pull = require('pull-stream')
const FlumeLog = require('flumelog-offset')
const AsyncLog = require('async-append-only-log')
const bipf = require('bipf')
const jsonCodec = require('flumecodec/json')
const Obv = require('obz')
const debug = require('debug')('ssb:db2:migrate')
const { BLOCK_SIZE, oldLogPath, newLogPath } = require('./defaults')
const seekers = require('./seekers')

function fileExists(filename) {
  return fs.existsSync(filename) && fs.statSync(filename).size > 0
}

function makeFileExistsObv(filename) {
  const obv = Obv()
  obv.set(fileExists(filename))
  return obv
}

function getOldLog(sbot, config, sbotPost) {
  const oldLog = FlumeLog(oldLogPath(config.path), {
    blockSize: BLOCK_SIZE,
    codec: jsonCodec,
  })
  const opts = {
    keys: true,
    seqs: true,
    value: true,
    sync: false,
    reverse: false,
    codec: jsonCodec,
  }
  const getStream = (moreOpts) =>
    oldLog.stream({ old: true, live: false, ...opts, ...moreOpts })
  const getLiveStream = () => oldLog.stream({ old: false, live: true, ...opts })
  const getSize = () => oldLog.since.value
  // FIXME: when we do #129, should replace Obv() with something from db.js
  const newMsgObv = sbotPost || sbot.post || Obv()
  return { getStream, getLiveStream, getSize, newMsgObv }
}

function toBIPF(msg) {
  const len = bipf.encodingLength(msg)
  const buf = Buffer.alloc(len)
  bipf.encode(msg, buf, 0)
  return buf
}

function scanAndCount(pushstream, cb) {
  let count = 0
  pushstream.pipe({
    paused: false,
    write: () => {
      count += 1
    },
    end: function (err) {
      if (this.ended) return
      this.ended = err || true
      cb(null, count)
    },
  })
}

function guardAgainstDecryptedMsg(msg) {
  if (
    (msg && msg.meta && msg.meta.private) ||
    (msg && msg.value && msg.value.meta && msg.value.meta.private)
  ) {
    return new Error(
      'ssb:db2:migrate was about to write ' +
        'private message *decrypted* to disk'
    )
  }
}

/**
 * Fallback algorithm that does the same as findMigratedOffset, but is slow
 * because it does a scan of BOTH new log and old log.
 */
function inefficientFindMigratedOffset(newLog, oldLog, cb) {
  scanAndCount(newLog.stream({ gte: 0 }), (err, msgCountNewLog) => {
    if (err) return cb(err) // TODO: might need an explain() here
    if (!msgCountNewLog) return cb(null, -1)

    let result = -1
    pull(
      oldLog.getStream({ gte: 0 }),
      pull.take(msgCountNewLog),
      pull.drain(
        (x) => {
          result = x.seq
        },
        (err) => {
          if (err) return cb(err) // TODO: might need an explain() here
          cb(null, result)
        }
      )
    )
  })
}

function findMigratedOffset(sbot, oldLog, newLog, cb) {
  if (!sbot.get) {
    debug('running in inefficient mode because no ssb-db is installed')
    inefficientFindMigratedOffset(newLog, oldLog, cb)
    return
  }

  newLog.onDrain(() => {
    if (typeof newLog.since.value !== 'number' || newLog.since.value < 0) {
      cb(null, -1)
      return
    }

    const offsetInNewLog = newLog.since.value
    newLog.get(offsetInNewLog, (err, buf) => {
      if (err) return cb(err) // TODO: might need an explain() here

      const msgKey = bipf.decode(buf, seekers.seekKey(buf))
      sbot.get(msgKey, (err2, msg, offsetInOldLog) => {
        if (err2) return cb(err2) // TODO: might need an explain() here

        if (typeof offsetInOldLog === 'number') {
          cb(null, offsetInOldLog)
        } else {
          // NOTE! Currently all versions of ssb-db do not support returning
          // byte offset, so this case will trigger always! The only way to
          // make ssb-db support it is to hack it with patch-package. This is
          // fine temporarily because the only change is faster performance.
          debug(
            'running in inefficient mode because your ssb-db ' +
              'does not support returning byte offset from ssb.get()'
          )
          inefficientFindMigratedOffset(newLog, oldLog, cb)
        }
      })
    })
  })
}

exports.name = 'db2migrate'

exports.version = '1.2.0'

exports.manifest = {
  start: 'sync',
  doesOldLogExist: 'sync',
  synchronized: 'async',
}

exports.init = function init(sbot, config) {
  const oldLogExists = makeFileExistsObv(oldLogPath(config.path))
  const sbotPost = sbot.post // !! Capture this before compat/ebt mutates it !!

  /**
   * Boolean obv that indicates whether the new log is synced with the old log.
   */
  const synchronized = Obv()
  synchronized.set(true) // assume true until we `start()`

  let started = false
  let hasCloseHook = false
  let retryPeriod = 250
  let drainAborter = null

  function oldLogMissingThenRetry(fn) {
    if (!hasCloseHook) {
      sbot.close.hook(function (fn, args) {
        if (drainAborter) drainAborter.abort()
        fn.apply(this, args)
      })
      hasCloseHook = true
    }
    oldLogExists.set(fileExists(oldLogPath(config.path)))
    if (oldLogExists.value === false) {
      retryPeriod = Math.min(retryPeriod * 2, 8000)
      setTimeout(fn, retryPeriod).unref()
      return true
    } else {
      return false
    }
  }

  if (config.db2 && config.db2.automigrate) {
    start()
  }

  function start() {
    if (started) return
    if (oldLogMissingThenRetry(start)) return
    started = true
    debug('started')

    synchronized.set(false)

    const oldLog = getOldLog(sbot, config, sbotPost)
    const newLog =
      sbot.db && sbot.db.getLog() && sbot.db.getLog().stream
        ? sbot.db.getLog()
        : AsyncLog(newLogPath(config.path), { blockSize: BLOCK_SIZE })

    let migratedSize = null

    function updateMigratedSizeAndPluck(obj) {
      // "seq" in flumedb is an abstract num, here it actually means "offset"
      migratedSize = obj.seq
      return obj.value
    }

    let progressCalls = 0
    function emitProgressEvent() {
      const oldSize = oldLog.getSize()
      if (oldSize > 0 && migratedSize !== null) {
        const progress = Math.min(migratedSize / oldSize, 1)
        if (
          progress === 1 ||
          progressCalls < 100 ||
          progressCalls++ % 1000 === 0
        ) {
          sbot.emit('ssb:db2:migrate:progress', progress)
        }
      }
    }

    let dataTransferred = 0 // FIXME: does this only work if the new log is empty?
    function writeToNewLog(data, cb) {
      dataTransferred += data.length
      // FIXME: could we use log.add since it already converts to BIPF?
      // FIXME: see also issue #16
      newLog.append(data, () => {})
      emitProgressEvent()
      if (dataTransferred % BLOCK_SIZE === 0) newLog.onDrain(cb)
      else cb()
    }

    findMigratedOffset(sbot, oldLog, newLog, (err, migratedOffset) => {
      if (err) return console.error(err)

      if (migratedOffset >= 0) {
        debug('continue migrating from previous offset %s', migratedOffset)
        migratedSize = migratedOffset
        emitProgressEvent()
      }

      let msgCountOldLog = 0
      pull(
        oldLog.getStream({ gt: migratedOffset }),
        pull.map(updateMigratedSizeAndPluck),
        pull.map(toBIPF),
        pull.asyncMap(writeToNewLog),
        (drainAborter = pull.drain(
          () => {
            msgCountOldLog++
          },
          (err) => {
            if (err) return console.error(err)

            // Inform the other parts of ssb-db2 that migration is done
            synchronized.set(true)
            debug('done migrating %s msgs from old log', msgCountOldLog)
            drainAborter = null

            // Setup periodic `debug` reporter of live msgs migrated
            let liveMsgCount = 0
            if (debug.enabled) {
              setInterval(() => {
                if (liveMsgCount === 0) return
                debug('%d msgs synced from old log to new log', liveMsgCount)
                liveMsgCount = 0
              }, 2000).unref()
            }

            // Setup migration of live new msgs identified on the old log
            oldLog.newMsgObv((msg) => {
              const guard = guardAgainstDecryptedMsg(msg)
              if (guard) throw guard

              writeToNewLog(toBIPF(msg), () => {
                liveMsgCount++
              })
            })
          }
        ))
      )
    })
  }

  return {
    start,
    doesOldLogExist: () => oldLogExists.value,
    synchronized,
    // dangerouslyKillOldLog, // FIXME: implement this
  }
}
