const fs = require('fs')
const path = require('path')
const pull = require('pull-stream')
const Notify = require('pull-notify')
const FlumeLog = require('flumelog-offset')
const bipf = require('bipf')
const jsonCodec = require('flumecodec/json')
const Obv = require('obv')
const debug = require('debug')('ssb:db2:migrate')

const blockSize = 64 * 1024

function getOldLogPath(config) {
  return path.join(config.path, 'flume', 'log.offset')
}

function skip(count, onDone) {
  let skipped = 0
  return pull.filter((x) => {
    if (skipped >= count) return true
    else {
      skipped++
      if (skipped === count && onDone) onDone(x)
      return false
    }
  })
}

function makeFileExistsObv(filename) {
  const obv = Obv()
  obv.set(fs.existsSync(filename))
  return obv
}

function getOldLogStreams(sbot, config) {
  if (sbot.createRawLogStream && sbot.createSequenceStream) {
    const logStream = sbot.createRawLogStream({ old: true, live: false })
    const logStreamLive = sbot.createRawLogStream({ old: false, live: true })
    const sizeStream = pull(
      sbot.createSequenceStream(),
      pull.filter((x) => x >= 0)
    )
    return [logStream, logStreamLive, sizeStream]
  } else {
    const oldLogPath = getOldLogPath(config)
    const oldLog = FlumeLog(oldLogPath, { blockSize, codec: jsonCodec })
    const opts = { seqs: true, codec: jsonCodec }
    const logStream = oldLog.stream({ old: true, live: false, ...opts })
    const logStreamLive = oldLog.stream({ old: false, live: true, ...opts })
    const notify = Notify()
    oldLog.since(notify)
    const sizeStream = pull(
      notify.listen(),
      pull.filter((x) => x >= 0)
    )
    return [logStream, logStreamLive, sizeStream]
  }
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

exports.init = function init(sbot, config, newLog) {
  const oldLogExists = makeFileExistsObv(getOldLogPath(config))

  let started = false

  if (config.db2 && config.db2.automigrate) {
    start()
  }

  function start() {
    if (oldLogExists.value === false) return
    if (started) return
    started = true

    const [oldLogStream, oldLogStreamLive, oldSizeStream] = getOldLogStreams(
      sbot,
      config
    )
    const newLogStream = newLog.stream({ gte: 0 })

    let oldSize = null
    let migratedSize = null
    let progressCalls = 0

    function updateOldSize(read) {
      read(null, function next(end, data) {
        if (end === true) return
        if (end) throw end
        oldSize = data
        read(null, next)
      })
    }

    function updateMigratedSize(obj) {
      migratedSize = obj.seq
    }

    function updateMigratedSizeAndPluck(obj) {
      updateMigratedSize(obj)
      return obj.value
    }

    function emitProgressEvent() {
      if (oldSize !== null && migratedSize !== null) {
        if (progressCalls < 100 || progressCalls++ % 1000 == 0) {
          const progress = migratedSize / oldSize
          sbot.emit('ssb:db2:migrate:progress', progress)
        }
      }
    }

    let dataTransferred = 0 // FIXME: does this only work if the new log is empty?
    function writeTo(log) {
      return (data, cb) => {
        dataTransferred += data.length
        // FIXME: could we use log.add since it already converts to BIPF?
        // FIXME: see also issue #16
        log.append(data, () => {})
        emitProgressEvent()
        if (dataTransferred % blockSize == 0) log.onDrain(cb)
        else cb()
      }
    }

    updateOldSize(oldSizeStream)

    scanAndCount(newLogStream, (err, msgCountNewLog) => {
      if (err) return console.error(err)
      if (msgCountNewLog === 0) debug('new log is empty, will start migrating')
      else debug('new log has %s msgs, will continue migrating', msgCountNewLog)
      pull(
        oldLogStream,
        skip(msgCountNewLog, function whenDoneSkipping(obj) {
          updateMigratedSize(obj)
          emitProgressEvent()
        }),
        pull.map(updateMigratedSizeAndPluck),
        pull.map(toBIPF),
        pull.asyncMap(writeTo(newLog)),
        pull.reduce(
          (x) => x + 1,
          0,
          (err, msgCountOldLog) => {
            if (err) return console.error(err)
            debug('done migrating %s msgs from old log', msgCountOldLog)

            pull(
              oldLogStreamLive,
              pull.map(updateMigratedSizeAndPluck),
              pull.map(toBIPF),
              pull.asyncMap(writeTo(newLog)),
              pull.drain(() => {
                debug('1 new msg synced from old log to new log')
              })
            )
          }
        )
      )
    })
  }

  return {
    start,
    oldLogExists,
    // dangerouslyKillOldLog, // FIXME: implement this
  }
}