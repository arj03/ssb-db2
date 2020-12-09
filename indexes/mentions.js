const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const jsonCodec = require('flumecodec/json')
const { offsets, liveOffsets } = require('../operators')

// 1 index:
// - mentions (msgId) => msg seqs

module.exports = function (log, dir) {
  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')
  const bMentions = Buffer.from('mentions')

  let batch = []

  function writeData(cb) {
    level.batch(batch, { keyEncoding: jsonCodec }, cb)
    batch = []
  }

  function handleData(data, processed) {
    if (data.seq < seq.value) return

    let p = 0 // note you pass in p!
    const pKey = bipf.seekKey(data.value, p, bKey)

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      const pContent = bipf.seekKey(data.value, p, bContent)
      if (~pContent) {
        const pMentions = bipf.seekKey(data.value, pContent, bMentions)
        if (~pMentions) {
          const mentionsData = bipf.decode(data.value, pMentions)
          if (Array.isArray(mentionsData)) {
            const shortKey = bipf.decode(data.value, pKey).slice(1, 10)
            mentionsData.forEach((mention) => {
              if (
                mention.link &&
                typeof mention.link === 'string' &&
                (mention.link[0] === '@' || mention.link[0] === '%')
              ) {
                batch.push({
                  type: 'put',
                  key: [mention.link, 'm', shortKey],
                  value: processed,
                })
              }
            })
          }
        }
      }
    }

    if (batch.length) return batch.length
    else return 0
  }

  function parseInt10(x) {
    return parseInt(x, 10)
  }

  const name = 'mentions'
  const { level, seq, onData, writeBatch } = Plugin(
    log,
    dir,
    name,
    1,
    handleData,
    writeData
  )

  function getResults(opts, live, cb) {
    pull(
      pl.read(level, opts),
      pull.collect((err, data) => {
        if (err) return cb(err)
        if (live) {
          const ps = pull(
            pl.read(level, Object.assign({}, opts, { live, old: false })),
            pull.map(parseInt10)
          )
          cb(null, liveOffsets(data.map(parseInt10), ps))
        } else cb(null, offsets(data.map(parseInt10)))
      })
    )
  }

  return {
    seq,
    onData,
    writeBatch,

    name,
    remove: level.clear,
    close: level.close.bind(level),

    getMessagesByMention: function (key, live, cb) {
      getResults(
        {
          gte: [key, 'm', ''],
          lte: [key, 'm', undefined],
          keyEncoding: jsonCodec,
          keys: false,
        },
        live,
        cb
      )
    },
  }
}
