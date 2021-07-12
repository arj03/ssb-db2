const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bSequence = Buffer.from('sequence')

// authorId => latestMsg { offset, sequence }
//
// Necessary for feed validation and for EBT
module.exports = function makeBaseIndex(privateIndex) {
  return class BaseIndex extends Plugin {
    constructor(log, dir) {
      super(log, dir, 'base', 2, undefined, 'json')
      this.privateIndex = privateIndex
      this.authorLatest = new Map()
    }

    onLoaded(cb) {
      pull(
        this.getAllLatest(),
        pull.drain(
          ({ key, value }) => {
            this.authorLatest.set(key, value)
          },
          (err) => {
            cb()
          }
        )
      )
    }

    processRecord(record, seq) {
      const buf = record.value
      const pValue = bipf.seekKey(buf, 0, bValue)
      if (pValue < 0) return
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))
      const latestSequence = this.authorLatest.has(author)
        ? this.authorLatest.get(author).sequence
        : 0
      if (sequence > latestSequence) {
        const latest = { offset: record.offset, sequence }
        this.authorLatest.set(author, latest)
        this.batch.push({
          type: 'put',
          key: author,
          value: latest,
        })
      }
    }

    onFlush(cb) {
      this.privateIndex.saveIndexes(cb)
    }

    // pull-stream where each item is { key, value }
    // where key is the authorId and value is { offset, sequence }
    getAllLatest() {
      const META = '\x00'
      return pl.read(this.level, {
        gt: META,
        valueEncoding: this.valueEncoding,
      })
    }

    // returns { offset, sequence }
    getLatest(feedId, cb) {
      this.level.get(feedId, { valueEncoding: this.valueEncoding }, cb)
    }

    removeFeedFromLatest(feedId, cb) {
      this.flush((err) => {
        if (err) cb(err)
        else
          this.level.del(feedId, (err2) => {
            if (err2) cb(err2)
            else cb()
          })
      })
    }
  }
}
