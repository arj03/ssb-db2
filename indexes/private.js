const Obv = require('obz')
const bipf = require('bipf')
const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const { readFile, writeFile } = require('atomic-file-rw')
const toBuffer = require('typedarray-to-buffer')
const ssbKeys = require('ssb-keys')
const DeferredPromise = require('p-defer')
const path = require('path')
const Debug = require('debug')

const bfe = require('ssb-bfe')
const bendy = require('ssb-bendy-butt')
const { unboxKey, unboxBody } = require('envelope-js')

const { indexesPath } = require('../defaults')

module.exports = function (dir, config, keystore) {
  const latestOffset = Obv()
  const stateLoaded = DeferredPromise()
  let encrypted = []
  let canDecrypt = []
  const sbotId = config.keys.id

  const debug = Debug('ssb:db2:private')

  const encryptedFile = path.join(indexesPath(dir), 'encrypted.index')
  // an option is to cache the read keys instead of only where the
  // messages are, this has an overhead around storage.  The
  // performance of that is a decrease in unbox time to 50% of
  // original for box1 and around 75% box2
  const canDecryptFile = path.join(indexesPath(dir), 'canDecrypt.index')

  function save(filename, arr) {
    const buf = toBuffer(fic.compress(arr))
    const b = Buffer.alloc(4 + buf.length)
    b.writeInt32LE(latestOffset.value, 0)
    buf.copy(b, 4)

    writeFile(filename, b, (err) => {
      if (err) debug("failed to save file %o, got error %o", filename, err)
    })
  }

  function load(filename, cb) {
    readFile(filename, (err, buf) => {
      if (err) return cb(err)
      else if (!buf) return cb(new Error('empty file'))

      const offset = buf.readInt32LE(0)
      const body = buf.slice(4)

      cb(null, { offset, arr: fic.uncompress(body) })
    })
  }

  function loadIndexes(cb) {
    load(encryptedFile, (err, data) => {
      if (err) {
        latestOffset.set(-1)
        stateLoaded.resolve()
        if (err.code === 'ENOENT') cb()
        else if (err.message === 'empty file') cb()
        else cb(err)
        return
      }

      const { offset, arr } = data
      encrypted = arr

      debug('encrypted loaded', encrypted.length)

      load(canDecryptFile, (err, data) => {
        let canDecryptOffset = -1
        if (!err) {
          canDecrypt = data.arr
          canDecryptOffset = data.offset
          debug('canDecrypt loaded', canDecrypt.length)
        }

        latestOffset.set(Math.min(offset, canDecryptOffset))
        stateLoaded.resolve()
        debug('loaded offset', latestOffset.value)

        cb()
      })
    })
  }

  loadIndexes((err) => {
    if (err) throw err
  })

  let savedTimer
  function saveIndexes(cb) {
    if (!savedTimer) {
      savedTimer = setTimeout(() => {
        savedTimer = null
        save(encryptedFile, encrypted)
        save(canDecryptFile, canDecrypt)
      }, 1000)
    }
    cb()
  }

  function reconstructMessage(record, unboxedContent) {
    const msg = bipf.decode(record.value, 0)
    const originalContent = msg.value.content
    if (msg.value.author.endsWith('.bbfeed-v1') && Array.isArray(unboxedContent)) {
      msg.value.content = unboxedContent[0]
      msg.value.contentSignature = unboxedContent[1]
    } else
      msg.value.content = unboxedContent

    msg.meta = {
      private: true,
      originalContent,
    }

    const len = bipf.encodingLength(msg)
    const buf = Buffer.alloc(len)
    bipf.encode(msg, buf, 0)

    return { offset: record.offset, value: buf }
  }

  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bPrevious = Buffer.from('previous')
  const bContent = Buffer.from('content')

  // FIXME: use from bfe
  const CLASSICFEEDTYPE = Buffer.concat([
    Buffer.from([0]),
    Buffer.from([0])
  ])

  function decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key) {
    const plaintext = unboxBody(envelope, feed_id, prev_msg_id, read_key)
    if (plaintext) {
      if (feed_id.slice(0, 2).equals(CLASSICFEEDTYPE))
        return JSON.parse(plaintext.toString('utf8'))
      else
        return bendy.decodeBox2(plaintext)
    }
    else return ''
  }

  function decryptBox2(ciphertext, author, previous) {
    const envelope = Buffer.from(ciphertext.replace('.box2', ''), 'base64')
    const authorBFE = bfe.encode(author)
    const previousBFE = bfe.encode(previous)

    const trial_dm_keys = author !== sbotId ?
          [keystore.sharedDMKey(author), ...keystore.ownDMKeys()] :
          keystore.ownDMKeys()

    read_key = unboxKey(envelope, authorBFE, previousBFE, trial_dm_keys, {
      maxAttempts: 16,
    })

    if (read_key)
      return decryptBox2Msg(envelope, authorBFE, previousBFE, read_key)
    else return ''
  }

  function decryptBox1(ciphertext, keys) {
    return ssbKeys.unbox(ciphertext, keys)
  }

  function tryDecryptContent(ciphertext, recBuffer, pValue) {
    let content = ''
    if (ciphertext.endsWith('.box')) content = decryptBox1(ciphertext, config.keys)
    else if (ciphertext.endsWith('.box2')) {
      const pAuthor = bipf.seekKey(recBuffer, pValue, bAuthor)
      if (pAuthor >= 0) {
        const author = bipf.decode(recBuffer, pAuthor)
        const pPrevious = bipf.seekKey(recBuffer, pValue, bPrevious)
        if (pPrevious >= 0) {
          const previousMsg = bipf.decode(recBuffer, pPrevious)
          content = decryptBox2(ciphertext, author, previousMsg)
        }
      }
    }
    return content
  }

  function decrypt(record, streaming) {
    const recOffset = record.offset
    const recBuffer = record.value
    let p = 0 // note you pass in p!
    if (bsb.eq(canDecrypt, recOffset) !== -1) {
      const pValue = bipf.seekKey(recBuffer, p, bValue)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, bContent)
      if (pContent < 0) return record

      const ciphertext = bipf.decode(recBuffer, pContent)
      const content = tryDecryptContent(ciphertext, recBuffer, pValue)
      if (!content) return record

      const originalMsg = reconstructMessage(record, content)
      return originalMsg
    } else if (recOffset > latestOffset.value || !streaming) {
      if (streaming) latestOffset.set(recOffset)

      const pValue = bipf.seekKey(recBuffer, p, bValue)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, bContent)
      if (pContent < 0) return record

      const type = bipf.getEncodedType(recBuffer, pContent)
      if (type !== bipf.types.string) return record

      if (streaming)
        encrypted.push(recOffset)

      const ciphertext = bipf.decode(recBuffer, pContent)
      const content = tryDecryptContent(ciphertext, recBuffer, pValue)
      if (!content) return record

      canDecrypt.push(recOffset)
      if (!streaming) saveIndexes(() => {})
      return reconstructMessage(record, content)
    } else {
      return record
    }
  }

  return {
    latestOffset,
    decrypt,
    saveIndexes,
    stateLoaded: stateLoaded.promise,
  }
}

module.exports.reEncrypt = function (msg) {
  if (msg.meta && msg.meta.private) {
    msg.value.content = msg.meta.originalContent
    delete msg.meta
  }
  return msg
}
