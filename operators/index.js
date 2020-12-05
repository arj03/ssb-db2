const jitdbOperators = require('jitdb/operators')
const seekers = require('../seekers')

function toBuffer(value) {
  return Buffer.isBuffer(value) ? value : Buffer.from(value)
}

function key(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekKey,
      value: Buffer.from(value),
      indexType: 'key',
      prefix: 32,
    },
  }
}

function type(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekType,
      value: toBuffer(value),
      indexType: 'type',
    },
  }
}

function author(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekAuthor,
      value: toBuffer(value),
      indexType: 'author',
    },
  }
}

function channel(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekChannel,
      value: toBuffer(value),
      indexType: 'channel',
    },
  }
}

function isRoot() {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekRoot,
      value: undefined,
      indexType: 'root',
    },
  }
}

let bTrue = Buffer.alloc(1)
bTrue[0] = 1
function isPrivate() {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekPrivate,
      value: bTrue,
      indexType: 'private',
    },
  }
}

module.exports = Object.assign({}, jitdbOperators, {
  type,
  author,
  channel,
  key,
  isRoot,
  isPrivate,
})
