const na = require('sodium-native')

// This is for use as:
// - a group's symmetric key (`group_key`)
// - an envelopes top level key (`msg_key`) from which others are derived

class SecretKey {
  constructor (length) {
    this.key = na.sodium_malloc(length || na.crypto_secretbox_KEYBYTES)
    na.randombytes_buf(this.key)
  }

  toString (encoding = 'base64') {
    return this.key.toString(encoding)
  }

  toBuffer () {
    return this.key
  }
}

module.exports = SecretKey
