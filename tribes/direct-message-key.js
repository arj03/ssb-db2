/* eslint-disable camelcase */

const na = require('sodium-native')
const hkdf = require('futoin-hkdf')
const { slp } = require('envelope-js')
const crypto = require('crypto')
const { SALT, INFO_CONTEXT } = require('private-group-spec').constants.directMessages

const FeedKeys = require('./feed-keys')
const DHKeys = require('./dh-keys')
const bfe = require('ssb-bendy-butt/ssb-bfe')

const hash = 'SHA256'
const length = 32
const salt = SHA256(SALT)

function directMessageKey (my_dh_secret, my_dh_public, my_feed, your_dh_public, your_feed) {
  const input_keying_material = Buffer.alloc(na.crypto_scalarmult_BYTES)
  na.crypto_scalarmult(
    input_keying_material,
    my_dh_secret.slice(2), // just the key part of TFK
    your_dh_public.slice(2) // just the key part of TFK
  )

  const info_context = Buffer.from(INFO_CONTEXT, 'utf8')
  const info_keys = [
    Buffer.concat([my_dh_public, my_feed]),
    Buffer.concat([your_dh_public, your_feed])
  ].sort()
  const info = slp.encode([info_context, ...info_keys])

  return hkdf(input_keying_material, length, { salt, info, hash })
}

function SHA256 (input) {
  const hash = crypto.createHash('sha256')

  hash.update(input)
  return hash.digest()
}

directMessageKey.easy = EasyDirectMessageKey

function EasyDirectMessageKey (ssbKeys) {
  const myFeedKeys = new FeedKeys(ssbKeys)
  const my = {
    dh: new DHKeys(myFeedKeys.toBuffer()).toTFK(),
    feedId: bfe.encode.feed(ssbKeys.id)
  }

  return function EasyDirectMessageKey (feedId) {
    const yourFeedKeys = new FeedKeys({ public: feedId.replace('@', '') })
    const your = {
      dh: new DHKeys(yourFeedKeys.toBuffer()).toTFK(),
      feedId: bfe.encode.feed(feedId)
    }

    return directMessageKey(
      my.dh.secret, my.dh.public, my.feedId,
      your.dh.public, your.feedId
    )
  }
}

module.exports = directMessageKey
