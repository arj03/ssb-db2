const directMessageKey = require('ssb-tribes/lib/direct-message-key')
const SecretKey = require('ssb-tribes/lib/secret-key')
const { FeedId } = require('ssb-tribes/lib/cipherlinks')
const { keySchemes } = require('private-group-spec')

module.exports = function (config) {
  const dmCache = {}

  const buildDMKey = directMessageKey.easy(config.keys)

  function sharedDMKey(author) {
    if (!dmCache[author]) dmCache[author] = buildDMKey(author)

    return {
      key: dmCache[author],
      scheme: keySchemes.feed_id_dm,
    }
  }

  // FIXME: maybe if a feed has a meta feed, then we can assume it
  // does box2 as well
  function supportsBox2(feedId) {
    if (config.db2 && config.db2.alwaysbox2) return true
    else return feedId.endsWith('.bbfeed-v1') || feedId.endsWith('.fusion-v1')
  }

  let ownKeys = []

  function addBox2DMKey(key) {
    ownKeys.push(key)
  }

  function ownDMKeys() {
    return ownKeys.map((key) => {
      return { key, scheme: keySchemes.feed_id_self }
    })
  }

  return {
    ownDMKeys,
    TFKId: new FeedId(config.keys.id).toTFK(),
    sharedDMKey,
    supportsBox2,
    addBox2DMKey,
  }
}
