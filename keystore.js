const { directMessageKey, SecretKey } = require('ssb-box2')
const bfe = require('ssb-bfe')
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
    TFKId: bfe.encode(config.keys.id),
    sharedDMKey,
    supportsBox2,
    addBox2DMKey,
  }
}
