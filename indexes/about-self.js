const bipf = require('bipf')
const pull = require('pull-stream')
const pl = require('pull-level')
const fastJson = require('fast-json-stringify')
const Plugin = require('./plugin')

const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bAbout = Buffer.from('about')

const stringify = fastJson({
  title: 'AboutSelf Value',
  type: 'object',
  properties: {
    name: {
      type: 'string',
    },
    image: {
      type: 'string',
    },
    description: {
      type: 'string',
    },
  },
})

const valueEncoding = {
  encode: stringify,
  decode: JSON.parse,
  buffer: false,
  type: 'json',
}

// feedId => hydratedAboutObj
module.exports = class AboutSelf extends Plugin {
  constructor(log, dir) {
    // super(log, dir, 'aboutSelf', 3, undefined, valueEncoding)
    super(log, dir, 'aboutSelf', 3, 'json', 'json')
    this.profiles = {}
  }

  onLoaded(cb) {
    pull(
      pl.read(this.level, {
        gte: '',
        lte: undefined,
        keyEncoding: this.keyEncoding,
        valueEncoding: this.valueEncoding,
        keys: true,
      }),
      pull.drain((data) => (this.profiles[data.key] = data.value), cb)
    )
  }

  processRecord(record, seq) {
    const buf = record.value

    let p = 0 // note you pass in p!
    p = bipf.seekKey(buf, p, bValue)
    if (p < 0) return
    const pAuthor = bipf.seekKey(buf, p, bAuthor)
    const pContent = bipf.seekKey(buf, p, bContent)
    if (pContent < 0) return
    const pType = bipf.seekKey(buf, pContent, bType)
    if (pType < 0) return

    if (bipf.compareString(buf, pType, bAbout) === 0) {
      const author = bipf.decode(buf, pAuthor)
      const content = bipf.decode(buf, pContent)
      if (content.about !== author) return

      this.updateProfileData(author, content)

      this.batch.push({
        type: 'put',
        key: author,
        value: this.profiles[author],
      })
    }
  }

  updateProfileData(author, content) {
    let profile = this.profiles[author] || {
      name: '',
      description: '',
      image: '',
    }

    if (content.name) profile.name = content.name

    if (content.description) profile.description = content.description

    if (content.image && typeof content.image.link === 'string')
      profile.image = content.image.link
    else if (typeof content.image === 'string') profile.image = content.image

    this.profiles[author] = profile
  }

  getProfile(feedId) {
    return this.profiles[feedId] || {}
  }

  getLiveProfile(feedId) {
    return pl.read(this.level, {
      gte: feedId,
      lte: feedId,
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding,
      keys: false,
      live: true,
      old: false,
    })
  }

  getProfiles() {
    return this.profiles
  }
}
