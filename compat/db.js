const pull = require('pull-stream')

// exports.name is blank to merge into global namespace

exports.manifest = {
  publish: 'async',
  whoami: 'sync',
  createWriteStream: 'sink',
}

exports.init = function (sbot, config) {
  sbot.add = sbot.db.add
  sbot.get = function get(idOrObject, cb) {
    if (typeof idOrObject === 'object' && idOrObject.meta)
      sbot.db.getMsg(idOrObject.id, cb)
    else if (typeof idOrObject === 'object')
      sbot.db.get(idOrObject.id, cb)
    else
      sbot.db.get(idOrObject, cb)
  }
  sbot.publish = sbot.db.publish
  sbot.whoami = () => ({ id: sbot.id })
  sbot.ready = () => true
  sbot.keys = config.keys
  sbot.createWriteStream = function createWriteStream(cb) {
    return pull(
      pull.asyncMap(sbot.db.add),
      pull.drain(
        () => {},
        cb ||
          ((err) => {
            console.error(`ssb-db2 createWriteStream got an error: ${err}`)
          })
      )
    )
  }
}
