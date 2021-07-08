const pull = require('pull-stream')

// exports.name is blank to merge into global namespace

exports.manifest = {
  publish: 'async',
  whoami: 'sync',
  createWriteStream: 'sink',
}

exports.init = function (sbot, config) {
  sbot.add = sbot.db.add
  sbot.get = sbot.db.get
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
