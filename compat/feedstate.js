const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  sbot.getFeedState = function(feedId, cb) {
    onceWhen(
      sbot.db.stateFeedsReady,
      (ready) => ready === true,
      () => {
        const feedState = sbot.db.getState().feeds[feedId]

        // this covers the case where you have a brand new feed
        if (!feedState) return cb(null, { id: null, sequence: 0 })

        return cb(null, {
          id: feedState.id,
          sequence: feedState.sequence
        })
      }
    )
  }
}
