module.exports = class DebouncingBatchAdd {
  constructor(addBatch, period) {
    this.addBatch = addBatch
    this.period = period
    this.queueByAuthor = new Map()
    this.timestampsByAuthor = new Map()
    this.timer = null
  }

  flush(authorId) {
    const queue = this.queueByAuthor.get(authorId)
    const n = queue.length
    const msgVals = queue.map((x) => x[0])
    // Clear the queue memory BEFORE the callbacks trigger more queue additions
    this.queueByAuthor.delete(authorId)
    this.timestampsByAuthor.delete(authorId)
    // Add the messages in the queue
    this.addBatch(msgVals, (err, kvts) => {
      if (err) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][1]
          cb(err)
        }
      } else if (kvts.length !== n) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][1]
          cb(new Error(`unexpected addBatch mismatch: ${kvts.length}} != ${n}`))
        }
      } else {
        for (let i = 0; i < n; ++i) {
          const kvt = kvts[i]
          const cb = queue[i][1]
          cb(null, kvt)
        }
      }
    })
  }

  scheduleFlush() {
    // Timer is already enabled
    if (this.timer) return

    this.timer = setInterval(() => {
      // Turn off the timer if there is nothing to flush
      if (this.queueByAuthor.size === 0) {
        clearInterval(this.timer)
        this.timer = null
      }
      // For each author, flush if enough time has passed
      else {
        const now = Date.now()
        for (const authorId of this.queueByAuthor.keys()) {
          const lastAdded = this.timestampsByAuthor.get(authorId)
          if (now - lastAdded > this.period) {
            this.flush(authorId)
          }
        }
      }
    }, this.period * 0.5)
  }

  add = (msgVal, cb) => {
    const authorId = msgVal.author
    const queue = this.queueByAuthor.get(authorId) || []
    queue.push([msgVal, cb])
    this.queueByAuthor.set(authorId, queue)
    this.timestampsByAuthor.set(authorId, Date.now())
    this.scheduleFlush()
  }
}
