const na = require('sodium-native')

// this is for converting FeedKeys (ed25119 signing keys) into
// Diffie-Hellman keys (curve25519 based shared key encryption)

module.exports = class DHKeys {
  constructor (feedKeys) {
    // feedKeys = { secret: Buffer, public: Buffer }
    this.type = 3 // dh-key
    this.format = 0 // curve25519

    this.pk = Buffer.alloc(na.crypto_scalarmult_SCALARBYTES)
    na.crypto_sign_ed25519_pk_to_curve25519(this.pk, feedKeys.public)

    if (feedKeys.secret) {
      this.sk = Buffer.alloc(na.crypto_scalarmult_SCALARBYTES)
      na.crypto_sign_ed25519_sk_to_curve25519(this.sk, feedKeys.secret)
    } else {
      this.sk = null
    }
  }

  toBuffer () {
    return {
      secret: this.sk,
      public: this.pk
    }
  }

  toTFK () {
    let _sk
    if (this.sk) {
      _sk = Buffer.concat([
        Buffer.from([this.type]),
        Buffer.from([this.format]),
        this.sk
      ])
    }

    const _pk = Buffer.concat([
      Buffer.from([this.type]),
      Buffer.from([this.format]),
      this.pk
    ])

    return { secret: _sk, public: _pk }
  }
}
