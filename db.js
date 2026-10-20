// db.js
// PostgreSQL persistence layer for BloxyVault's multiplayer server.
// This file only ever runs server-side (required by server.js) - nothing
// here is ever sent to or reachable from the browser, and DATABASE_URL
// itself only ever lives in Railway's environment variables, never in code.
//
// Design: server.js keeps its existing in-memory `users` Map as a
// write-through cache (unchanged game logic, same synchronous balance
// checks it already had), and calls into this file to persist state to
// Postgres alongside every mutation. Postgres is the durable source of
// truth; memory is just a fast, always-current mirror of it.

const { Pool } = require('pg');

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL is not set. Add the PostgreSQL plugin to this Railway project, " +
    "then reference its DATABASE_URL into this service's Variables tab."
  );
}

// Railway's internal Postgres connections (service-to-service, same project)
// don't need SSL. If DATABASE_URL ever points somewhere external (e.g. the
// public proxy URL, or a local dev DB you're testing against), enable it
// automatically instead of needing a separate flag to remember.
const useSSL = /sslmode=require/i.test(process.env.DATABASE_URL) || process.env.PGSSL === 'true';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

// A dropped idle connection must never crash the whole process - the pool
// recovers on the next query either way.
pool.on('error', (err) => {
  console.error('[db] Unexpected error on idle Postgres client:', err);
});

// ---------------------------------------------------------------------------
// Schema - created on boot if it doesn't already exist. Safe to run on every
// deploy (IF NOT EXISTS everywhere), so there's no separate migration step
// to remember.
// ---------------------------------------------------------------------------
async function initSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      username        TEXT PRIMARY KEY,
      coins           BIGINT NOT NULL DEFAULT 0 CHECK (coins >= 0),
      stats_wagered   BIGINT NOT NULL DEFAULT 0,
      stats_won       BIGINT NOT NULL DEFAULT 0,
      stats_lost      BIGINT NOT NULL DEFAULT 0,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS inventory_items (
      username     TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
      catalog_id   TEXT NOT NULL,
      qty          INTEGER NOT NULL DEFAULT 0 CHECK (qty >= 0),
      PRIMARY KEY (username, catalog_id)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS withdraw_requests (
      id                TEXT PRIMARY KEY,
      username          TEXT NOT NULL REFERENCES users(username),
      items             JSONB NOT NULL,
      status            TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending','fulfilled','rejected','cancelled')),
      requested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      resolved_at       TIMESTAMPTZ,
      actually_removed  JSONB
    );
  `);

  // A waiting Coinflip lobby escrows the creator's items out of their
  // inventory the instant it's created (see handleCoinflipCreate), well
  // before anyone actually joins it. Without persisting the lobby itself,
  // a server restart while it's still sitting there waiting would wipe it
  // from memory with no record of who those escrowed items belong to -
  // they'd just be gone, even though nobody ever joined and "won" them.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS coinflip_lobbies (
      id            TEXT PRIMARY KEY,
      creator       TEXT NOT NULL REFERENCES users(username),
      side          TEXT NOT NULL,
      items         JSONB NOT NULL,
      locked        BOOLEAN,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // Migration for any database that already had this table from before the
  // normal/duped pet-lock feature existed - CREATE TABLE IF NOT EXISTS
  // alone won't add a missing column to an already-existing table.
  await pool.query(`ALTER TABLE coinflip_lobbies ADD COLUMN IF NOT EXISTS locked BOOLEAN;`);

  // Notifications - withdrawal fulfilled/rejected, Case Battle wins. Server-
  // created only (see createNotification in server.js); a user can only
  // ever read/mark-read their OWN rows, enforced by every query below
  // always filtering on the requesting connection's own username, never a
  // client-supplied one.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id            BIGSERIAL PRIMARY KEY,
      username      TEXT NOT NULL REFERENCES users(username),
      type          TEXT NOT NULL CHECK (type IN ('withdraw_fulfilled','withdraw_rejected','casebattle_win')),
      title         TEXT NOT NULL,
      message       TEXT NOT NULL,
      ref_id        TEXT,
      is_read       BOOLEAN NOT NULL DEFAULT false,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(username, type, ref_id)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_notifications_username_created ON notifications(username, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(username) WHERE is_read = false;`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS transactions (
      id               BIGSERIAL PRIMARY KEY,
      username         TEXT NOT NULL REFERENCES users(username),
      type             TEXT NOT NULL,
      amount           BIGINT NOT NULL,
      balance_before   BIGINT NOT NULL,
      balance_after    BIGINT NOT NULL,
      items_delta      JSONB,
      reason           TEXT,
      admin_username   TEXT,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_username ON transactions(username, created_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_withdraw_status ON withdraw_requests(status);`);

  // Coinflip history - was previously only tracked client-side in a
  // browser-local array (cfMyHistory), which is why it never survived a
  // restart/redeploy (and didn't even survive a page reload). One row per
  // resolved flip, from both participants' perspective at once - the query
  // that reads this back does the per-viewer transform (who's "opponent",
  // whether "you" won, etc.), same as the client used to compute locally.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS coinflip_history (
      id               BIGSERIAL PRIMARY KEY,
      creator          TEXT NOT NULL,
      joiner           TEXT NOT NULL,
      creator_side     TEXT NOT NULL,
      joiner_side      TEXT NOT NULL,
      creator_items    JSONB NOT NULL,
      joiner_items     JSONB NOT NULL,
      creator_value    BIGINT NOT NULL,
      joiner_value     BIGINT NOT NULL,
      result           TEXT NOT NULL,
      winner           TEXT NOT NULL,
      resolved_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_cf_history_creator ON coinflip_history(creator, resolved_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_cf_history_joiner ON coinflip_history(joiner, resolved_at DESC);`);

  // Escrow ledger: every coin/pet that is temporarily held by a running game
  // (dice duel, jackpot, case battle, sink, mines) gets a row here in the
  // SAME transaction that takes it from the player, and the row is deleted
  // in the SAME transaction that pays the game out. Anything still here on
  // boot belonged to a game that was interrupted (crash / redeploy) and is
  // refunded automatically - so a restart can never eat anyone's pets.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS escrows (
      id          TEXT PRIMARY KEY,
      username    TEXT NOT NULL,
      kind        TEXT NOT NULL,
      items       JSONB NOT NULL DEFAULT '[]'::jsonb,
      coins       BIGINT NOT NULL DEFAULT 0,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // Exchange stock (pets sold to the shop) - used to live only in memory,
  // so every restart wiped every pet anyone had sold.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS shop_stock (
      catalog_id  TEXT PRIMARY KEY,
      qty         INTEGER NOT NULL DEFAULT 0 CHECK (qty >= 0)
    );
  `);
  // Small key/value store for other global state that must survive restarts
  // (currently: the rain pool).
  await pool.query(`
    CREATE TABLE IF NOT EXISTS server_state (
      key    TEXT PRIMARY KEY,
      value  JSONB NOT NULL
    );
  `);

  // Discord linking (one Discord account can only be linked to one player).
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS discord_id TEXT;`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS discord_username TEXT;`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS discord_avatar TEXT;`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id) WHERE discord_id IS NOT NULL;`);

  // Per-player chat ignore list.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ignored_users (
      username    TEXT NOT NULL,
      ignored     TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (username, ignored)
    );
  `);

  // One row per finished game per player, for the Game History page.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS game_history (
      id          BIGSERIAL PRIMARY KEY,
      username    TEXT NOT NULL,
      game        TEXT NOT NULL,
      wager       BIGINT NOT NULL DEFAULT 0,
      payout      BIGINT NOT NULL DEFAULT 0,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_game_history_user ON game_history(username, created_at DESC);`);
  // Total value (coins + pets) of a transaction, so pet tips/withdrawals can
  // show a real number in Transaction History. Older rows leave it empty.
  await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS value BIGINT;`);

  // Allow giveaway wins in notifications.
  await pool.query(`ALTER TABLE notifications DROP CONSTRAINT IF EXISTS notifications_type_check;`);
  await pool.query(`ALTER TABLE notifications ADD CONSTRAINT notifications_type_check CHECK (type IN ('withdraw_fulfilled','withdraw_rejected','casebattle_win','giveaway_win'));`);

  // Vault: coins + pets a player sets aside (not usable in games), with an
  // optional time lock that blocks withdrawing until it runs out.
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS vault_coins BIGINT NOT NULL DEFAULT 0;`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS vault_lock_until TIMESTAMPTZ;`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS vault_items (
      username     TEXT NOT NULL,
      catalog_id   TEXT NOT NULL,
      qty          INTEGER NOT NULL DEFAULT 0 CHECK (qty >= 0),
      PRIMARY KEY (username, catalog_id)
    );
  `);
  // Community cases: cases players build themselves. items is a JSON array of
  // {id, chance, value} - value is frozen at creation so a case never changes
  // underneath the people opening it.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS community_cases (
      id          BIGSERIAL PRIMARY KEY,
      creator     TEXT NOT NULL,
      name        TEXT NOT NULL,
      image       TEXT NOT NULL,
      commission  NUMERIC(4,2) NOT NULL DEFAULT 0,
      price       BIGINT NOT NULL CHECK (price > 0),
      items       JSONB NOT NULL,
      opens       BIGINT NOT NULL DEFAULT 0,
      earned      BIGINT NOT NULL DEFAULT 0,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // Added later: deleted cases go inactive (kept for the creator's stats),
  // and commission collects in 'unclaimed' until the creator claims it.
  await pool.query(`ALTER TABLE community_cases ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true`);
  await pool.query(`ALTER TABLE community_cases ADD COLUMN IF NOT EXISTS unclaimed BIGINT NOT NULL DEFAULT 0`);
  // Promo codes (admin-made, coins only) and who has used which.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS promo_codes (
      code        TEXT PRIMARY KEY,
      coins       BIGINT NOT NULL CHECK (coins > 0),
      max_uses    INTEGER NOT NULL CHECK (max_uses > 0),
      uses        INTEGER NOT NULL DEFAULT 0,
      expires_at  TIMESTAMPTZ,
      active      BOOLEAN NOT NULL DEFAULT true,
      created_by  TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS promo_redemptions (
      code         TEXT NOT NULL,
      username     TEXT NOT NULL,
      redeemed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (code, username)
    );
  `);

  console.log('[db] Schema ready.');
}

// ---------------------------------------------------------------------------
// Boot-time loads - populate server.js's in-memory Map/array from Postgres
// before the server starts accepting connections.
// ---------------------------------------------------------------------------

// Returns a Map in the exact shape server.js's `users` Map already expects:
// username -> { coins, inventory: {catalogId: qty}, stats: {wagered, won, lost} }
async function loadAllUsers() {
  const usersRes = await pool.query('SELECT username, coins, stats_wagered, stats_won, stats_lost, discord_id, discord_username, discord_avatar, vault_coins, vault_lock_until FROM users');
  // This game has no duped pets: clear out any left over from the version
  // that briefly had them (only ids starting with dup_ - nothing else).
  await pool.query("DELETE FROM inventory_items WHERE catalog_id LIKE 'dup\\_%'");
  await pool.query("DELETE FROM vault_items WHERE catalog_id LIKE 'dup\\_%'");
  const invRes = await pool.query('SELECT username, catalog_id, qty FROM inventory_items WHERE qty > 0');
  const vaultRes = await pool.query('SELECT username, catalog_id, qty FROM vault_items WHERE qty > 0');

  const map = new Map();
  for (const row of usersRes.rows) {
    map.set(row.username, {
      coins: Number(row.coins),
      inventory: {},
      stats: { wagered: Number(row.stats_wagered), won: Number(row.stats_won), lost: Number(row.stats_lost) },
      discord: row.discord_id ? { id: row.discord_id, username: row.discord_username, avatar: row.discord_avatar } : null,
      vault: { coins: Number(row.vault_coins || 0), items: {}, lockUntil: row.vault_lock_until ? new Date(row.vault_lock_until).getTime() : 0 },
    });
  }
  for (const row of invRes.rows) {
    const u = map.get(row.username);
    if (u) u.inventory[row.catalog_id] = row.qty;
  }
  for (const row of vaultRes.rows) {
    const u = map.get(row.username);
    if (u) u.vault.items[row.catalog_id] = row.qty;
  }
  return map;
}

// Returns pending withdrawal requests in the exact shape server.js's
// `withdrawRequests` array already expects.
async function loadPendingWithdrawRequests() {
  const res = await pool.query(
    `SELECT id, username, items, status, requested_at, actually_removed
     FROM withdraw_requests WHERE status = 'pending' ORDER BY requested_at ASC`
  );
  return res.rows.map((r) => ({
    id: r.id,
    username: r.username,
    items: r.items,
    status: r.status,
    requestedAt: new Date(r.requested_at).getTime(),
    actuallyRemoved: r.actually_removed || undefined,
  }));
}

// Returns still-waiting Coinflip lobbies in the exact shape server.js's
// `cfLobbies` array already expects - every row here is a lobby that was
// created but never joined/cancelled, so its items are still escrowed.
async function loadPendingCoinflipLobbies() {
  const res = await pool.query(
    `SELECT id, creator, side, items, locked FROM coinflip_lobbies ORDER BY created_at ASC`
  );
  return res.rows.map((r) => ({
    id: r.id,
    creator: r.creator,
    side: r.side,
    items: r.items,
    // null (a lobby persisted from before this feature existed) is
    // deliberately left as undefined here, not coerced to false - see
    // joinItemsMatchLockedLobby in server.js, which treats undefined as
    // "legacy lobby, no pet-identity restriction" rather than "unlocked".
    locked: r.locked == null ? undefined : r.locked,
  }));
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

// Inserts a brand-new user row (idempotent - ON CONFLICT DO NOTHING, since
// ensureUser() in server.js is the caller and only fires this the first
// time it creates someone in memory, but a second caller racing in is fine).
async function insertNewUser(username, coins, stats) {
  await pool.query(
    `INSERT INTO users (username, coins, stats_wagered, stats_won, stats_lost)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (username) DO NOTHING`,
    [username, coins, stats.wagered, stats.won, stats.lost]
  );
}

// The main persistence call: writes a user's current coins/stats, upserts
// only the specific inventory rows that changed (not the whole inventory -
// keeps this cheap even for accounts with a lot of items), and - if a txn
// is supplied - writes one immutable transaction/audit row. All of it in a
// single real Postgres transaction, so a crash mid-write can never leave
// coins updated but the inventory or audit row missing, and the audit row
// is only ever written alongside a successfully committed balance change.
//
// `user` is the in-memory user object AFTER the mutation already happened
// (server.js updates memory synchronously first, exactly as before - this
// just mirrors that already-decided state into Postgres).
//
// `txn` (optional): {
//   type: string,                 // 'game_wager' | 'game_resolve' | 'tip_send' | ...
//   amount: number,                // coin delta, +/- (0 for item-only txns)
//   balanceBefore: number,
//   balanceAfter: number,
//   itemsTouched: string[],        // catalogIds whose qty changed - current qty is read from `user.inventory`
//   reason: string,
//   adminUsername: string,
// }
async function persistUserState(username, user, txn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Single atomic upsert. The old UPDATE-then-INSERT could collide with the
    // fire-and-forget insertNewUser() for a brand-new account (e.g. someone
    // tipped pets to a player who'd never logged in): the INSERT hit a
    // unique-key error, the whole transaction rolled back, and the tipped
    // pets were never saved - they vanished on the next restart.
    await client.query(
      `INSERT INTO users (username, coins, stats_wagered, stats_won, stats_lost) VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (username) DO UPDATE SET coins = EXCLUDED.coins, stats_wagered = EXCLUDED.stats_wagered,
         stats_won = EXCLUDED.stats_won, stats_lost = EXCLUDED.stats_lost, updated_at = now()`,
      [username, Math.max(0, Math.trunc(user.coins)), user.stats.wagered, user.stats.won, user.stats.lost]
    );

    // Vault rides in the same transaction as the wallet, so moving coins/pets
    // between the two can never lose or duplicate anything.
    if (user.vault) {
      await client.query(
        `UPDATE users SET vault_coins = $2, vault_lock_until = $3 WHERE username = $1`,
        [username, Math.max(0, Math.trunc(user.vault.coins || 0)), user.vault.lockUntil ? new Date(user.vault.lockUntil) : null]
      );
      if (txn && Array.isArray(txn.vaultItemsTouched)) {
        for (const catalogId of txn.vaultItemsTouched) {
          const qty = Math.max(0, (user.vault.items && user.vault.items[catalogId]) || 0);
          await client.query(
            `INSERT INTO vault_items (username, catalog_id, qty) VALUES ($1,$2,$3)
             ON CONFLICT (username, catalog_id) DO UPDATE SET qty = $3`,
            [username, catalogId, qty]
          );
        }
      }
    }

    if (txn && Array.isArray(txn.itemsTouched) && txn.itemsTouched.length) {
      for (const catalogId of txn.itemsTouched) {
        const qty = Math.max(0, user.inventory[catalogId] || 0);
        await client.query(
          `INSERT INTO inventory_items (username, catalog_id, qty) VALUES ($1,$2,$3)
           ON CONFLICT (username, catalog_id) DO UPDATE SET qty = $3`,
          [username, catalogId, qty]
        );
      }
    }

    // Escrow bookkeeping rides in the same transaction as the balance change,
    // so "pets left your inventory" and "pets are held by game X" (or "game X
    // paid out" and "hold released") always commit together or not at all.
    if (txn && Array.isArray(txn.escrowAdd)) {
      for (const e of txn.escrowAdd) {
        await client.query(
          `INSERT INTO escrows (id, username, kind, items, coins) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING`,
          [e.id, username, e.kind, JSON.stringify(e.items || []), Math.trunc(e.coins || 0)]
        );
      }
    }
    if (txn && Array.isArray(txn.escrowRelease) && txn.escrowRelease.length) {
      await client.query(`DELETE FROM escrows WHERE id = ANY($1)`, [txn.escrowRelease]);
    }

    if (txn) {
      await client.query(
        `INSERT INTO transactions (username, type, amount, balance_before, balance_after, items_delta, reason, admin_username, value)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [
          username,
          txn.type,
          Math.trunc(txn.amount || 0),
          Math.trunc(txn.balanceBefore != null ? txn.balanceBefore : user.coins),
          Math.trunc(txn.balanceAfter != null ? txn.balanceAfter : user.coins),
          txn.itemsTouched && txn.itemsTouched.length ? JSON.stringify(txn.itemsTouched) : null,
          txn.reason || null,
          txn.adminUsername || null,
          Number.isFinite(txn.value) ? Math.trunc(txn.value) : null,
        ]
      );
      // A finished game: one Game History row, in the same DB transaction
      // as the payout itself.
      if (txn.game && txn.game.game) {
        await client.query(
          `INSERT INTO game_history (username, game, wager, payout) VALUES ($1,$2,$3,$4)`,
          [username, String(txn.game.game), Math.max(0, Math.trunc(txn.game.wager || 0)), Math.max(0, Math.trunc(txn.game.payout || 0))]
        );
      }
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[db] Failed to persist state for ${username}:`, err);
    throw err;
  } finally {
    client.release();
  }
}

async function insertWithdrawRequest(req) {
  await pool.query(
    `INSERT INTO withdraw_requests (id, username, items, status, requested_at) VALUES ($1,$2,$3,$4, to_timestamp($5/1000.0))`,
    [req.id, req.username, JSON.stringify(req.items), req.status, req.requestedAt]
  );
}

async function insertCoinflipLobby(lobby) {
  await pool.query(
    `INSERT INTO coinflip_lobbies (id, creator, side, items, locked) VALUES ($1,$2,$3,$4,$5)`,
    [lobby.id, lobby.creator, lobby.side, JSON.stringify(lobby.items), !!lobby.locked]
  );
}

// Called once a lobby is joined, cancelled, or otherwise resolved - it's no
// longer "waiting" at that point, so there's nothing left to protect against
// a restart for. Safe to call even if the row's already gone (e.g. a race
// between join and cancel) - just deletes 0 rows in that case.
async function deleteCoinflipLobby(id) {
  await pool.query(`DELETE FROM coinflip_lobbies WHERE id = $1`, [id]);
}

// Atomic status transition: only succeeds if the request is currently in
// `fromStatus`. Returns the updated row, or null if it had already been
// resolved by someone else (or another concurrent request) - the caller
// uses that to know whether it actually needs to act, so the same request
// can never be fulfilled/rejected/cancelled twice even under a race.
async function resolveWithdrawRequest(id, fromStatus, toStatus, actuallyRemoved) {
  const res = await pool.query(
    `UPDATE withdraw_requests SET status = $1, resolved_at = now(), actually_removed = $2
     WHERE id = $3 AND status = $4 RETURNING id`,
    [toStatus, actuallyRemoved ? JSON.stringify(actuallyRemoved) : null, id, fromStatus]
  );
  return res.rowCount > 0;
}

// Returns groups of usernames that differ only by capitalization - e.g.
// ["DTN_bgsi", "DTN_BGSI"] would come back as one cluster. Each cluster has
// 2+ members; usernames with no case-variant duplicate aren't included.
async function findDuplicateCaseClusters() {
  const res = await pool.query('SELECT username FROM users');
  const byLower = new Map();
  for (const { username } of res.rows) {
    const key = username.toLowerCase();
    if (!byLower.has(key)) byLower.set(key, []);
    byLower.get(key).push(username);
  }
  return [...byLower.values()].filter((cluster) => cluster.length > 1);
}

// Merges every username in `variants` into `canonicalUsername`, atomically,
// with no data loss:
//   - coins: MAX across variants (not summed - two accidental duplicates
//     both created at STARTING_BALANCE, or both independently topped up by
//     the same self-healing logic like DTN_BGSI's 99x/2B seed, would double
//     count if summed even though nothing extra was actually earned).
//   - inventory: MAX per item, same reasoning - the union of everything any
//     variant owned, without inflating an item both variants happened to
//     share.
//   - stats (wagered/won/lost): SUMMED - these are cumulative counters of
//     genuinely distinct real actions, so two real histories combining is
//     correct, not a duplication.
//   - withdrawal requests, transactions, and coinflip history: every row
//     is re-pointed to the canonical username, not duplicated or dropped -
//     the full audit trail survives under one identity.
// `canonicalUsername` does not need to already exist in `variants` - if
// Roblox's real casing doesn't match any current duplicate, this creates it
// fresh with the merged totals.
async function mergeCasedDuplicates(canonicalUsername, variants) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const usersRes = await client.query('SELECT username, coins, stats_wagered, stats_won, stats_lost FROM users WHERE username = ANY($1)', [variants]);
    if (usersRes.rows.length < 2) { await client.query('ROLLBACK'); return null; } // nothing to merge after all

    const invRes = await client.query('SELECT username, catalog_id, qty FROM inventory_items WHERE username = ANY($1)', [variants]);

    // SUM, not MAX: new accounts start at 0 coins, so nothing here was ever
    // a free starting grant that summing would double-count - MAX silently
    // deleted whatever the smaller variant had earned.
    const mergedCoins = usersRes.rows.reduce((s, r) => s + Number(r.coins), 0);
    const mergedWagered = usersRes.rows.reduce((s, r) => s + Number(r.stats_wagered), 0);
    const mergedWon = usersRes.rows.reduce((s, r) => s + Number(r.stats_won), 0);
    const mergedLost = usersRes.rows.reduce((s, r) => s + Number(r.stats_lost), 0);

    const mergedInventory = {}; // catalogId -> max qty across variants
    for (const row of invRes.rows) {
      mergedInventory[row.catalog_id] = (mergedInventory[row.catalog_id] || 0) + row.qty; // SUM - MAX deleted pets
    }

    // Upsert the canonical row with the merged totals.
    const upd = await client.query(
      `UPDATE users SET coins = $2, stats_wagered = $3, stats_won = $4, stats_lost = $5, updated_at = now() WHERE username = $1`,
      [canonicalUsername, mergedCoins, mergedWagered, mergedWon, mergedLost]
    );
    if (upd.rowCount === 0) {
      await client.query(
        `INSERT INTO users (username, coins, stats_wagered, stats_won, stats_lost) VALUES ($1,$2,$3,$4,$5)`,
        [canonicalUsername, mergedCoins, mergedWagered, mergedWon, mergedLost]
      );
    }

    for (const [catalogId, qty] of Object.entries(mergedInventory)) {
      await client.query(
        `INSERT INTO inventory_items (username, catalog_id, qty) VALUES ($1,$2,$3)
         ON CONFLICT (username, catalog_id) DO UPDATE SET qty = $3`,
        [canonicalUsername, catalogId, qty]
      );
    }

    const others = variants.filter((v) => v !== canonicalUsername);
    if (others.length) {
      // Re-point every historical record so nothing is lost or orphaned -
      // just relabeled under the one identity that now owns everything.
      await client.query(`UPDATE withdraw_requests SET username = $1 WHERE username = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE transactions SET username = $1 WHERE username = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE transactions SET admin_username = $1 WHERE admin_username = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE coinflip_history SET creator = $1 WHERE creator = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE coinflip_history SET joiner = $1 WHERE joiner = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE coinflip_history SET winner = $1 WHERE winner = ANY($2)`, [canonicalUsername, others]);
      // These tables reference users(username) too - without re-pointing them
      // the DELETE below hit a foreign-key error and the merge never finished.
      await client.query(`UPDATE coinflip_lobbies SET creator = $1 WHERE creator = ANY($2)`, [canonicalUsername, others]);
      await client.query(`UPDATE escrows SET username = $1 WHERE username = ANY($2)`, [canonicalUsername, others]);
      await client.query(`DELETE FROM ignored_users WHERE username = ANY($1) OR ignored = ANY($1)`, [others]);
      await client.query(`DELETE FROM notifications WHERE username = ANY($1)`, [others]);
      // Deleting the old rows cascades to their now-redundant inventory_items
      // rows automatically (ON DELETE CASCADE) - already fully accounted
      // for in mergedInventory above, so nothing is lost by the cascade.
      await client.query(`DELETE FROM users WHERE username = ANY($1)`, [others]);
    }

    // Leave a visible audit trail of the merge itself.
    await client.query(
      `INSERT INTO transactions (username, type, amount, balance_before, balance_after, items_delta, reason)
       VALUES ($1, 'account_merge', 0, $2, $2, $3, $4)`,
      [canonicalUsername, mergedCoins, JSON.stringify(Object.keys(mergedInventory)), `Merged duplicate case-variant accounts: ${variants.join(', ')}`]
    );

    await client.query('COMMIT');
    return { canonicalUsername, mergedFrom: others, coins: mergedCoins, itemCount: Object.keys(mergedInventory).length };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// One row per resolved flip, both participants at once. Fire-and-forget
// from the caller, same pattern as persistUser - a coinflip's outcome has
// already been decided and broadcast by the time this is called, so a slow
// DB write here should never hold up the live result.
// Returns a user's most recent transactions, newest first - used by the
// admin lookup panel. Straightforward read against the existing Phase 1
// transactions table, no schema changes needed.
async function loadRecentTransactionsForUser(username, limit) {
  const res = await pool.query(
    `SELECT type, amount, balance_before, balance_after, items_delta, reason, admin_username, created_at
     FROM transactions WHERE username = $1 ORDER BY created_at DESC LIMIT $2`,
    [username, limit]
  );
  return res.rows.map((r) => ({
    type: r.type,
    amount: Number(r.amount),
    balanceBefore: Number(r.balance_before),
    balanceAfter: Number(r.balance_after),
    itemsDelta: r.items_delta,
    reason: r.reason,
    adminUsername: r.admin_username,
    timestamp: new Date(r.created_at).getTime(),
  }));
}

async function insertCoinflipHistory(row) {
  await pool.query(
    `INSERT INTO coinflip_history (creator, joiner, creator_side, joiner_side, creator_items, joiner_items, creator_value, joiner_value, result, winner)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      row.creator, row.joiner, row.creatorSide, row.joinerSide,
      JSON.stringify(row.creatorItems), JSON.stringify(row.joinerItems),
      row.creatorValue, row.joinerValue, row.result, row.winner,
    ]
  );
}

// Returns a user's last `limit` flips already shaped exactly like the
// client's cfMyHistory entries ({opponent, myValue, oppValue, won,
// resultSide, timestamp, creator, joiner, creatorSide, joinerSide,
// creatorItems, joinerItems, youAreCreator}) - the "who's the opponent /
// did I win" transform happens here so the client doesn't need any new
// logic, just a place to drop the array in. Includes the full item lists
// and sides (not just the aggregate totals) so a flip loaded from here
// after a page refresh can still be rewatched via "View", the same as one
// that happened earlier in the current session.
async function loadCoinflipHistoryForUser(username, limit) {
  const res = await pool.query(
    `SELECT creator, joiner, creator_side, joiner_side, creator_items, joiner_items, creator_value, joiner_value, result, winner, resolved_at
     FROM coinflip_history WHERE creator = $1 OR joiner = $1
     ORDER BY resolved_at DESC LIMIT $2`,
    [username, limit]
  );
  return res.rows.map((r) => {
    const youAreCreator = r.creator === username;
    return {
      opponent: youAreCreator ? r.joiner : r.creator,
      myValue: youAreCreator ? Number(r.creator_value) : Number(r.joiner_value),
      oppValue: youAreCreator ? Number(r.joiner_value) : Number(r.creator_value),
      won: r.winner === username,
      resultSide: r.result,
      timestamp: new Date(r.resolved_at).getTime(),
      creator: r.creator,
      joiner: r.joiner,
      creatorSide: r.creator_side,
      joinerSide: r.joiner_side,
      creatorItems: typeof r.creator_items === 'string' ? JSON.parse(r.creator_items) : (r.creator_items || []),
      joinerItems: typeof r.joiner_items === 'string' ? JSON.parse(r.joiner_items) : (r.joiner_items || []),
      youAreCreator,
    };
  });
}

// Creates a notification, silently doing nothing if one already exists for
// this exact (username, type, ref_id) combo - see the UNIQUE constraint on
// the table. This is a safety net; the actual call sites in server.js are
// already exactly-once on their own (an atomic pending->resolved DB
// transition for withdrawals, a single-execution battle resolution for
// Case Battles), so this should never actually need to catch anything in
// practice, but costs nothing to have.
async function insertNotification({ username, type, title, message, refId }) {
  const res = await pool.query(
    `INSERT INTO notifications (username, type, title, message, ref_id)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (username, type, ref_id) DO NOTHING
     RETURNING id, username, type, title, message, ref_id, is_read, created_at`,
    [username, type, title, message, refId || null]
  );
  if (res.rows.length === 0) return null; // was a duplicate - nothing created
  const r = res.rows[0];
  return {
    id: r.id, username: r.username, type: r.type, title: r.title, message: r.message,
    refId: r.ref_id, read: r.is_read, createdAt: new Date(r.created_at).getTime(),
  };
}

// Recent notifications for one user, most recent first, capped at `limit` -
// see NOTIFICATION_LOAD_LIMIT in server.js. Always scoped to exactly the
// username passed in; there's no way to request another user's rows
// through this function.
async function loadNotificationsForUser(username, limit) {
  const res = await pool.query(
    `SELECT id, type, title, message, ref_id, is_read, created_at
     FROM notifications WHERE username = $1 ORDER BY created_at DESC LIMIT $2`,
    [username, limit]
  );
  return res.rows.map((r) => ({
    id: r.id, type: r.type, title: r.title, message: r.message,
    refId: r.ref_id, read: r.is_read, createdAt: new Date(r.created_at).getTime(),
  }));
}

// Marks exactly one notification read - the WHERE clause requires it to
// belong to `username`, so this can never touch another user's row no
// matter what id is passed in.
async function markNotificationRead(username, notificationId) {
  await pool.query(
    `UPDATE notifications SET is_read = true WHERE id = $1 AND username = $2`,
    [notificationId, username]
  );
}

async function markAllNotificationsRead(username) {
  await pool.query(
    `UPDATE notifications SET is_read = true WHERE username = $1 AND is_read = false`,
    [username]
  );
}

// Returns false if that Discord account is already linked to someone else.
async function setDiscordLink(username, discord) {
  try {
    await pool.query(
      `UPDATE users SET discord_id = $2, discord_username = $3, discord_avatar = $4, updated_at = now() WHERE username = $1`,
      [username, discord.id, discord.username, discord.avatar || null]
    );
    return true;
  } catch (err) {
    if (err.code === '23505') return false; // unique violation: linked to another player
    throw err;
  }
}
async function clearDiscordLink(username) {
  await pool.query(`UPDATE users SET discord_id = NULL, discord_username = NULL, discord_avatar = NULL WHERE username = $1`, [username]);
}
// ---------------------------------------------------------------------------
// Account history pages (always filtered to the requesting player).
// ---------------------------------------------------------------------------
async function loadGameHistory(username, game, limit, offset) {
  const params = [username];
  let where = 'username = $1';
  if (game) { params.push(game); where += ` AND game = $${params.length}`; }
  const total = Number((await pool.query(`SELECT COUNT(*)::bigint AS n FROM game_history WHERE ${where}`, params)).rows[0].n);
  params.push(limit, offset);
  const res = await pool.query(
    `SELECT game, wager, payout, created_at FROM game_history WHERE ${where}
     ORDER BY created_at DESC, id DESC LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
  return { total, rows: res.rows.map((r) => ({ game: r.game, wager: Number(r.wager), payout: Number(r.payout), at: r.created_at })) };
}

async function loadTransactionHistory(username, types, excludeTypes, limit, offset) {
  const params = [username];
  let where = 'username = $1';
  if (types && types.length) { params.push(types); where += ` AND type = ANY($${params.length})`; }
  if (excludeTypes && excludeTypes.length) { params.push(excludeTypes); where += ` AND NOT (type = ANY($${params.length}))`; }
  const total = Number((await pool.query(`SELECT COUNT(*)::bigint AS n FROM transactions WHERE ${where}`, params)).rows[0].n);
  params.push(limit, offset);
  const res = await pool.query(
    `SELECT type, amount, value, items_delta, reason, created_at FROM transactions WHERE ${where}
     ORDER BY created_at DESC, id DESC LIMIT $${params.length - 1} OFFSET $${params.length}`, params);
  return { total, rows: res.rows.map((r) => ({
    type: r.type, amount: Number(r.amount), value: r.value == null ? null : Number(r.value),
    items: Array.isArray(r.items_delta) ? r.items_delta : [], reason: r.reason, at: r.created_at,
  })) };
}

// ---------------------------------------------------------------------------
// Promo codes
// ---------------------------------------------------------------------------
async function createPromoCode({ code, coins, maxUses, expiresAt, createdBy }) {
  const res = await pool.query(
    `INSERT INTO promo_codes (code, coins, max_uses, expires_at, created_by) VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (code) DO NOTHING RETURNING code`,
    [code, coins, maxUses, expiresAt ? new Date(expiresAt) : null, createdBy]
  );
  return res.rowCount === 1;
}
async function listPromoCodes() {
  const res = await pool.query(`SELECT code, coins, max_uses, uses, expires_at, active, created_at FROM promo_codes ORDER BY created_at DESC LIMIT 100`);
  return res.rows.map((r) => ({ code: r.code, coins: Number(r.coins), maxUses: r.max_uses, uses: r.uses, expiresAt: r.expires_at ? new Date(r.expires_at).getTime() : null, active: r.active, createdAt: new Date(r.created_at).getTime() }));
}
async function setPromoActive(code, active) {
  await pool.query(`UPDATE promo_codes SET active = $2 WHERE code = $1`, [code, !!active]);
}
// One atomic check-and-claim: the code must exist, be active, not expired,
// have uses left, and this player must not have used it before.
async function redeemPromoCode(code, username) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(`SELECT coins, max_uses, uses, expires_at, active FROM promo_codes WHERE code = $1 FOR UPDATE`, [code]);
    if (!r.rows.length || !r.rows[0].active) { await client.query('ROLLBACK'); return { error: "That code doesn't exist." }; }
    const row = r.rows[0];
    if (row.expires_at && new Date(row.expires_at).getTime() < Date.now()) { await client.query('ROLLBACK'); return { error: 'That code has expired.' }; }
    if (row.uses >= row.max_uses) { await client.query('ROLLBACK'); return { error: 'That code has been fully used up.' }; }
    const ins = await client.query(`INSERT INTO promo_redemptions (code, username) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [code, username]);
    if (ins.rowCount !== 1) { await client.query('ROLLBACK'); return { error: "You've already used that code." }; }
    await client.query(`UPDATE promo_codes SET uses = uses + 1 WHERE code = $1`, [code]);
    await client.query('COMMIT');
    return { ok: true, coins: Number(row.coins) };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// FULL RESET (admin "delete everything", changing games): empties EVERY
// table - accounts, pets, coins, vaults, stats, all history, notifications,
// promo codes, ignore lists, server state. One statement, so it either all
// happens or nothing does. There is no backup.
async function wipeEverything() {
  await pool.query(`TRUNCATE TABLE
    community_cases, promo_redemptions, promo_codes, game_history, ignored_users, server_state, shop_stock, escrows,
    transactions, notifications, coinflip_history, coinflip_lobbies, withdraw_requests,
    vault_items, inventory_items, users
    RESTART IDENTITY CASCADE`);
}

// ---- community cases ----
async function loadCommunityCases() {
  const res = await pool.query('SELECT id, creator, name, image, commission, price, items, opens, earned, unclaimed, active, created_at FROM community_cases ORDER BY id');
  return res.rows.map((r) => ({ id: String(r.id), creator: r.creator, name: r.name, image: r.image, commission: Number(r.commission), price: Number(r.price), items: r.items, opens: Number(r.opens), earned: Number(r.earned), unclaimed: Number(r.unclaimed), active: r.active !== false, createdAt: new Date(r.created_at).getTime() }));
}
async function insertCommunityCase({ creator, name, image, commission, price, items }) {
  const res = await pool.query(
    'INSERT INTO community_cases (creator, name, image, commission, price, items) VALUES ($1,$2,$3,$4,$5,$6::jsonb) RETURNING id, created_at',
    [creator, name, image, commission, price, JSON.stringify(items)]
  );
  return { id: String(res.rows[0].id), createdAt: new Date(res.rows[0].created_at).getTime() };
}
async function deleteCommunityCase(id) {
  const res = await pool.query('DELETE FROM community_cases WHERE id = $1', [id]);
  return res.rowCount === 1;
}
async function bumpCommunityCase(id, earned, opens = 1) {
  await pool.query('UPDATE community_cases SET opens = opens + $3, earned = earned + $2, unclaimed = unclaimed + $2 WHERE id = $1', [id, earned, opens]);
}
async function setCommunityCaseActive(id, active) {
  const res = await pool.query('UPDATE community_cases SET active = $2 WHERE id = $1', [id, !!active]);
  return res.rowCount === 1;
}
// Zeroes every unclaimed commission for this creator in one statement and
// returns how much that was - the database is the source of truth here.
async function claimCommunityEarnings(creator) {
  const res = await pool.query(
    `WITH old AS (SELECT id, unclaimed FROM community_cases WHERE creator = $1 AND unclaimed > 0 FOR UPDATE)
     UPDATE community_cases c SET unclaimed = 0 FROM old WHERE c.id = old.id RETURNING old.unclaimed`, [creator]);
  return res.rows.reduce((sum, r) => sum + Number(r.unclaimed), 0);
}

async function loadAllIgnores() {
  const res = await pool.query(`SELECT username, ignored FROM ignored_users`);
  const map = new Map();
  for (const r of res.rows) {
    if (!map.has(r.username)) map.set(r.username, new Set());
    map.get(r.username).add(r.ignored);
  }
  return map;
}
async function addIgnore(username, ignored) {
  await pool.query(`INSERT INTO ignored_users (username, ignored) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [username, ignored]);
}
async function removeIgnore(username, ignored) {
  await pool.query(`DELETE FROM ignored_users WHERE username = $1 AND LOWER(ignored) = LOWER($2)`, [username, ignored]);
}

async function loadEscrows() {
  const res = await pool.query(`SELECT id, username, kind, items, coins FROM escrows ORDER BY created_at ASC`);
  return res.rows.map((r) => ({ id: r.id, username: r.username, kind: r.kind, items: Array.isArray(r.items) ? r.items : [], coins: Number(r.coins) }));
}

async function loadShopStock() {
  const res = await pool.query(`SELECT catalog_id, qty FROM shop_stock WHERE qty > 0`);
  const out = {};
  for (const r of res.rows) out[r.catalog_id] = r.qty;
  return out;
}

// Writes absolute quantities for the given catalog ids.
async function saveShopStock(entries) {
  const ids = Object.keys(entries);
  if (!ids.length) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const id of ids) {
      await client.query(
        `INSERT INTO shop_stock (catalog_id, qty) VALUES ($1,$2) ON CONFLICT (catalog_id) DO UPDATE SET qty = $2`,
        [id, Math.max(0, Math.trunc(entries[id] || 0))]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function loadServerState(key) {
  const res = await pool.query(`SELECT value FROM server_state WHERE key = $1`, [key]);
  return res.rows.length ? res.rows[0].value : null;
}

async function saveServerState(key, value) {
  await pool.query(
    `INSERT INTO server_state (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = $2`,
    [key, JSON.stringify(value)]
  );
}

module.exports = {
  loadCommunityCases,
  insertCommunityCase,
  deleteCommunityCase,
  bumpCommunityCase,
  setCommunityCaseActive,
  claimCommunityEarnings,
  pool,
  setDiscordLink,
  clearDiscordLink,
  loadAllIgnores,
  wipeEverything,
  createPromoCode,
  listPromoCodes,
  setPromoActive,
  redeemPromoCode,
  loadGameHistory,
  loadTransactionHistory,
  addIgnore,
  removeIgnore,
  loadEscrows,
  loadShopStock,
  saveShopStock,
  loadServerState,
  saveServerState,
  initSchema,
  loadAllUsers,
  loadPendingWithdrawRequests,
  loadPendingCoinflipLobbies,
  insertNewUser,
  persistUserState,
  insertWithdrawRequest,
  resolveWithdrawRequest,
  insertCoinflipLobby,
  deleteCoinflipLobby,
  insertCoinflipHistory,
  loadCoinflipHistoryForUser,
  insertNotification,
  loadNotificationsForUser,
  markNotificationRead,
  markAllNotificationsRead,
  findDuplicateCaseClusters,
  mergeCasedDuplicates,
  loadRecentTransactionsForUser,
};
