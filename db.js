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

  console.log('[db] Schema ready.');
}

// ---------------------------------------------------------------------------
// Boot-time loads - populate server.js's in-memory Map/array from Postgres
// before the server starts accepting connections.
// ---------------------------------------------------------------------------

// Returns a Map in the exact shape server.js's `users` Map already expects:
// username -> { coins, inventory: {catalogId: qty}, stats: {wagered, won, lost} }
async function loadAllUsers() {
  const usersRes = await pool.query('SELECT username, coins, stats_wagered, stats_won, stats_lost FROM users');
  const invRes = await pool.query('SELECT username, catalog_id, qty FROM inventory_items WHERE qty > 0');

  const map = new Map();
  for (const row of usersRes.rows) {
    map.set(row.username, {
      coins: Number(row.coins),
      inventory: {},
      stats: { wagered: Number(row.stats_wagered), won: Number(row.stats_won), lost: Number(row.stats_lost) },
    });
  }
  for (const row of invRes.rows) {
    const u = map.get(row.username);
    if (u) u.inventory[row.catalog_id] = row.qty;
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

    const upd = await client.query(
      `UPDATE users SET coins = $2, stats_wagered = $3, stats_won = $4, stats_lost = $5, updated_at = now()
       WHERE username = $1`,
      [username, user.coins, user.stats.wagered, user.stats.won, user.stats.lost]
    );
    if (upd.rowCount === 0) {
      await client.query(
        `INSERT INTO users (username, coins, stats_wagered, stats_won, stats_lost) VALUES ($1,$2,$3,$4,$5)`,
        [username, user.coins, user.stats.wagered, user.stats.won, user.stats.lost]
      );
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

    if (txn) {
      await client.query(
        `INSERT INTO transactions (username, type, amount, balance_before, balance_after, items_delta, reason, admin_username)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          username,
          txn.type,
          Math.trunc(txn.amount || 0),
          Math.trunc(txn.balanceBefore != null ? txn.balanceBefore : user.coins),
          Math.trunc(txn.balanceAfter != null ? txn.balanceAfter : user.coins),
          txn.itemsTouched && txn.itemsTouched.length ? JSON.stringify(txn.itemsTouched) : null,
          txn.reason || null,
          txn.adminUsername || null,
        ]
      );
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

module.exports = {
  pool,
  initSchema,
  loadAllUsers,
  loadPendingWithdrawRequests,
  insertNewUser,
  persistUserState,
  insertWithdrawRequest,
  resolveWithdrawRequest,
};
