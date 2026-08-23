// reset_all_accounts.js
//
// One-off cleanup script: wipes every NORMAL account's coins back to 0,
// clears their pet inventory, and zeroes out wagered/won/lost stats (which
// is what the leaderboard is computed directly from - see
// handleLeaderboardRequest in server.js - so this also clears it).
//
// DTN_BGSI (the hand-out/giveaway + trading account, see GIVEAWAY_USERNAME
// in server.js) is deliberately EXCLUDED from all of this, case-insensitive.
// Its inventory self-heals back to 99x of every pet on its own (see
// ensureUser's "Self-healing top-up" block), but its coin balance does NOT -
// coins are only ever seeded once, at the account's original creation, on
// purpose (so admin balance adjustments actually stick afterward rather than
// being silently undone). A blanket reset would zero that balance with no
// automatic way back, since the account already exists and wouldn't be
// treated as "new" again. Excluding it here avoids that entirely.
//
// Nothing else is touched - coinflip history, feed events, withdraw
// requests, etc. all stay exactly as they are for every account, DTN_BGSI
// included. Run this once against your live database, then restart the
// server so its in-memory `users` Map picks up the fresh state (the server
// only reads accounts from Postgres at boot - running this script alone
// won't change what a currently-running server has in memory).
//
// Usage (from wherever this file lives on your server, with DATABASE_URL
// already set in the environment - same variable server.js/db.js already
// require):
//
//   node reset_all_accounts.js
//
// This is IRREVERSIBLE for the accounts it does touch. There's no
// confirmation prompt on purpose (so it can be scripted/piped), so make
// sure you actually mean to run this against your real database before you
// do.

const { Pool } = require('pg');

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is not set. Run this with the same environment your server uses.');
  process.exit(1);
}

// Must match GIVEAWAY_USERNAME in server.js exactly (case doesn't matter -
// both queries below compare case-insensitively).
const EXCLUDED_USERNAME = 'DTN_BGSI';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined,
});

(async () => {
  try {
    const { rowCount: userCount } = await pool.query(
      `UPDATE users SET coins = 0, stats_wagered = 0, stats_won = 0, stats_lost = 0, updated_at = now()
       WHERE LOWER(username) <> LOWER($1)`,
      [EXCLUDED_USERNAME]
    );
    const { rowCount: itemRowCount } = await pool.query(
      `DELETE FROM inventory_items WHERE LOWER(username) <> LOWER($1)`,
      [EXCLUDED_USERNAME]
    );

    console.log(`Reset coins and stats (wagered/won/lost) for ${userCount} account(s).`);
    console.log(`Deleted ${itemRowCount} inventory row(s), excluding ${EXCLUDED_USERNAME}.`);
    console.log(`${EXCLUDED_USERNAME} was left completely untouched.`);
    console.log('Done. Restart the server now so it reloads this clean state into memory.');
  } catch (err) {
    console.error('Reset failed:', err.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
