
const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const db = require('./db');

const gameData = JSON.parse(fs.readFileSync(path.join(__dirname, 'gameData.json'), 'utf8'));
const { petCatalog } = gameData;

const STARTING_BALANCE = 0; // normal accounts start with nothing - no free coins on creation

// Site owner - the only account allowed to use the admin panel.
const ADMIN_USERNAME = 'tim_tim1345';

// Hand-out/giveaway account - not an admin, just a stocked account used to
// tip pets to players (see handleTipSend). Pre-loaded on first creation
// with one of every pet and a large coin balance so it never runs dry.
const GIVEAWAY_USERNAME = 'DTN_BGSI';
const GIVEAWAY_STARTING_BALANCE = 2000000000;

// A plain ledger account (not an admin, no special seeding/inventory like
// DTN_BGSI has) that the admin funds/drains manually via the coin
// management tool - just needs a distinct chat role color to be visually
// identifiable, same as the other two.
const STOCK_USERNAME = 'BloxyVault_Stock';

// Gates every testing-only, no-real-verification path in this file:
// /dev/skip-login (logs in as anyone with zero Roblox check) and
// account:reset. Both exist purely for local development and are OFF by
// default - Railway doesn't reliably set NODE_ENV on its own, so this
// deliberately requires an explicit opt-in rather than trusting an
// environment flag that might not be set. Only add ENABLE_DEV_BYPASS=true
// in Railway's Variables tab if you actually want these reachable (e.g. a
// separate staging deploy) - never on the real one.
// (debug:addTestCoins used to live here too - fully removed, not just
// gated, since normal users must have no path to free coins at all now.)
const ENABLE_DEV_BYPASS = process.env.ENABLE_DEV_BYPASS === 'true';
if (ENABLE_DEV_BYPASS) {
  console.warn('[security] ENABLE_DEV_BYPASS is ON - /dev/skip-login and account:reset are reachable. Do not leave this on in production.');
}

// Roblox OAuth 2.0 ("Sign in with Roblox") - see the /roblox/oauth/login
// and /roblox/oauth/callback routes below for the actual flow. All three
// of these must be set in Railway's Variables tab for it to work; if any
// are missing, those two routes fail cleanly instead of crashing.
const ROBLOX_OAUTH_CLIENT_ID = process.env.ROBLOX_OAUTH_CLIENT_ID || '';
const ROBLOX_OAUTH_CLIENT_SECRET = process.env.ROBLOX_OAUTH_CLIENT_SECRET || '';
// This server's own real public URL, no trailing slash (e.g.
// https://your-app.up.railway.app) - must exactly match (plus
// '/roblox/oauth/callback') whatever Redirect URL is registered in
// Roblox's OAuth app settings.
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');

// Discord account linking (OAuth2, "identify" scope only - we only ever read
// the Discord user id, username and avatar). Set DISCORD_CLIENT_ID and
// DISCORD_CLIENT_SECRET in Railway, and add
// PUBLIC_BASE_URL + '/discord/oauth/callback' as a Redirect in the Discord
// Developer Portal (OAuth2 tab). Until then the button explains it isn't set up.
const DISCORD_CLIENT_ID = process.env.DISCORD_CLIENT_ID || '';
const DISCORD_CLIENT_SECRET = process.env.DISCORD_CLIENT_SECRET || '';
const pendingDiscordStates = new Map(); // state -> { username, expires }
function discordConfigured() { return !!(DISCORD_CLIENT_ID && DISCORD_CLIENT_SECRET && PUBLIC_BASE_URL); }
function discordPublic(d) {
  if (!d || !d.id) return null;
  let avatarUrl;
  if (d.avatar) avatarUrl = `https://cdn.discordapp.com/avatars/${d.id}/${d.avatar}.${String(d.avatar).startsWith('a_') ? 'gif' : 'png'}?size=128`;
  else {
    let idx = 0; try { idx = Number((BigInt(d.id) >> 22n) % 6n); } catch (e) {}
    avatarUrl = `https://cdn.discordapp.com/embed/avatars/${idx}.png`;
  }
  return { username: d.username, avatarUrl };
}

// Chat ignore lists: username -> Set of ignored usernames (persisted).
let ignoreLists = new Map();
const MAX_IGNORES = 200;
function ignoredListFor(username) { return [...(ignoreLists.get(username) || [])].sort((a, b) => a.localeCompare(b)); }
if (!ROBLOX_OAUTH_CLIENT_ID || !ROBLOX_OAUTH_CLIENT_SECRET || !PUBLIC_BASE_URL) {
  console.warn('[roblox-oauth] Not fully configured (need ROBLOX_OAUTH_CLIENT_ID, ROBLOX_OAUTH_CLIENT_SECRET, PUBLIC_BASE_URL) - "Log in with Roblox" will not work until all three are set.');
}
// state -> expiry timestamp. One-time-use CSRF nonces for the OAuth
// redirect flow - see /roblox/oauth/login and /roblox/oauth/callback.
const pendingOAuthStates = new Map();

const AVATAR_CACHE_TTL_MS = 30 * 60 * 1000;
const avatarCache = new Map(); // username(lowercase) -> {imageUrl, userId, cachedAt}

const NOTIFICATION_LOAD_LIMIT = 50; // matches the client's own history cap

async function createNotification({ username, type, title, message, refId }) {
  let notif;
  try {
    notif = await db.insertNotification({ username, type, title, message, refId });
  } catch (err) {
    console.error(`[notifications] Failed to persist notification for ${username}:`, err.message);
    return;
  }
  if (!notif) return; // duplicate - already existed, nothing to push
  const ws = usernameToSocket.get(username);
  if (ws) send(ws, { type: 'notifications:new', notification: notif });
}

// ---------------------------------------------------------------------------
// Connection + account state
// ---------------------------------------------------------------------------
const users = new Map();          // username -> { coins, inventory: {catalogId: qty} }

// The Exchange's "coin to item" side now draws from real player-sold stock
// instead of minting any pet on demand. Starts completely empty - a pet
// only becomes buyable once someone actually sells one into it via
// handleExchangeSell. Shared across all players (not per-user).
const shopStock = {}; // catalogId -> qty available to buy
const socketToUsername = new Map(); // ws -> username
const usernameToSocket = new Map(); // username -> ws (most recent connection)

// ---------------------------------------------------------------------------
// Session tokens - proves a 'login' message actually came from someone who
// passed Roblox bio verification (or the dev skip-login bypass below), not
// just anyone who knows/guesses a username. In-memory like everything else
// here, so it resets on restart along with balances/inventory - swap for a
// real database if you need sessions to survive deploys.
// ---------------------------------------------------------------------------
const sessionTokens = new Map(); // token -> { username, expires }
const SESSION_TOKEN_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days, matches client-side persistence

function issueSessionToken(username) {
  const token = crypto.randomBytes(24).toString('hex');
  sessionTokens.set(token, { username, expires: Date.now() + SESSION_TOKEN_TTL_MS });
  return token;
}

function checkSessionToken(username, token) {
  if (!token) return false;
  const rec = sessionTokens.get(token);
  if (!rec) return false;
  if (rec.username !== username) return false;
  if (Date.now() > rec.expires) { sessionTokens.delete(token); return false; }
  return true;
}

// periodic cleanup so this map doesn't grow forever
setInterval(() => {
  const now = Date.now();
  for (const [token, rec] of sessionTokens) {
    if (now > rec.expires) sessionTokens.delete(token);
  }
}, 60 * 60 * 1000);

// Roblox usernames: 3-20 chars, letters/digits/underscore. Anything a client
// types as a *target* name (tips, /mute, profile lookups, role grants) must
// pass this before it can create a DB row or be echoed into chat - stops
// both phantom-account spam and HTML/script injection via usernames.
const ROBLOX_USERNAME_RE = /^[A-Za-z0-9_]{3,20}$/;
function isValidUsername(name) {
  return typeof name === 'string' && ROBLOX_USERNAME_RE.test(name);
}

function ensureUser(username) {
  const isNewAccount = !users.has(username);
  const isGiveaway = username.toLowerCase() === GIVEAWAY_USERNAME.toLowerCase();

  if (isNewAccount) {
    const startingCoins = isGiveaway ? GIVEAWAY_STARTING_BALANCE : STARTING_BALANCE;
    const startingInventory = {};
    if (isGiveaway) for (const id of Object.keys(petCatalog)) startingInventory[id] = 99;
    users.set(username, { coins: startingCoins, inventory: startingInventory, stats: { xp: 0, won: 0, lost: 0 }, specialRole: null, mutedUntil: 0 });
    if (isGiveaway) {
      // Seed both the balance AND the full inventory atomically in one go,
      // with a real transaction row - this only ever happens once, right
      // at creation, not on every touch (see below for why that matters).
      persistUser(username, { type: 'giveaway_seed', amount: startingCoins, balanceBefore: 0, balanceAfter: startingCoins, itemsTouched: Object.keys(startingInventory), reason: 'Initial giveaway account setup' });
    } else {
      // Fire-and-forget: this username has never been seen before in this
      // process. It's already usable in memory immediately (no need to wait
      // on the DB round-trip before the player can act) - Postgres just
      // needs to catch up with a matching row.
      db.insertNewUser(username, startingCoins, { xp: 0, won: 0, lost: 0 }).catch((err) => {
        console.error(`[db] insertNewUser failed for ${username}:`, err.message);
      });
    }
  }
  const u = users.get(username);
  if (!u.stats) u.stats = { xp: 0, won: 0, lost: 0 }; // backfill for accounts created before stats existed
  if (u.specialRole === undefined) u.specialRole = null; // backfill for accounts created before manually-grantable roles existed
  if (u.mutedUntil === undefined) u.mutedUntil = 0; // backfill for accounts created before chat muting existed
  if (!u.vault) u.vault = { coins: 0, items: {}, lockUntil: 0 };
  if (isGiveaway && !isNewAccount) {
    // Keep this hand-out account's INVENTORY topped up on every touch (not
    // just at creation) - covers accounts that already existed in memory
    // before this feature was added, and any pets added to the catalog
    // later (they'll get topped up to 99x automatically too). Only the
    // items that actually needed correcting get persisted - once stable at
    // 99x, later calls change nothing and skip the DB write entirely.
    //
    // Coins are deliberately NOT re-floored here anymore (unlike the
    // inventory). They used to be, but that meant an admin's "Remove
    // Coins" action on this account got silently undone within the same
    // request - syncAccount() calls ensureUser() again right after the
    // deduction, which would see the now-lower balance and reset it straight
    // back up before the response even went out. Coins are only ever
    // seeded once now, at creation, same as a normal account - after that
    // they behave like any other balance and admin adjustments actually stick.
    const toppedUp = [];
    for (const id of Object.keys(petCatalog)) {
      if (!u.inventory[id] || u.inventory[id] < 99) { u.inventory[id] = 99; toppedUp.push(id); }
    }
    if (toppedUp.length) {
      persistUser(username, { type: 'giveaway_topup', amount: 0, balanceBefore: u.coins, balanceAfter: u.coins, itemsTouched: toppedUp, reason: 'Self-healing top-up to 99x' });
    }
  }
  return u;
}
// Resolves a manually-typed username against real, already-known accounts
// case-insensitively - e.g. admin types "dtn_bgsi" but the account is
// actually stored as "DTN_BGSI" (its real Roblox-canonical casing). Used
// anywhere a human free-types someone else's name rather than it coming
// from their own verified login. Returns the real stored key if a match
// exists, otherwise null - callers decide what "no match" means for them
// (admin lookups should say "not found"; tipping may still want to create
// a fresh account for a genuinely new username).
function resolveExistingUsername(raw) {
  const target = String(raw || '').trim();
  if (!target) return null;
  if (users.has(target)) return target;
  const lower = target.toLowerCase();
  for (const key of users.keys()) {
    if (key.toLowerCase() === lower) return key;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Level curve - mirrors the client's exact formula (see xpForLevel /
// levelForXp in the frontend) so a player's level, and therefore their
// Rain Pool share, is computed identically on both sides.
// ---------------------------------------------------------------------------
const LEVEL_CURVE_C = 446161.656; // 5x easier than the original 2230808.28
const LEVEL_CURVE_K = 1.353373;
const MAX_LEVEL = 500;
function levelForXp(xp) {
  const w = Math.max(0, xp || 0);
  if (w <= 0) return 1;
  const lvl = Math.floor(Math.pow(w / LEVEL_CURVE_C, 1 / LEVEL_CURVE_K)) + 1;
  return Math.max(1, Math.min(MAX_LEVEL, lvl));
}

// ---------------------------------------------------------------------------
// Discord level roles
// The linking app's bot gives players a role in the Discord server that
// matches their site level. Settings (Railway -> Variables):
//   DISCORD_BOT_TOKEN    the bot's token (Developer Portal -> your app -> Bot)
//   DISCORD_GUILD_ID     your server's ID
//   DISCORD_LEVEL_ROLES  level:roleId pairs, e.g. 1:111,10:222,25:333,50:444,75:555,100:666
//   DISCORD_STICKY_LEVELS (optional, default "1") levels whose role is kept forever
// Rules: a "sticky" role (Level 1) is kept once earned. Of the other level
// roles a player only ever has the HIGHEST one they've reached - lower ones
// are removed. Roles that aren't level roles are never touched.
// ---------------------------------------------------------------------------
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || '';
const DISCORD_API_BASE = process.env.DISCORD_API_BASE || 'https://discord.com/api/v10'; // only overridden for local testing
const DISCORD_GUILD_ID = (process.env.DISCORD_GUILD_ID || '').trim();
const DISCORD_LEVEL_ROLES = String(process.env.DISCORD_LEVEL_ROLES || '')
  .split(/[,\s]+/).map((pair) => pair.split(':').map((x) => x.trim()))
  .filter(([lvl, role]) => /^\d+$/.test(lvl || '') && /^\d{5,25}$/.test(role || ''))
  .map(([lvl, role]) => ({ level: Number(lvl), roleId: role }))
  .sort((a, b) => a.level - b.level);
const DISCORD_STICKY_LEVELS = new Set(String(process.env.DISCORD_STICKY_LEVELS || '1').split(/[,\s]+/).filter(Boolean).map(Number));
function discordRolesConfigured() { return !!(DISCORD_BOT_TOKEN && DISCORD_GUILD_ID && DISCORD_LEVEL_ROLES.length); }
if (!discordRolesConfigured() && (DISCORD_BOT_TOKEN || DISCORD_GUILD_ID || process.env.DISCORD_LEVEL_ROLES)) {
  console.warn('[discord-roles] Partly configured - need DISCORD_BOT_TOKEN, DISCORD_GUILD_ID and DISCORD_LEVEL_ROLES (e.g. 1:123,10:456). Level roles are OFF.');
}

// Which level-role ids someone at `level` should have.
function wantedLevelRoleIds(level) {
  const want = new Set();
  let best = null;
  for (const r of DISCORD_LEVEL_ROLES) {
    if (r.level > level) continue;
    if (DISCORD_STICKY_LEVELS.has(r.level)) want.add(r.roleId);
    else if (!best || r.level > best.level) best = r;
  }
  if (best) want.add(best.roleId);
  return want;
}

// One request at a time, respecting Discord's rate limits (429 retry_after).
let discordQueue = Promise.resolve();
function discordApi(method, path, body) {
  const run = async () => {
    for (let attempt = 0; attempt < 4; attempt++) {
      const headers = { Authorization: `Bot ${DISCORD_BOT_TOKEN}`, 'X-Audit-Log-Reason': 'BloxyVault' };
      if (body !== undefined) headers['Content-Type'] = 'application/json';
      const res = await fetch(`${DISCORD_API_BASE}${path}`, { method, headers, body: body !== undefined ? JSON.stringify(body) : undefined });
      if (res.status === 429) {
        const body = await res.json().catch(() => ({}));
        await new Promise((r) => setTimeout(r, Math.ceil((Number(body.retry_after) || 1) * 1000) + 100));
        continue;
      }
      let data = null;
      if (res.status !== 204) data = await res.json().catch(() => null);
      return { status: res.status, data };
    }
    return { status: 429, data: null };
  };
  const p = discordQueue.then(run, run);
  discordQueue = p.catch(() => {});
  return p;
}

const discordRoleSyncedLevel = new Map(); // username -> level we last successfully synced
const discordRoleSyncTimers = new Map();
const discordLoginCheckAt = new Map();
function scheduleDiscordRoleSync(username, delayMs = 1500) {
  if (!discordRolesConfigured()) return;
  const u = users.get(username);
  if (!u || !u.discord || !u.discord.id) return;
  if (discordRoleSyncTimers.has(username)) clearTimeout(discordRoleSyncTimers.get(username));
  discordRoleSyncTimers.set(username, setTimeout(() => {
    discordRoleSyncTimers.delete(username);
    syncDiscordLevelRoles(username).catch((err) => console.error(`[discord-roles] sync failed for ${username}:`, err.message));
  }, delayMs));
}

async function syncDiscordLevelRoles(username, { force = false } = {}) {
  if (!discordRolesConfigured()) return 'off';
  const u = users.get(username);
  if (!u || !u.discord || !u.discord.id) return 'not-linked';
  const level = levelForXp(u.stats.xp);
  if (!force && discordRoleSyncedLevel.get(username) === level) return 'unchanged';
  const discordId = u.discord.id;
  const member = await discordApi('GET', `/guilds/${DISCORD_GUILD_ID}/members/${discordId}`);
  if (member.status === 404) return 'not-in-server'; // will retry next time their level changes / they log in
  if (member.status !== 200 || !member.data) { console.warn(`[discord-roles] couldn't read member ${discordId} (${member.status})`); return 'error'; }
  const has = new Set(member.data.roles || []);
  const want = wantedLevelRoleIds(level);
  const levelRoleIds = new Set(DISCORD_LEVEL_ROLES.map((r) => r.roleId));
  for (const roleId of want) {
    if (has.has(roleId)) continue;
    const r = await discordApi('PUT', `/guilds/${DISCORD_GUILD_ID}/members/${discordId}/roles/${roleId}`);
    if (r.status !== 204) console.warn(`[discord-roles] add role ${roleId} to ${username} failed (${r.status}) - is the bot's role above the level roles and does it have Manage Roles?`);
  }
  for (const roleId of has) {
    if (!levelRoleIds.has(roleId) || want.has(roleId)) continue;
    // Sticky roles are never taken away once earned.
    const def = DISCORD_LEVEL_ROLES.find((r) => r.roleId === roleId);
    if (def && DISCORD_STICKY_LEVELS.has(def.level)) continue;
    const r = await discordApi('DELETE', `/guilds/${DISCORD_GUILD_ID}/members/${discordId}/roles/${roleId}`);
    if (r.status !== 204) console.warn(`[discord-roles] remove role ${roleId} from ${username} failed (${r.status})`);
  }
  discordRoleSyncedLevel.set(username, level);
  return 'synced';
}

// Removes every level role (used when someone unlinks their Discord).
async function clearDiscordLevelRoles(discordId) {
  if (!discordRolesConfigured() || !discordId) return;
  for (const { roleId } of DISCORD_LEVEL_ROLES) {
    await discordApi('DELETE', `/guilds/${DISCORD_GUILD_ID}/members/${discordId}/roles/${roleId}`);
  }
}

// ---------------------------------------------------------------------------
// Rain Pool - one shared, server-timed pool everyone sees the same countdown
// for (driven by an absolute phaseEndsAt timestamp broadcast to clients, not
// a per-client local timer). Counts down RAIN_COUNTDOWN_MS, then opens a
// RAIN_CLAIM_WINDOW_MS claim window. Clicking "claim" during that window
// doesn't pay out immediately - it just marks you down as a claimant, so the
// button can show "Claimed" until the window closes. When the window closes,
// the pool is split once among everyone who claimed, weighted by level (a
// higher-level player gets a bigger slice), so the total paid out is always
// exactly the pool amount no matter how many people claimed - never one full
// 25k per person.
// ---------------------------------------------------------------------------
const RAIN_BASE = 25000;
const RAIN_COUNTDOWN_MS = 25 * 60 * 1000;
const RAIN_CLAIM_WINDOW_MS = 5 * 60 * 1000;

let rainPool = RAIN_BASE;
let rainPhase = 'counting'; // 'counting' | 'claimable'
let rainPhaseEndsAt = Date.now() + RAIN_COUNTDOWN_MS;
let rainClaimants = new Set(); // usernames who've claimed this cycle, cleared each cycle

function rainPublicState() {
  return { pool: rainPool, phase: rainPhase, phaseEndsAt: rainPhaseEndsAt, claimants: Array.from(rainClaimants) };
}
function broadcastRainState() {
  broadcast({ type: 'rain:state', ...rainPublicState() });
}

function payoutRain() {
  if (rainClaimants.size > 0) {
    const weights = new Map();
    let totalWeight = 0;
    for (const username of rainClaimants) {
      const w = levelForXp(ensureUser(username).stats.xp);
      weights.set(username, w);
      totalWeight += w;
    }
    for (const username of rainClaimants) {
      const share = Math.floor(rainPool * (weights.get(username) / totalWeight));
      if (share > 0) {
        const u = ensureUser(username);
        const before = u.coins;
        u.coins += share;
        persistUser(username, { type: 'rain_payout', amount: share, balanceBefore: before, balanceAfter: u.coins });
        syncAccount(username);
        send(usernameToSocket.get(username), { type: 'rain:payout', amount: share });
      }
    }
  }
  rainPool = RAIN_BASE;
  persistRainPool();
  rainPhase = 'counting';
  rainPhaseEndsAt = Date.now() + RAIN_COUNTDOWN_MS;
  rainClaimants = new Set();
  broadcastRainState();
}

function handleRainDeposit(username, msg) {
  const amount = Math.floor(Number(msg.amount));
  if (!amount || amount < 1) return;
  const u = ensureUser(username);
  if (u.coins < amount) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't have enough coins for that." });
  }
  const before = u.coins;
  u.coins -= amount;
  rainPool += amount;
  persistUser(username, { type: 'rain_deposit', amount: -amount, balanceBefore: before, balanceAfter: u.coins });
  persistRainPool();
  syncAccount(username);
  broadcastRainState();
}

function handleRainClaim(username) {
  if (rainPhase !== 'claimable') return;
  if (rainClaimants.has(username)) return; // already claimed this cycle
  rainClaimants.add(username);
  broadcastRainState(); // lets every client (including this one) know it's claimed
}

setInterval(() => {
  const now = Date.now();
  if (now < rainPhaseEndsAt) return;
  if (rainPhase === 'counting') {
    rainPhase = 'claimable';
    rainPhaseEndsAt = now + RAIN_CLAIM_WINDOW_MS;
    rainClaimants = new Set();
    broadcastRainState();
  } else {
    payoutRain();
  }
}, 1000);

function itemValue(id) {
  const p = petCatalog[id];
  return p ? p.value : 0;
}

function itemsValue(items) {
  return items.reduce((sum, id) => sum + itemValue(id), 0);
}

function ownsAll(username, items) {
  const inv = ensureUser(username).inventory;
  const locked = lockedItemsForUser(username);
  const need = {};
  for (const id of items) need[id] = (need[id] || 0) + 1;
  return Object.entries(need).every(([id, qty]) => ((inv[id] || 0) - (locked[id] || 0)) >= qty);
}
function lockedItemsForUser(username) {
  const locked = {};
  for (const req of withdrawRequests) {
    if (req.username !== username || req.status !== 'pending' || req.kind === 'deposit') continue;
    for (const id of req.items) locked[id] = (locked[id] || 0) + 1;
  }
  return locked;
}
function removeItems(username, items) {
  const inv = ensureUser(username).inventory;
  for (const id of items) inv[id] = (inv[id] || 0) - 1;
}
function addItems(username, items) {
  const inv = ensureUser(username).inventory;
  for (const id of items) inv[id] = (inv[id] || 0) + 1;
}

function send(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN) ws.send(data);
  }
}

const CHAT_HISTORY_MAX = 100;
let chatHistory = [];

function pushChatMessage(entry) {
  chatHistory.push(entry);
  if (chatHistory.length > CHAT_HISTORY_MAX) chatHistory.shift();
  broadcast({ type: 'chat:message', ...entry });
}

function syncAccount(username) {
  const ws = usernameToSocket.get(username);
  if (!ws) return; // not currently connected - they'll get fresh state on next login
  const u = ensureUser(username);
  const myPendingWithdrawals = myPendingRequests(username);
  send(ws, {
    type: 'account',
    user: {
      username, coins: u.coins, inventory: u.inventory, stats: u.stats,
      specialRole: u.specialRole || null, mutedUntil: u.mutedUntil || 0,
      lockedItems: lockedItemsForUser(username),
      pendingWithdrawals: myPendingWithdrawals,
      discord: discordPublic(u.discord),
      ignored: ignoredListFor(username),
      vault: vaultPublic(u),
    },
  });
}

// Fire-and-forget persistence: memory has already been updated synchronously
// (same as before this feature existed) by the time this is called, so this
// just mirrors that already-decided state into Postgres afterward. Errors
// are logged, not thrown - a slow/hiccupping DB write should never crash a
// request or block the response the player already got.
//
// `txn` (optional) - see db.js's persistUserState doc comment for shape.
// Pass it whenever coins or stats changed, so a real audit row gets written.
const PERSIST_MAX_ATTEMPTS = 4;
function persistUser(username, txn, attempt = 1) {
  const u = users.get(username);
  if (!u) return;
  db.persistUserState(username, u, txn).catch((err) => {
    console.error(`[db] persistUser failed for ${username} (${txn ? txn.type : 'no-txn'}) attempt ${attempt}/${PERSIST_MAX_ATTEMPTS}:`, err.message);
    // Retry instead of silently leaving Postgres behind memory - a single
    // dropped write used to mean pets quietly reverting/vanishing on the
    // next restart. Each retry re-reads the CURRENT in-memory state, so a
    // late retry can never write stale numbers over newer ones.
    if (attempt < PERSIST_MAX_ATTEMPTS) {
      setTimeout(() => persistUser(username, txn, attempt + 1), 750 * attempt);
    } else {
      console.error(`[db] GAVE UP persisting ${username} - memory and DB may disagree. txn=${JSON.stringify(txn)}`);
    }
  });
}

function newEscrowId(kind) {
  return `${kind}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

// Every client-supplied item list goes through this: must be a real array of
// real catalog ids, with a sane length. Anything else is rejected outright
// (previously a malformed list could throw inside a handler and crash the
// whole server - taking any escrowed pets with it).
function sanitizeItems(raw, max = 100) {
  if (!Array.isArray(raw) || raw.length > max) return null;
  for (const id of raw) {
    if (typeof id !== 'string' || !Object.prototype.hasOwnProperty.call(petCatalog, id)) return null;
  }
  return raw.slice();
}

function refuseNaItems(username, items) {
  if (!items || !items.some((id) => petCatalog[id] && petCatalog[id].na)) return false;
  send(usernameToSocket.get(username), { type: 'error', message: "N/A pets don't have a value yet, so they can't be sold." });
  return true;
}

// ---------------------------------------------------------------------------
// LIVE PET VALUES - every few minutes the server pulls the latest values from
// demand.gg and updates the site's pets (estimates included; a pet that goes
// N/A there becomes N/A here, and comes back when it gets a value again).
// Pets with a hand-picked value (MANUAL_VALUE_IDS) are never touched.
// Settings (Railway -> Variables, both optional):
//   VALUE_SYNC=off              turn automatic updates off
//   VALUE_SYNC_MINUTES=10       how often to check (minimum 2)
// ---------------------------------------------------------------------------
const VALUE_SYNC_ON = String(process.env.VALUE_SYNC || '').toLowerCase() !== 'off';
const VALUE_SYNC_MS = Math.max(2, Number(process.env.VALUE_SYNC_MINUTES) || 10) * 60e3;
const DEMAND_ITEMS_URL = process.env.DEMAND_API_URL || 'https://api.demand.gg/api/g/bgsi/items';
const MANUAL_VALUE_IDS = new Set(['tourist-doggy', 'mythic-dumb-sinister-lord']);
// The values the site's page was built with - clients only need the pets
// whose live value differs from this.
const BASE_VALUES = Object.fromEntries(Object.entries(petCatalog).map(([id, p]) => [id, [p.value, p.na ? 1 : 0]]));
let valuesUpdatedAt = 0;
let valueSyncRunning = false;

function applyLiveValue(id, value, na) {
  const p = petCatalog[id]; if (!p) return false;
  const v = na ? 0 : Math.max(0, Math.round(value));
  const was = [p.value, p.na ? 1 : 0];
  if (was[0] === v && was[1] === (na ? 1 : 0)) return false;
  p.value = v;
  if (na) p.na = true; else delete p.na;
  return true;
}
function liveValueDiff(onlyIds) {
  const out = {};
  for (const id of onlyIds || Object.keys(petCatalog)) {
    const p = petCatalog[id]; const base = BASE_VALUES[id]; if (!p || !base) continue;
    const cur = [p.value, p.na ? 1 : 0];
    if (onlyIds || cur[0] !== base[0] || cur[1] !== base[1]) out[id] = cur;
  }
  return out;
}
async function fetchDemandPage(page) {
  const ctrl = new AbortController(); const t = setTimeout(() => ctrl.abort(), 30000);
  try {
    const res = await fetch(`${DEMAND_ITEMS_URL}?limit=500&page=${page}`, { headers: { accept: 'application/json', 'user-agent': 'BloxyVault value sync' }, signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally { clearTimeout(t); }
}
async function syncValuesFromDemand() {
  if (valueSyncRunning) return; valueSyncRunning = true;
  try {
    const live = new Map();
    let page = 1, pages = 1, total = 0;
    do {
      const data = await fetchDemandPage(page);
      const items = Array.isArray(data && data.items) ? data.items : [];
      pages = Math.min(100, Number(data && data.pagination && data.pagination.pages) || 1);
      total = Number(data && data.pagination && data.pagination.total) || total;
      for (const it of items) {
        if (!it || it.categoryId !== 'pets' || typeof it.slug !== 'string') continue;
        const val = it.value || {};
        const n = Number(val.value != null ? val.value : val.estimate);
        live.set(it.slug, Number.isFinite(n) && n > 0 ? { value: n, na: false } : { value: 0, na: true });
      }
      page++;
      if (page <= pages) await new Promise((r) => setTimeout(r, 250)); // be gentle with their API
    } while (page <= pages);
    // A half-broken answer must never wipe out good values.
    if (live.size < Math.max(1000, total * 0.5)) throw new Error(`only got ${live.size} of ${total} pets - skipping this update`);
    const changed = [];
    for (const id of Object.keys(petCatalog)) {
      if (MANUAL_VALUE_IDS.has(id) || !live.has(id)) continue;
      const l = live.get(id);
      if (applyLiveValue(id, l.value, l.na)) changed.push(id);
    }
    valuesUpdatedAt = Date.now();
    if (changed.length) {
      db.saveServerState('liveValues', { values: liveValueDiff(), at: valuesUpdatedAt }).catch((e) => console.error('[values] save failed:', e.message));
      broadcast({ type: 'catalog:values', values: liveValueDiff(changed), at: valuesUpdatedAt });
    }
    console.log(`[values] Updated from demand.gg: ${changed.length} pet value(s) changed.`);
  } catch (err) {
    console.error('[values] Update from demand.gg failed (keeping current values):', err.message);
  } finally { valueSyncRunning = false; }
}

// Exchange stock + rain pool used to live only in memory (wiped by every
// restart). These mirror changes to Postgres.
function persistShopStock(ids) {
  const entries = {};
  for (const id of ids) entries[id] = shopStock[id] || 0;
  db.saveShopStock(entries).catch((err) => console.error('[db] Failed to persist shop stock:', err.message));
}
function persistRainPool() {
  db.saveServerState('rainPool', { pool: rainPool }).catch((err) => console.error('[db] Failed to persist rain pool:', err.message));
}

// ---------------------------------------------------------------------------
// Exchange - convert items to coins or coins to items. This was previously
// client-only (never touched the server), which is exactly why a purchase
// looked like it worked locally but the server - which is now the actual
// source of truth for your account - never learned about it.
// ---------------------------------------------------------------------------
const TAX_RATE = 0.05; // must match the client's TAX_RATE

function handleExchangeSell(username, msg) {
  // Sells exactly the requested quantity of each item (how many times its id
  // appears in the array), capped by what's actually owned - not the whole
  // stack regardless of selection, so partial sells work as expected.
  const sellIds = sanitizeItems(msg.items, 500);
  if (!sellIds) return send(usernameToSocket.get(username), { type: 'error', message: 'Invalid items.' });
  if (refuseNaItems(username, sellIds)) return;
  const requested = {};
  for (const id of sellIds) requested[id] = (requested[id] || 0) + 1;
  const u = ensureUser(username);
  // Pets in a pending withdrawal must not be sellable - otherwise they could
  // be sold for coins AND still be handed over in Roblox by the admin.
  const lockedForSell = lockedItemsForUser(username);
  let rawTotal = 0;
  let stockChanged = false;
  const itemsTouched = [];
  for (const [id, reqQty] of Object.entries(requested)) {
    const owned = u.inventory[id] || 0;
    const sellable = Math.max(0, owned - (lockedForSell[id] || 0));
    const sellQty = Math.min(reqQty, sellable);
    if (sellQty <= 0) continue;
    rawTotal += sellQty * itemValue(id);
    u.inventory[id] = owned - sellQty;
    itemsTouched.push(id);
    // The pet doesn't just vanish - it becomes available for someone
    // else to buy in the Exchange's "coin to item" side.
    shopStock[id] = (shopStock[id] || 0) + sellQty;
    stockChanged = true;
  }
  const before = u.coins;
  const payout = Math.floor(rawTotal * (1 - TAX_RATE));
  u.coins += payout;
  persistUser(username, { type: 'exchange_sell', amount: payout, balanceBefore: before, balanceAfter: u.coins, itemsTouched });
  syncAccount(username);
  if (stockChanged) { persistShopStock(itemsTouched); broadcast({ type: 'exchange:stock', stock: shopStock }); }
}

function handleExchangeBuy(username, msg) {
  const ids = sanitizeItems(msg.items, 500);
  if (!ids || !ids.length) return send(usernameToSocket.get(username), { type: 'error', message: 'Invalid items.' });
  if (refuseNaItems(username, ids)) return;
  const requested = {};
  for (const id of ids) requested[id] = (requested[id] || 0) + 1;
  // Every requested pet has to actually be in stock - no more minting pets
  // out of nowhere just because a player has the coins for it.
  for (const [id, reqQty] of Object.entries(requested)) {
    if ((shopStock[id] || 0) < reqQty) {
      return send(usernameToSocket.get(username), { type: 'error', message: `Not enough stock of that item in the Exchange right now.` });
    }
  }
  const cost = ids.reduce((sum, id) => sum + itemValue(id), 0);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: `Not enough coins - you need ${cost}.` });
  const before = u.coins;
  u.coins -= cost;
  for (const [id, reqQty] of Object.entries(requested)) shopStock[id] -= reqQty;
  addItems(username, ids);
  persistUser(username, { type: 'exchange_buy', amount: -cost, balanceBefore: before, balanceAfter: u.coins, itemsTouched: [...new Set(ids)] });
  syncAccount(username);
  persistShopStock(Object.keys(requested));
  broadcast({ type: 'exchange:stock', stock: shopStock });
}

function handleAccountReset(username) {
  if (!ENABLE_DEV_BYPASS) return send(usernameToSocket.get(username), { type: 'error', message: 'That feature is disabled.' });
  const u = ensureUser(username);
  const before = u.coins;
  u.coins = STARTING_BALANCE;
  const itemsTouched = Object.keys(u.inventory);
  u.inventory = {};
  // Zero out every previously-owned item's row too, not just the ones that
  // happened to still be non-zero - a stale row with an old qty would
  // otherwise reappear on the next server restart's DB load.
  persistUser(username, { type: 'account_reset', amount: u.coins - before, balanceBefore: before, balanceAfter: u.coins, itemsTouched, reason: 'Dev bypass: account reset' });
  syncAccount(username);
}

// Admin-only, view-only lookup of another player's current coins/inventory
// - lets the site owner verify a withdrawal claim without being able to
// add, remove, or edit anything in that account. Silently ignored for
// anyone who isn't ADMIN_USERNAME.
async function handleAdminLookup(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const rawTarget = String(msg.username || '');
  const resolved = resolveExistingUsername(rawTarget);
  // ensureUser() (not a plain users.get()) so an existing account's
  // self-healing runs before we display it - otherwise a real account
  // that just hasn't been touched since a new pet was added to the
  // catalog would show as "missing" items that have actually already
  // been backfilled for everyone else who's logged in since.
  const u = resolved ? ensureUser(resolved) : null;
  const target = resolved || rawTarget;
  let transactions = [];
  if (u) {
    try {
      transactions = await db.loadRecentTransactionsForUser(target, 15);
    } catch (err) {
      console.error(`[db] Failed to load transaction history for ${target}:`, err.message);
    }
  }
  send(usernameToSocket.get(username), {
    type: 'admin:lookupResult',
    username: target,
    found: !!u,
    coins: u ? u.coins : null,
    inventory: u ? u.inventory : null,
    stats: u ? u.stats : null,
    vault: u ? vaultPublic(u) : null,
    transactions,
  });
}

// Admin-only balance adjustment - covers both "add coins" and "remove
// coins" (removeCoins is just addCoins with a negated, clamped amount).
// Every call writes a real transaction row via the same persistUser() path
// every other coin-mutating action already uses, so this shows up in the
// user's transaction history exactly like a tip would,
// just tagged as an admin action with who did it and why.
async function handleAdminAdjustCoins(username, msg, direction) {
  if (username !== ADMIN_USERNAME) return;
  const errTo = usernameToSocket.get(username);
  const rawTarget = String(msg.username || '');
  const amount = Math.floor(Number(msg.amount) || 0);
  const reason = String(msg.reason || '').slice(0, 200) || (direction > 0 ? 'Admin coin grant' : 'Admin coin removal');
  if (!rawTarget || amount <= 0) {
    return send(errTo, { type: 'error', message: 'Enter a username and a positive amount.' });
  }
  const resolved = resolveExistingUsername(rawTarget);
  if (!resolved && direction < 0) {
    return send(errTo, { type: 'error', message: `No account found for "${rawTarget}".` });
  }
  // Adding coins is allowed to create the account fresh if it doesn't exist
  // yet (e.g. funding BloxyVault_Stock or DTN_BGSI before they've ever
  // logged in) - same idea as tipping someone who hasn't logged in yet.
  // Removing from nothing doesn't make sense, so that still requires a
  // real existing account (checked above).
  const target = resolved || rawTarget;
  const u = ensureUser(target);
  const before = u.coins;
  const delta = direction > 0 ? amount : -Math.min(amount, u.coins); // never push a balance negative
  u.coins += delta;
  persistUser(target, {
    type: 'admin_adjust',
    amount: delta,
    balanceBefore: before,
    balanceAfter: u.coins,
    reason,
    adminUsername: username,
  });
  syncAccount(target);
  send(errTo, { type: 'admin:adjustResult', username: target, ok: true, coins: u.coins, delta });
}
// pets out for the real Roblox items", the admin sees the request (plus can
// double-check their actual inventory via admin:lookup above to make sure
// they're not lying about what they have), does the real-world trade
// themselves outside this app, then marks it fulfilled here - which is what
// actually removes the items, so there's a clear log of who asked for what
// and when, rather than silent inventory edits with no trail.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Withdrawal requests - a real player says "I want to cash these specific
// pets out for the real Roblox items", the admin sees the request (plus can
// double-check their actual inventory via admin:lookup above to make sure
// they're not lying about what they have), does the real-world trade
// themselves outside this app, then marks it fulfilled here - which is what
// actually removes the items, so there's a clear log of who asked for what
// and when, rather than silent inventory edits with no trail.
// ---------------------------------------------------------------------------
let withdrawRequests = []; // { id, username, items, requestedAt, status, actuallyRemoved? }
const MAX_WITHDRAW_ITEMS = 8;

async function handleWithdrawRequest(username, msg) {
  const errTo = usernameToSocket.get(username);
  const items = sanitizeItems(msg.items, MAX_WITHDRAW_ITEMS * 2);
  if (!items) return send(errTo, { type: 'error', message: 'Invalid items.' });
  if (!items.length) return send(errTo, { type: 'error', message: 'Select at least one item to withdraw.' });
  if (items.length > MAX_WITHDRAW_ITEMS) return send(errTo, { type: 'error', message: `You can withdraw at most ${MAX_WITHDRAW_ITEMS} pets at a time.` });
  if (!ownsAll(username, items)) return send(errTo, { type: 'error', message: "You don't own all of those items." });

  if (openRequestCount(username) >= MAX_OPEN_REQUESTS) {
    return send(errTo, { type: 'error', message: `You already have ${MAX_OPEN_REQUESTS} open requests. Wait for those first (or cancel one).` });
  }
  const req = { id: 'wd_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8), kind: 'withdraw', username, items, requestedAt: Date.now(), status: 'pending', claimedBy: null };
  try {
    await db.insertWithdrawRequest(req);
  } catch (err) {
    console.error(`[db] Failed to persist withdraw request for ${username}:`, err.message);
    return send(errTo, { type: 'error', message: 'Something went wrong submitting that - try again in a moment.' });
  }
  withdrawRequests.push(req);
  send(errTo, { type: 'withdraw:requested', id: req.id });
  discordPostRequest(req);
  refreshRequestOwners();
  syncAccount(username);
  broadcastPendingWithdrawalsToAdmin();
}

function broadcastPendingWithdrawalsToAdmin(){
  const adminWs = usernameToSocket.get(ADMIN_USERNAME);
  if (!adminWs) return;
  send(adminWs, { type: 'admin:withdrawList', requests: withdrawRequests.filter(r => r.status === 'pending') });
}

function handleAdminWithdrawList(username) {
  if (username !== ADMIN_USERNAME) return;
  broadcastPendingWithdrawalsToAdmin();
}

async function handleAdminWithdrawFulfill(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req) return;
  const r = await completeRequest(req, ADMIN_USERNAME);
  if (r !== 'ok' && r !== 'busy') send(usernameToSocket.get(username), { type: 'error', message: r });
}
async function handleAdminWithdrawReject(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req) return;
  const r = await rejectRequest(req, ADMIN_USERNAME);
  if (r !== 'ok' && r !== 'busy') send(usernameToSocket.get(username), { type: 'error', message: r });
}
async function handleAdminRequestClaim(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req) return;
  const r = msg.release ? await releaseReq(req, ADMIN_USERNAME) : await claimReq(req, ADMIN_USERNAME);
  if (r !== 'ok' && r !== 'busy') send(usernameToSocket.get(username), { type: 'error', message: r });
}

// A player can cancel their own request until someone claims it (a claim
// means a helper is about to trade, so it locks from then on).
async function handleWithdrawCancel(username, msg) {
  const ws = usernameToSocket.get(username);
  const req = withdrawRequests.find((r) => r.id === String(msg.requestId || '') && r.username === username && r.status === 'pending');
  if (!req) return send(ws, { type: 'error', message: 'That request is no longer open.' });
  if (req.claimedBy) return send(ws, { type: 'error', message: `${req.claimedBy} is already handling this, so it can't be cancelled now.` });
  let ok;
  try { ok = await db.cancelRequest(req.id); } catch (e) { console.error('[requests] cancel failed:', e.message); return send(ws, { type: 'error', message: 'Could not cancel right now. Try again.' }); }
  if (!ok) return send(ws, { type: 'error', message: "Someone just claimed it, so it can't be cancelled now." });
  req.status = 'cancelled';
  persistUser(username, { type: req.kind === 'deposit' ? 'deposit_cancelled' : 'withdraw_cancelled', amount: 0, reason: 'Cancelled by player' });
  send(ws, { type: 'toast', message: req.kind === 'deposit' ? 'Deposit request cancelled.' : 'Withdrawal cancelled. Your pets are unlocked.' });
  discordFinishRequest(req, `🚫 Cancelled by ${username}`);
  afterRequestChange(req);
}

// ---------------------------------------------------------------------------
// REQUESTS (withdrawals + deposits) - shared by the admin panel and the
// Discord helper bot. Each action returns 'ok', 'busy' (already being
// handled / already closed) or a readable error message.
// ---------------------------------------------------------------------------
const MAX_OPEN_REQUESTS = 3;
const MAX_DEPOSIT_PETS = 100;
function requestQueuePos(req) {
  if (req.status !== 'pending' || req.claimedBy) return 0;
  const waiting = withdrawRequests.filter((r) => r.status === 'pending' && !r.claimedBy).sort((a, b) => a.requestedAt - b.requestedAt);
  return waiting.findIndex((r) => r.id === req.id) + 1;
}
function myPendingRequests(username) {
  return withdrawRequests.filter((r) => r.username === username && r.status === 'pending')
    .map((r) => ({ id: r.id, kind: r.kind || 'withdraw', items: r.items, requestedAt: r.requestedAt, claimedBy: r.claimedBy || null, queuePos: requestQueuePos(r) }));
}
// Queue positions move for everyone when one request changes.
function refreshRequestOwners(extra) {
  const names = new Set(withdrawRequests.filter((r) => r.status === 'pending').map((r) => r.username));
  if (extra) names.add(extra);
  for (const n of names) syncAccount(n);
}
function afterRequestChange(req) {
  refreshRequestOwners(req.username);
  broadcastPendingWithdrawalsToAdmin();
}
function openRequestCount(username) {
  return withdrawRequests.filter((r) => r.username === username && r.status === 'pending').length;
}

async function handleDepositRequest(username, msg) {
  const ws = usernameToSocket.get(username);
  const items = sanitizeItems(msg.items, MAX_DEPOSIT_PETS);
  if (!items || !items.length) return send(ws, { type: 'error', message: 'Pick the pets you are going to trade in.' });
  if (openRequestCount(username) >= MAX_OPEN_REQUESTS) {
    return send(ws, { type: 'error', message: `You already have ${MAX_OPEN_REQUESTS} open requests. Wait for those first (or cancel one).` });
  }
  const req = { id: 'dp_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8), kind: 'deposit', username, items, requestedAt: Date.now(), status: 'pending', claimedBy: null };
  try { await db.insertWithdrawRequest(req); } catch (e) { console.error('[requests] deposit insert failed:', e.message); return send(ws, { type: 'error', message: 'Something went wrong - try again in a moment.' }); }
  withdrawRequests.push(req);
  send(ws, { type: 'deposit:requested', id: req.id });
  discordPostRequest(req);
  afterRequestChange(req);
}

async function claimReq(req, by) {
  if (req.status !== 'pending') return 'busy';
  if (req.claimedBy) return req.claimedBy === by ? 'ok' : `${req.claimedBy} already claimed this.`;
  let ok;
  try { ok = await db.claimRequest(req.id, by); } catch (e) { console.error('[requests] claim failed:', e.message); return 'Database error - try again.'; }
  if (!ok) return 'Someone else just claimed it (or the player cancelled).';
  if (req.status !== 'pending') return 'busy';
  req.claimedBy = by; req.claimedAt = Date.now();
  const what = req.kind === 'deposit' ? 'deposit' : 'withdrawal';
  createNotification({ username: req.username, type: 'withdraw_fulfilled', title: '🔒 Being handled', message: `${by} is handling your ${what} now. Get ready to trade in Roblox.`, refId: req.id });
  send(usernameToSocket.get(req.username), { type: 'toast', message: `🔒 ${by} is handling your ${what} now. Get ready to trade!` });
  discordUpdateRequest(req);
  afterRequestChange(req);
  return 'ok';
}
async function releaseReq(req, by) {
  if (req.status !== 'pending' || !req.claimedBy) return 'busy';
  if (req.claimedBy !== by && by !== ADMIN_USERNAME) return `Only ${req.claimedBy} can release this.`;
  try { await db.unclaimRequest(req.id); } catch (e) { return 'Database error - try again.'; }
  req.claimedBy = null; req.claimedAt = 0;
  discordUpdateRequest(req);
  afterRequestChange(req);
  return 'ok';
}
async function completeRequest(req, by) {
  if (req.kind === 'deposit') return fulfillDepositRequest(req, by);
  const r = await fulfillWithdrawRequest(req, by);
  return r === 'db_error' ? 'Database error - nothing was removed. Try again.' : r;
}
async function rejectRequest(req, by) {
  if (!req || req.status !== 'pending' || req.resolving) return 'busy';
  req.resolving = true;
  let ok;
  try { ok = await db.resolveWithdrawRequest(req.id, 'pending', 'rejected', null, by); } catch (err) { req.resolving = false; console.error(`[db] Failed to reject request ${req.id}:`, err.message); return 'Database error - try again.'; }
  if (!ok) { req.resolving = false; return 'busy'; }
  req.status = 'rejected';
  const what = req.kind === 'deposit' ? 'deposit' : 'withdrawal';
  persistUser(req.username, { type: req.kind === 'deposit' ? 'deposit_rejected' : 'withdraw_rejected', amount: 0, reason: `Rejected by ${by}`, adminUsername: by === ADMIN_USERNAME ? ADMIN_USERNAME : undefined });
  send(usernameToSocket.get(req.username), { type: 'error', message: `Your ${what} request was declined.` });
  createNotification({ username: req.username, type: 'withdraw_rejected', title: `❌ ${what[0].toUpperCase() + what.slice(1)} rejected`, message: `Your ${what} request was rejected by ${by}.`, refId: req.id });
  discordFinishRequest(req, `❌ Rejected by ${by}`);
  afterRequestChange(req);
  return 'ok';
}
async function fulfillDepositRequest(req, by) {
  if (!req || req.status !== 'pending' || req.resolving) return 'busy';
  req.resolving = true;
  const added = {};
  for (const id of req.items) added[id] = (added[id] || 0) + 1;
  let ok;
  try { ok = await db.resolveWithdrawRequest(req.id, 'pending', 'fulfilled', added, by); } catch (err) { req.resolving = false; console.error(`[db] Failed to complete deposit ${req.id}:`, err.message); return 'Database error - nothing was added. Try again.'; }
  if (!ok) { req.resolving = false; return 'busy'; }
  addItems(req.username, req.items);
  req.status = 'fulfilled';
  const u = ensureUser(req.username);
  const value = req.items.reduce((sum, id) => sum + itemValue(id), 0);
  persistUser(req.username, { type: 'deposit_credit', amount: 0, balanceBefore: u.coins, balanceAfter: u.coins, itemsTouched: Object.keys(added), reason: `Deposit handled by ${by}`, value });
  const names = Object.entries(added).map(([id, q]) => `${petCatalog[id] ? petCatalog[id].name : id}${q > 1 ? ' x' + q : ''}`).join(', ');
  createNotification({ username: req.username, type: 'withdraw_fulfilled', title: '📥 Deposit received', message: `${by} added: ${names}.`, refId: req.id });
  send(usernameToSocket.get(req.username), { type: 'toast', message: `📥 Deposit received: ${names}` });
  discordFinishRequest(req, `✅ Done by ${by}`);
  afterRequestChange(req);
  return 'ok';
}

// Marks a pending withdrawal as handed over and removes the pets from the
// player's site inventory. Returns 'ok', 'busy' or 'db_error'.
async function fulfillWithdrawRequest(req, by) {
  if (!req || req.status !== 'pending' || req.resolving) return 'busy';
  req.resolving = true; // blocks a double-click from running this twice while we wait on the DB
  // Work out what will be removed (capped by what they still own), but DON'T
  // touch the inventory until the DB has confirmed pending->fulfilled.
  const target = ensureUser(req.username);
  const requested = {};
  for (const id of req.items) requested[id] = (requested[id] || 0) + 1;
  const plannedRemoval = {};
  for (const [id, reqQty] of Object.entries(requested)) {
    const removeQty = Math.min(reqQty, target.inventory[id] || 0);
    if (removeQty > 0) plannedRemoval[id] = removeQty;
  }
  let ok;
  try {
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'fulfilled', plannedRemoval, by);
  } catch (err) {
    req.resolving = false;
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return 'db_error';
  }
  if (!ok) { req.resolving = false; return 'busy'; }
  const actuallyRemoved = {};
  for (const [id, qty] of Object.entries(plannedRemoval)) {
    const owned = target.inventory[id] || 0;
    const removeQty = Math.min(qty, owned);
    if (removeQty > 0) { target.inventory[id] = owned - removeQty; actuallyRemoved[id] = removeQty; }
  }
  req.status = 'fulfilled';
  req.actuallyRemoved = actuallyRemoved;
  const withdrawValue = Object.entries(actuallyRemoved).reduce((sum, [id, q]) => sum + itemValue(id) * q, 0);
  persistUser(req.username, { type: 'withdraw_fulfilled', amount: 0, itemsTouched: Object.keys(actuallyRemoved), reason: `Fulfilled by ${by}`, adminUsername: by === ADMIN_USERNAME ? ADMIN_USERNAME : undefined, value: -withdrawValue });
  send(usernameToSocket.get(req.username), { type: 'chat:message', username: 'System', text: `Your withdrawal was fulfilled by ${by}.`, timestamp: Date.now(), system: true });
  const removedNames = Object.keys(actuallyRemoved).map((id) => (petCatalog[id] ? petCatalog[id].name : id)).join(', ');
  createNotification({
    username: req.username,
    type: 'withdraw_fulfilled',
    title: '✅ Withdrawal fulfilled',
    message: removedNames ? `Your withdrawal was fulfilled by ${by}: ${removedNames}.` : 'Your withdrawal request was fulfilled.',
    refId: req.id,
  });
  discordFinishRequest(req, `✅ Done by ${by}`);
  afterRequestChange(req);
  return 'ok';
}

// ---------------------------------------------------------------------------
// DISCORD HELPER BOT - posts every request in a private channel with buttons.
// Helpers (anyone with the Helper role) press Claim -> trade in Roblox ->
// Done. Settings (Railway -> Variables), on top of DISCORD_BOT_TOKEN:
//   DISCORD_PUBLIC_KEY          the app's Public Key (Developer Portal -> General Information)
//   DISCORD_REQUESTS_CHANNEL_ID the private channel the requests go to
//   DISCORD_HELPER_ROLE_ID      the role allowed to press the buttons (also pinged for reminders)
//   REQUEST_REMIND_MINUTES      optional, default 30
// In the Developer Portal set Interactions Endpoint URL to
//   https://<your site>/discord/interactions
// ---------------------------------------------------------------------------
const DISCORD_PUBLIC_KEY = String(process.env.DISCORD_PUBLIC_KEY || '').trim();
const DISCORD_REQUESTS_CHANNEL_ID = String(process.env.DISCORD_REQUESTS_CHANNEL_ID || '').trim();
const DISCORD_HELPER_ROLE_ID = String(process.env.DISCORD_HELPER_ROLE_ID || '').trim();
const REQUEST_REMIND_MS = Math.max(0.02, Number(process.env.REQUEST_REMIND_MINUTES) || 30) * 60e3;
function discordRequestsOn() { return !!(DISCORD_BOT_TOKEN && /^\d{5,25}$/.test(DISCORD_REQUESTS_CHANNEL_ID)); }
let discordPublicKeyObj = null;
try {
  if (/^[0-9a-f]{64}$/i.test(DISCORD_PUBLIC_KEY)) discordPublicKeyObj = crypto.createPublicKey({ key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), Buffer.from(DISCORD_PUBLIC_KEY, 'hex')]), format: 'der', type: 'spki' });
  else if (DISCORD_PUBLIC_KEY) console.error('[discord-requests] DISCORD_PUBLIC_KEY should be 64 hex characters.');
} catch (e) { console.error('[discord-requests] DISCORD_PUBLIC_KEY is not valid:', e.message); }

function requestEmbed(req, finalText) {
  const grouped = {};
  for (const id of req.items) grouped[id] = (grouped[id] || 0) + 1;
  const lines = Object.entries(grouped).sort((a, b) => itemValue(b[0]) - itemValue(a[0]))
    .map(([id, q]) => `• ${petCatalog[id] ? petCatalog[id].name : id}${q > 1 ? ` ×${q}` : ''}`);
  const shown = lines.slice(0, 25).join('\n') + (lines.length > 25 ? `\n…and ${lines.length - 25} more` : '');
  const value = req.items.reduce((s, id) => s + itemValue(id), 0);
  const isDep = req.kind === 'deposit';
  let status, color;
  if (finalText) { status = finalText; color = /Done/.test(finalText) ? 0x34d399 : 0x6b7280; }
  else if (req.claimedBy) { status = `🔒 Claimed by **${req.claimedBy}** — trading now`; color = 0xf4c430; }
  else { status = '🕒 Waiting for a helper — press **Claim** before you trade'; color = isDep ? 0x3b82f6 : 0xff8a2a; }
  const u = users.get(req.username);
  const discordName = u && u.discord && u.discord.username ? ` · Discord: ${u.discord.username}` : '';
  return {
    title: `${isDep ? '📥 Deposit' : '📦 Withdrawal'} — ${req.username}`,
    description: `**Roblox:** ${req.username}${discordName}\n**${isDep ? 'They will give you' : 'Give them'}:**\n${shown || '—'}\n\n**Total:** ${value.toLocaleString('en-US')} coins\n\n${status}`,
    color,
    footer: { text: `Request ${req.id}` },
    timestamp: new Date(req.requestedAt).toISOString(),
  };
}
function requestButtons(req, finalText) {
  if (finalText || req.status !== 'pending') return [];
  const btn = (style, label, action) => ({ type: 2, style, label, custom_id: `rq:${action}:${req.id}` });
  const row = req.claimedBy
    ? [btn(3, req.kind === 'deposit' ? '✅ Done (pets received)' : '✅ Done (pets given)', 'done'), btn(2, 'Release', 'release'), btn(4, 'Reject', 'reject')]
    : [btn(1, '✋ Claim', 'claim'), btn(4, 'Reject', 'reject')];
  return [{ type: 1, components: row }];
}
async function discordPostRequest(req) {
  if (!discordRequestsOn()) return;
  try {
    const r = await discordApi('POST', `/channels/${DISCORD_REQUESTS_CHANNEL_ID}/messages`, { embeds: [requestEmbed(req)], components: requestButtons(req) });
    if (r && r.status === 200 && r.data && r.data.id) {
      req.discordMessageId = r.data.id;
      db.setRequestDiscordMessage(req.id, r.data.id).catch(() => {});
      // If it changed while the post was in flight, bring the message up to date.
      if (req.claimedBy || req.status !== 'pending') discordUpdateRequest(req, req.discordFinal);
    } else console.error('[discord-requests] post failed:', r && r.status, JSON.stringify((r && r.data) || {}).slice(0, 200));
  } catch (e) { console.error('[discord-requests] post failed:', e.message); }
}
function discordUpdateRequest(req, finalText) {
  if (!discordRequestsOn() || !req.discordMessageId) return;
  discordApi('PATCH', `/channels/${DISCORD_REQUESTS_CHANNEL_ID}/messages/${req.discordMessageId}`, { embeds: [requestEmbed(req, finalText)], components: requestButtons(req, finalText) })
    .catch((e) => console.error('[discord-requests] update failed:', e.message));
}
function discordFinishRequest(req, finalText) { req.discordFinal = finalText; discordUpdateRequest(req, finalText); }

function verifyDiscordSignature(req, rawBody) {
  if (!discordPublicKeyObj) return false;
  const sig = String(req.headers['x-signature-ed25519'] || ''); const ts = String(req.headers['x-signature-timestamp'] || '');
  if (!/^[0-9a-f]{128}$/i.test(sig) || !ts) return false;
  try { return crypto.verify(null, Buffer.concat([Buffer.from(ts), rawBody]), discordPublicKeyObj, Buffer.from(sig, 'hex')); } catch (e) { return false; }
}
function readRawBody(req, max = 65536) {
  return new Promise((resolve) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => { size += c.length; if (size > max) { req.destroy(); resolve(null); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', () => resolve(null));
  });
}
async function handleDiscordInteraction(req, res) {
  const json = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
  const raw = await readRawBody(req);
  if (!raw || !verifyDiscordSignature(req, raw)) { res.writeHead(401); res.end('invalid request signature'); return; }
  let it; try { it = JSON.parse(raw.toString('utf8')); } catch (e) { res.writeHead(400); res.end(); return; }
  if (it.type === 1) return json(200, { type: 1 }); // Discord's "are you there?" check
  const reply = (text) => json(200, { type: 4, data: { content: text, flags: 64 } }); // only the clicker sees it
  if (it.type !== 3 || !it.data || typeof it.data.custom_id !== 'string') return reply('Unknown action.');
  const roles = (it.member && it.member.roles) || [];
  if (!DISCORD_HELPER_ROLE_ID || !roles.includes(DISCORD_HELPER_ROLE_ID)) return reply('Only Helpers can do that.');
  const userObj = (it.member && it.member.user) || it.user || {};
  const by = String((it.member && it.member.nick) || userObj.global_name || userObj.username || 'a helper').replace(/[*_`~|>]/g, '').slice(0, 40) || 'a helper';
  const m = /^rq:(claim|done|release|reject):(.+)$/.exec(it.data.custom_id);
  if (!m) return reply('Unknown action.');
  const rq = withdrawRequests.find((r) => r.id === m[2]);
  if (!rq || rq.status !== 'pending') {
    const closed = rq ? (rq.discordFinal || `Already ${rq.status}.`) : 'This request is already closed.';
    if (!rq) return reply(closed);
    return json(200, { type: 7, data: { embeds: [requestEmbed(rq, closed)], components: [] } });
  }
  if (siteLockdown && m[1] !== 'release') return reply("The site is paused right now (lockdown), so requests can't be handled.");
  let r;
  if (m[1] === 'claim') r = await claimReq(rq, by);
  else if (m[1] === 'release') r = await releaseReq(rq, by);
  else if (m[1] === 'reject') r = await rejectRequest(rq, by);
  else {
    if (!rq.claimedBy) r = 'Press Claim first.';
    else if (rq.claimedBy !== by) r = `${rq.claimedBy} claimed this - only they can press Done.`;
    else r = await completeRequest(rq, by);
  }
  if (r !== 'ok') return reply(r === 'busy' ? 'Someone else is already finishing this one.' : r);
  const fin = rq.status === 'pending' ? null : rq.discordFinal;
  return json(200, { type: 7, data: { embeds: [requestEmbed(rq, fin)], components: requestButtons(rq, fin) } });
}

// Pings helpers when requests have waited too long without a claim
// (no quiet hours - at most one reminder per REQUEST_REMIND_MS).
let lastRequestReminder = 0;
function checkRequestReminders() {
  if (!discordRequestsOn()) return;
  const now = Date.now();
  const late = withdrawRequests.filter((r) => r.status === 'pending' && !r.claimedBy && now - r.requestedAt > REQUEST_REMIND_MS);
  if (!late.length || now - lastRequestReminder < REQUEST_REMIND_MS) return;
  lastRequestReminder = now;
  const mins = Math.round(REQUEST_REMIND_MS / 60e3);
  const list = late.slice(0, 10).map((r) => `• ${r.kind === 'deposit' ? '📥 Deposit' : '📦 Withdrawal'} for **${r.username}** (waiting ${Math.round((now - r.requestedAt) / 60e3)} min)`).join('\n');
  discordApi('POST', `/channels/${DISCORD_REQUESTS_CHANNEL_ID}/messages`, {
    content: `${DISCORD_HELPER_ROLE_ID ? `<@&${DISCORD_HELPER_ROLE_ID}> ` : ''}⏰ ${late.length} request${late.length === 1 ? ' has' : 's have'} been waiting over ${mins} minutes:\n${list}`,
    allowed_mentions: { roles: DISCORD_HELPER_ROLE_ID ? [DISCORD_HELPER_ROLE_ID] : [] },
  }).catch(() => {});
}
setInterval(checkRequestReminders, Number(process.env.REQUEST_REMIND_CHECK_MS) || 60e3);

// ---------------------------------------------------------------------------
// Chat - real, shared, no bots. Very light rate limiting (one message per
// 500ms per connection) just to stop an accidental flood from one client.
// ---------------------------------------------------------------------------
const lastChatAt = new Map(); // ws -> timestamp

// Moderators get ONLY this one permission (mute/unmute) - deliberately kept
// completely separate from ADMIN_USERNAME checks everywhere else in the
// file (coin adjustments, withdrawals, role granting, etc. all still check
// ADMIN_USERNAME alone, untouched by this). This is the sole place a
// moderator's specialRole grants any actual capability.
function isModOrAdmin(username) {
  if (username === ADMIN_USERNAME) return true;
  const u = users.get(username);
  return !!u && u.specialRole === 'moderator';
}

const MAX_MUTE_MS = 24 * 60 * 60 * 1000; // hard cap - no accidental/malicious "forever" mutes
// Parses "30m", "1h", "1h30m", "2h" etc. Returns milliseconds, or null if it
// doesn't look like a valid duration at all.
function parseMuteDuration(str) {
  const s = String(str || '').trim().toLowerCase();
  const m = s.match(/^(?:(\d+)h)?(?:(\d+)m)?$/);
  if (!m || (!m[1] && !m[2])) return null;
  const hours = parseInt(m[1] || '0', 10);
  const mins = parseInt(m[2] || '0', 10);
  const totalMs = (hours * 60 + mins) * 60 * 1000;
  if (totalMs <= 0) return null;
  return Math.min(totalMs, MAX_MUTE_MS);
}
function formatMuteDuration(ms) {
  const totalMin = Math.round(ms / 60000);
  const h = Math.floor(totalMin / 60), m = totalMin % 60;
  if (h > 0 && m > 0) return `${h}h${m}m`;
  if (h > 0) return `${h}h`;
  return `${m}m`;
}

let siteLockdown = false;
const LOCKDOWN_BLOCKED_TYPES = new Set([
  'rain:deposit', 'rain:claim',
  'exchange:sell', 'exchange:buy',
  'deposit:request',
  'account:reset',
  'withdraw:request', 'withdraw:cancel',
  'tip:send',
  'vault:deposit', 'vault:withdraw', 'vault:depositItems', 'vault:withdrawItems',
  'promo:redeem', 'giveaway:create', 'giveaway:join',
]);

function handleChatSend(username, msg, ws) {
  const now = Date.now();
  if (now - (lastChatAt.get(ws) || 0) < 500) return;
  lastChatAt.set(ws, now);
  const text = String(msg.text || '').slice(0, 70).trim();
  if (!text) return;

  // /mute, /unmute, and /lockdown are commands, not real chat messages -
  // they never get broadcast as text even if a non-mod/non-admin types
  // them (the permission check below just quietly rejects it with a
  // private error, same as if they'd tried any other admin action they
  // don't have).
  const muteMatch = text.match(/^\/mute\s+(\S+)\s+(\S+)/i);
  const unmuteMatch = !muteMatch && text.match(/^\/unmute\s+(\S+)/i);
  const lockdownMatch = !muteMatch && !unmuteMatch && text.match(/^\/lockdown\s+(on|off)/i);
  if (lockdownMatch) {
    // Deliberately stricter than /mute's isModOrAdmin - this freezes every
    // player's ability to touch coins/items/escrow site-wide, so it's
    // restricted to the actual site owner only, not moderators.
    if (username !== ADMIN_USERNAME) {
      return send(usernameToSocket.get(username), { type: 'error', message: "You don't have permission to do that." });
    }
    const turningOn = lockdownMatch[1].toLowerCase() === 'on';
    if (turningOn === siteLockdown) {
      return send(usernameToSocket.get(username), { type: 'toast', message: `Lockdown is already ${turningOn ? 'ON' : 'OFF'}.` });
    }
    siteLockdown = turningOn;
    send(usernameToSocket.get(username), { type: 'toast', message: `Site lockdown is now ${turningOn ? 'ON' : 'OFF'}.` });
    pushChatMessage({
      username: 'System',
      text: turningOn
        ? '🔒 The site has been locked down by an admin. All gameplay is paused until further notice.'
        : '🔓 The lockdown has been lifted. Gameplay is back to normal.',
      timestamp: now, xp: 0, system: true,
    });
    return;
  }
  if (muteMatch || unmuteMatch) {
    if (!isModOrAdmin(username)) {
      return send(usernameToSocket.get(username), { type: 'error', message: "You don't have permission to do that." });
    }
    if (muteMatch) {
      const targetName = resolveExistingUsername(muteMatch[1]);
      if (!targetName) return send(usernameToSocket.get(username), { type: 'error', message: 'No user with that name.' });
      const durationMs = parseMuteDuration(muteMatch[2]);
      if (!durationMs) {
        return send(usernameToSocket.get(username), { type: 'error', message: 'Usage: /mute username 30m (or 1h, 1h30m, etc. - max 24h).' });
      }
      const targetUser = ensureUser(targetName);
      if (targetName === ADMIN_USERNAME) {
        return send(usernameToSocket.get(username), { type: 'error', message: "Can't mute the admin account." });
      }
      targetUser.mutedUntil = now + durationMs;
      const label = formatMuteDuration(durationMs);
      send(usernameToSocket.get(username), { type: 'toast', message: `Muted ${targetName} for ${label}.` });
      const targetWs = usernameToSocket.get(targetName);
      if (targetWs) send(targetWs, { type: 'toast', message: `You've been muted for ${label} by a moderator.`, isError: true });
      pushChatMessage({ username: 'System', text: `${targetName} was muted for ${label}.`, timestamp: now, xp: 0, system: true });
    } else {
      const targetName = resolveExistingUsername(unmuteMatch[1]);
      if (!targetName) return send(usernameToSocket.get(username), { type: 'error', message: 'No user with that name.' });
      const targetUser = ensureUser(targetName);
      targetUser.mutedUntil = 0;
      send(usernameToSocket.get(username), { type: 'toast', message: `Unmuted ${targetName}.` });
      const targetWs = usernameToSocket.get(targetName);
      if (targetWs) send(targetWs, { type: 'toast', message: `You've been unmuted.` });
      pushChatMessage({ username: 'System', text: `${targetName} was unmuted.`, timestamp: now, xp: 0, system: true });
    }
    return;
  }

  const u = ensureUser(username);
  if (u.mutedUntil && u.mutedUntil > now) {
    const remainingLabel = formatMuteDuration(u.mutedUntil - now);
    return send(usernameToSocket.get(username), { type: 'error', message: `You're muted for another ${remainingLabel}.` });
  }

  const xp = u.stats.xp || 0;
  pushChatMessage({ username, text, timestamp: now, xp, specialRole: u.specialRole || null });
}

// ---------------------------------------------------------------------------
// Public profile lookup - clicking any avatar site-wide requests this for
// the clicked user. Only ever returns PUBLIC stats (xp)
// and whether they're the admin account - never balance, inventory, or
// anything else private. Looks the user up (creating their record if this
// is literally the first time anyone's referenced them) rather than trusting
// anything the client already thinks it knows about that user.
// ---------------------------------------------------------------------------
function handleProfileRequest(username, msg) {
  const rawName = String(msg.username || '').trim();
  if (!isValidUsername(rawName)) return;
  const targetName = resolveExistingUsername(rawName) || rawName;
  // Read-only: don't create a DB row just because someone clicked/typed a name.
  const u = users.get(targetName) || { stats: { xp: 0, won: 0, lost: 0 }, specialRole: null };
  send(usernameToSocket.get(username), {
    type: 'profile:data',
    username: targetName,
    stats: { xp: u.stats.xp || 0, won: u.stats.won || 0, lost: u.stats.lost || 0 },
    isAdmin: targetName === ADMIN_USERNAME,
    specialRole: u.specialRole || null,
  });
}

const GRANTABLE_ROLES = new Set(['vip', 'moderator']);
// Manually-assigned roles (VIP / Moderator) - separate from the automatic
// xp-based milestone roles, which are never stored anywhere since
// they're always derived live from stats.xp (so they can never be
// stale, faked, or handed out early). These two ARE stored, since they're
// not tied to any measurable progress - only the real admin account can
// grant or remove one, checked here server-side regardless of what the
// client claims; the target user's own connection (if online) also gets a
// fresh sync so it takes effect immediately for them too, not just for
// whoever's viewing their profile.
function handleAdminSetRole(username, msg) {
  if (username !== ADMIN_USERNAME) return; // never trust a client claiming to be the admin
  const targetName = String(msg.targetUsername || '').trim();
  if (!isValidUsername(targetName)) return;
  const role = msg.role === null ? null : String(msg.role || '');
  if (role !== null && !GRANTABLE_ROLES.has(role)) return;
  const u = ensureUser(targetName);
  const before = u.specialRole || null;
  u.specialRole = role;
  persistUser(targetName, { type: 'admin_set_role', amount: 0, reason: `${before || 'none'} -> ${role || 'none'} (by ${username})` });
  syncAccount(targetName); // updates their own client immediately if they're online
  send(usernameToSocket.get(username), { type: 'profile:data', username: targetName, stats: { xp: u.stats.xp || 0, won: u.stats.won || 0, lost: u.stats.lost || 0 }, isAdmin: targetName === ADMIN_USERNAME, specialRole: u.specialRole || null });
}

// ---------------------------------------------------------------------------
// Discord linking + chat ignore list
// ---------------------------------------------------------------------------
function handleDiscordLinkStart(username) {
  const errTo = usernameToSocket.get(username);
  if (!discordConfigured()) return send(errTo, { type: 'error', message: "Discord linking isn't set up on the server yet." });
  const state = crypto.randomBytes(16).toString('hex');
  pendingDiscordStates.set(state, { username, expires: Date.now() + 10 * 60 * 1000 });
  const authorizeUrl = new URL('https://discord.com/oauth2/authorize');
  authorizeUrl.searchParams.set('client_id', DISCORD_CLIENT_ID);
  authorizeUrl.searchParams.set('redirect_uri', `${PUBLIC_BASE_URL}/discord/oauth/callback`);
  authorizeUrl.searchParams.set('response_type', 'code');
  authorizeUrl.searchParams.set('scope', 'identify');
  authorizeUrl.searchParams.set('state', state);
  authorizeUrl.searchParams.set('prompt', 'consent');
  send(errTo, { type: 'discord:authUrl', url: authorizeUrl.toString() });
}

async function handleDiscordUnlink(username) {
  const u = ensureUser(username);
  if (!u.discord) return;
  const oldDiscordId = u.discord.id;
  await db.clearDiscordLink(username);
  u.discord = null;
  discordRoleSyncedLevel.delete(username);
  clearDiscordLevelRoles(oldDiscordId).catch((err) => console.error('[discord-roles] clear on unlink failed:', err.message));
  syncAccount(username);
  send(usernameToSocket.get(username), { type: 'toast', message: 'Discord unlinked.' });
}

// ---------------------------------------------------------------------------
// Account history (Transaction History). Always the requesting
// player's own rows - the username comes from the logged-in connection, never
// from the message.
// ---------------------------------------------------------------------------
const HISTORY_PAGE_SIZE = 10;
const TXN_CATEGORIES = {
  deposit: ['tip_receive', 'deposit_credit'],          // split into Deposit / Tip Received below
  tip: ['tip_send', 'tip_receive'],
  withdraw: ['withdraw_fulfilled', 'withdraw_rejected'],
  exchange: ['exchange_buy', 'exchange_sell'],
  rain: ['rain_deposit', 'rain_payout'],
  vault: ['vault_deposit', 'vault_withdraw', 'vault_deposit_items', 'vault_withdraw_items', 'vault_lock'],
  giveaway: ['giveaway_create', 'giveaway_win', 'giveaway_return', 'promo_redeem'],
  other: ['admin_adjust', 'escrow_refund'],
};
// Every transaction type that is allowed to show up at all.
const TXN_VISIBLE = [...new Set(Object.values(TXN_CATEGORIES).flat())];
let historyLastAt = new Map();

async function handleHistoryRequest(username, msg) {
  const ws = usernameToSocket.get(username);
  // Light flood guard - each request is a DB query.
  const now = Date.now();
  if (now - (historyLastAt.get(username) || 0) < 250) return;
  historyLastAt.set(username, now);
  const kind = 'transactions';
  const page = Math.min(100000, Math.max(1, Math.floor(Number(msg.page) || 1)));
  const offset = (page - 1) * HISTORY_PAGE_SIZE;
  try {
    const filter = Object.prototype.hasOwnProperty.call(TXN_CATEGORIES, msg.filter) ? msg.filter : null;
    let types = filter ? TXN_CATEGORIES[filter] : TXN_VISIBLE;
    let result;
    if (filter === 'deposit' || filter === 'tip') {
      // Deposits are pets/coins sent from the stock account; anything else
      // received is a normal tip (told apart by the reason text).
      result = await db.loadTransactionHistory(username, types, null, 1000, 0);
      const fromStock = (r) => r.type === 'deposit_credit' || r.type === 'tip_receive' && String(r.reason || '').toLowerCase() === `from ${STOCK_USERNAME.toLowerCase()}`;
      const all = result.rows.filter((r) => (filter === 'deposit' ? fromStock(r) : !fromStock(r)));
      result = { total: all.length, rows: all.slice(offset, offset + HISTORY_PAGE_SIZE) };
    } else {
      result = await db.loadTransactionHistory(username, types, null, HISTORY_PAGE_SIZE, offset);
    }
    const rows = result.rows.map((r) => {
      let value = r.value;
      // Older rows (before values were stored): coins moved, else catalog
      // value of the pets listed (one of each - quantities weren't stored).
      if (value == null) {
        value = r.amount;
        if (!value && r.items.length) {
          const v = r.items.reduce((sum, id) => sum + (petCatalog[id] ? petCatalog[id].value : 0), 0);
          value = (r.type === 'tip_send' || r.type === 'withdraw_fulfilled' || r.type === 'exchange_sell') ? -v : v;
        }
      }
      const fromStock = r.type === 'deposit_credit' || r.type === 'tip_receive' && String(r.reason || '').toLowerCase() === `from ${STOCK_USERNAME.toLowerCase()}`;
      return {
        type: fromStock ? 'deposit' : r.type,
        value,
        balance: r.amount !== 0 ? 'Coins' : (r.items.length ? 'Items' : 'Coins'),
        note: fromStock ? null : r.reason,
        at: r.at,
      };
    });
    return send(ws, { type: 'history:result', kind, filter: filter || 'all', page, pageSize: HISTORY_PAGE_SIZE, total: result.total, rows });
  } catch (err) {
    console.error(`[history] ${kind} for ${username} failed:`, err.message);
    return send(ws, { type: 'history:result', kind, error: true, page, pageSize: HISTORY_PAGE_SIZE, total: 0, rows: [] });
  }
}

// ---------------------------------------------------------------------------
// VAULT - coins and pets set aside from the wallet.
// Optional time lock: while locked you can still deposit, but nothing can be
// withdrawn until it runs out. A lock can only ever be extended - there is
// deliberately no way (not even for the admin) to shorten or remove it.
// ---------------------------------------------------------------------------
const VAULT_LOCK_OPTIONS_MS = { '1h': 3600e3, '6h': 6 * 3600e3, '12h': 12 * 3600e3, '1d': 86400e3, '3d': 3 * 86400e3, '7d': 7 * 86400e3 };
function vaultPublic(u) {
  const v = u.vault || { coins: 0, items: {}, lockUntil: 0 };
  const items = {};
  for (const [id, q] of Object.entries(v.items || {})) if (q > 0) items[id] = q;
  return { coins: v.coins || 0, items, lockUntil: v.lockUntil && v.lockUntil > Date.now() ? v.lockUntil : 0 };
}
function vaultLocked(u) { return !!(u.vault && u.vault.lockUntil && u.vault.lockUntil > Date.now()); }
function vaultLockMsg(u) {
  const mins = Math.max(1, Math.ceil((u.vault.lockUntil - Date.now()) / 60e3));
  const h = Math.floor(mins / 60), m = mins % 60;
  return `Your vault is locked for another ${h > 0 ? h + 'h ' : ''}${m}m.`;
}

function handleVaultCoins(username, msg, dir) {
  const ws = usernameToSocket.get(username);
  const amount = Math.floor(Number(msg.amount));
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1e13) return send(ws, { type: 'error', message: 'Enter a valid amount.' });
  const u = ensureUser(username);
  const before = u.coins;
  if (dir > 0) {
    if (amount > u.coins) return send(ws, { type: 'error', message: "You don't have that many coins." });
    u.coins -= amount; u.vault.coins += amount;
  } else {
    if (vaultLocked(u)) return send(ws, { type: 'error', message: vaultLockMsg(u) });
    if (amount > u.vault.coins) return send(ws, { type: 'error', message: "Your vault doesn't have that many coins." });
    u.vault.coins -= amount; u.coins += amount;
  }
  persistUser(username, { type: dir > 0 ? 'vault_deposit' : 'vault_withdraw', amount: u.coins - before, balanceBefore: before, balanceAfter: u.coins });
  syncAccount(username);
  send(ws, { type: 'toast', message: dir > 0 ? `Put ${amount.toLocaleString()} coins in your vault.` : `Took ${amount.toLocaleString()} coins out of your vault.` });
}

function handleVaultItems(username, msg, dir) {
  const ws = usernameToSocket.get(username);
  const items = sanitizeItems(msg.items, 200);
  if (!items || !items.length) return send(ws, { type: 'error', message: 'Pick at least one pet.' });
  const u = ensureUser(username);
  const need = {};
  for (const id of items) need[id] = (need[id] || 0) + 1;
  if (dir > 0) {
    if (!ownsAll(username, items)) return send(ws, { type: 'error', message: "You don't own those pets (or they're in a withdrawal request)." });
    removeItems(username, items);
    for (const [id, q] of Object.entries(need)) u.vault.items[id] = (u.vault.items[id] || 0) + q;
  } else {
    if (vaultLocked(u)) return send(ws, { type: 'error', message: vaultLockMsg(u) });
    if (!Object.entries(need).every(([id, q]) => (u.vault.items[id] || 0) >= q)) return send(ws, { type: 'error', message: "Those pets aren't in your vault." });
    for (const [id, q] of Object.entries(need)) u.vault.items[id] -= q;
    addItems(username, items);
  }
  const touched = Object.keys(need);
  const value = itemsValue(items);
  persistUser(username, { type: dir > 0 ? 'vault_deposit_items' : 'vault_withdraw_items', amount: 0, itemsTouched: touched, vaultItemsTouched: touched, value: dir > 0 ? -value : value });
  syncAccount(username);
  send(ws, { type: 'toast', message: dir > 0 ? `Put ${items.length} pet${items.length === 1 ? '' : 's'} in your vault.` : `Took ${items.length} pet${items.length === 1 ? '' : 's'} out of your vault.` });
}

function handleVaultLock(username, msg) {
  const ws = usernameToSocket.get(username);
  const dur = VAULT_LOCK_OPTIONS_MS[msg.duration];
  if (!dur) return send(ws, { type: 'error', message: 'Pick a lock time.' });
  const u = ensureUser(username);
  const target = Date.now() + dur;
  const current = vaultLocked(u) ? u.vault.lockUntil : 0;
  if (target <= current) return send(ws, { type: 'error', message: "Your vault is already locked for longer than that. A lock can only be extended." });
  u.vault.lockUntil = target;
  persistUser(username, { type: 'vault_lock', amount: 0, reason: `Locked until ${new Date(target).toISOString()}` });
  syncAccount(username);
  send(ws, { type: 'toast', message: 'Vault locked.' });
}

// ---------------------------------------------------------------------------
// PROMO CODES - admin creates, players redeem once each, coins only.
// ---------------------------------------------------------------------------
const promoLastTry = new Map();
function normalizePromo(raw) {
  const c = String(raw || '').trim().toLowerCase();
  return /^[a-z0-9_-]{3,32}$/.test(c) ? c : null;
}
async function handlePromoRedeem(username, msg) {
  const ws = usernameToSocket.get(username);
  const now = Date.now();
  if (now - (promoLastTry.get(username) || 0) < 1500) return send(ws, { type: 'promo:result', ok: false, message: 'Slow down a little.' });
  promoLastTry.set(username, now);
  const code = normalizePromo(msg.code);
  if (!code) return send(ws, { type: 'promo:result', ok: false, message: "That code doesn't exist." });
  let res;
  try { res = await db.redeemPromoCode(code, username); }
  catch (err) { console.error('[promo] redeem failed:', err.message); return send(ws, { type: 'promo:result', ok: false, message: 'Something went wrong, try again.' }); }
  if (!res.ok) return send(ws, { type: 'promo:result', ok: false, message: res.error });
  const u = ensureUser(username);
  const before = u.coins;
  u.coins += res.coins;
  persistUser(username, { type: 'promo_redeem', amount: res.coins, balanceBefore: before, balanceAfter: u.coins, reason: `Code ${code}` });
  syncAccount(username);
  send(ws, { type: 'promo:result', ok: true, coins: res.coins, message: `You got ${res.coins.toLocaleString()} coins!` });
}
async function handleAdminPromoCreate(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const ws = usernameToSocket.get(username);
  const code = normalizePromo(msg.code);
  const coins = Math.floor(Number(msg.coins));
  const maxUses = Math.floor(Number(msg.maxUses));
  const hours = Number(msg.expiresHours) || 0;
  if (!code) return send(ws, { type: 'error', message: 'Code must be 3-32 letters, numbers, - or _.' });
  if (!Number.isFinite(coins) || coins <= 0 || coins > 1e12) return send(ws, { type: 'error', message: 'Enter a coin amount.' });
  if (!Number.isFinite(maxUses) || maxUses <= 0 || maxUses > 1e6) return send(ws, { type: 'error', message: 'Enter how many people can use it.' });
  const ok = await db.createPromoCode({ code, coins, maxUses, expiresAt: hours > 0 ? Date.now() + hours * 3600e3 : null, createdBy: username }).catch((e) => { console.error(e); return false; });
  if (!ok) return send(ws, { type: 'error', message: 'That code already exists.' });
  send(ws, { type: 'toast', message: `Code "${code}" created.` });
  handleAdminPromoList(username);
}
async function handleAdminPromoList(username) {
  if (username !== ADMIN_USERNAME) return;
  const codes = await db.listPromoCodes().catch(() => []);
  send(usernameToSocket.get(username), { type: 'admin:promoList', codes });
}
async function handleAdminPromoToggle(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const code = normalizePromo(msg.code);
  if (!code) return;
  await db.setPromoActive(code, !!msg.active).catch(() => {});
  handleAdminPromoList(username);
}

// ---------------------------------------------------------------------------
// ADMIN: DELETE EVERYTHING (changing games). Empties the whole database -
// accounts, pets, coins, stats, history - logs everyone out, and clears all
// in-memory state. No backup. Database first; memory only changes if that
// succeeds.
// ---------------------------------------------------------------------------
const WIPE_CONFIRM_PHRASE = 'DELETE EVERYTHING';
let wipeInProgress = false;
async function handleAdminWipeAll(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const ws = usernameToSocket.get(username);
  if (String(msg.confirm || '') !== WIPE_CONFIRM_PHRASE) return send(ws, { type: 'error', message: `Type ${WIPE_CONFIRM_PHRASE} exactly to confirm.` });
  if (wipeInProgress) return;
  wipeInProgress = true;
  const lockdownBefore = siteLockdown;
  siteLockdown = true; // block every money/pet action while this runs
  try {
    await db.wipeEverything();
  } catch (err) {
    siteLockdown = lockdownBefore; wipeInProgress = false;
    console.error('[wipe] FAILED, nothing was changed:', err);
    return send(ws, { type: 'error', message: 'Wipe failed - nothing was changed. Check the server logs.' });
  }
  for (const g of giveaways.values()) clearTimeout(g.timer);
  giveaways.clear();
  withdrawRequests = [];
  for (const id of Object.keys(shopStock)) delete shopStock[id];
  // --- forget every account and everything about them ---
  users.clear();
  ignoreLists.clear();
  sessionTokens.clear();
  discordRoleSyncedLevel.clear();
  pendingDiscordStates.clear();
  rainPool = RAIN_BASE;
  rainClaimants.clear();
  chatHistory = [];
  siteLockdown = lockdownBefore;
  wipeInProgress = false;
  console.log(`[wipe] ${username} deleted EVERYTHING (all accounts, pets, coins, stats, history).`);
  // --- log everyone out; their pages reload as brand-new visitors ---
  broadcast({ type: 'site:reset', message: 'The site has been reset for a new game. Please log in again.' });
  setTimeout(() => {
    for (const client of wss.clients) { try { client.close(4000, 'site reset'); } catch (e) {} }
    socketToUsername.clear(); usernameToSocket.clear();
  }, 800);
}

// ---------------------------------------------------------------------------
// GIVEAWAYS - a player gives away pets; every pet is its own giveaway with a
// timer. Anyone (except the host) can join, optionally only from a minimum
// level. Random winner when it ends; if nobody joined, the pet goes back.
// Pets are held in escrow the whole time, so a restart refunds the host.
// ---------------------------------------------------------------------------
const GIVEAWAY_DURATIONS_MIN = new Set([5, 10, 15, 30, 60]);
const GIVEAWAY_MAX_PER_HOST = 10;
const GIVEAWAY_MAX_TOTAL = 60;
const giveaways = new Map(); // id -> { id, host, petId, minLevel, endsAt, entrants:Set, escrowId, timer }
function giveawayPublicList() {
  return [...giveaways.values()].sort((a, b) => a.endsAt - b.endsAt).map((g) => ({
    id: g.id, host: g.host, petId: g.petId, minLevel: g.minLevel, endsAt: g.endsAt,
    entrants: g.entrants.size, joined: [...g.entrants].slice(0, 500),
  }));
}
function broadcastGiveaways() { broadcast({ type: 'giveaway:list', giveaways: giveawayPublicList(), now: Date.now() }); }

function handleGiveawayCreate(username, msg) {
  const ws = usernameToSocket.get(username);
  const items = sanitizeItems(msg.items, GIVEAWAY_MAX_PER_HOST);
  if (!items || !items.length) return send(ws, { type: 'error', message: `Pick 1-${GIVEAWAY_MAX_PER_HOST} pets to give away.` });
  const minutes = Math.floor(Number(msg.minutes));
  if (!GIVEAWAY_DURATIONS_MIN.has(minutes)) return send(ws, { type: 'error', message: 'Pick how long the giveaway lasts.' });
  const minLevel = Math.max(0, Math.min(100, Math.floor(Number(msg.minLevel) || 0)));
  const mine = [...giveaways.values()].filter((g) => g.host === username).length;
  if (mine + items.length > GIVEAWAY_MAX_PER_HOST) return send(ws, { type: 'error', message: `You can have at most ${GIVEAWAY_MAX_PER_HOST} giveaways running.` });
  if (giveaways.size + items.length > GIVEAWAY_MAX_TOTAL) return send(ws, { type: 'error', message: 'Too many giveaways running right now - try again soon.' });
  if (!ownsAll(username, items)) return send(ws, { type: 'error', message: "You don't own those pets (or they're in a withdrawal request)." });
  removeItems(username, items);
  const endsAt = Date.now() + minutes * 60e3;
  const created = items.map((petId) => {
    const id = newEscrowId('giveaway');
    const g = { id, host: username, petId, minLevel, endsAt, entrants: new Set(), escrowId: id };
    g.timer = setTimeout(() => resolveGiveaway(id), minutes * 60e3);
    giveaways.set(id, g);
    return g;
  });
  persistUser(username, {
    type: 'giveaway_create', amount: 0, itemsTouched: [...new Set(items)], value: -itemsValue(items),
    reason: `${items.length} giveaway${items.length === 1 ? '' : 's'}`,
    escrowAdd: created.map((g) => ({ id: g.escrowId, kind: 'giveaway', items: [g.petId], coins: 0 })),
  });
  syncAccount(username);
  broadcastGiveaways();
  send(ws, { type: 'toast', message: `Started ${items.length} giveaway${items.length === 1 ? '' : 's'}!` });
}

function handleGiveawayJoin(username, msg) {
  const ws = usernameToSocket.get(username);
  const g = giveaways.get(String(msg.id || ''));
  if (!g) return send(ws, { type: 'error', message: 'That giveaway already ended.' });
  if (g.host === username) return send(ws, { type: 'error', message: "You can't join your own giveaway." });
  if (g.entrants.has(username)) return;
  const lvl = levelForXp(ensureUser(username).stats.xp || 0);
  if (lvl < g.minLevel) return send(ws, { type: 'error', message: `You need level ${g.minLevel} to join this giveaway (you're level ${lvl}).` });
  g.entrants.add(username);
  broadcastGiveaways();
}

function resolveGiveaway(id) {
  const g = giveaways.get(id);
  if (!g) return;
  giveaways.delete(id);
  clearTimeout(g.timer);
  const entrants = [...g.entrants].filter((n) => users.has(n));
  const value = itemsValue([g.petId]);
  if (!entrants.length) {
    addItems(g.host, [g.petId]);
    persistUser(g.host, { type: 'giveaway_return', amount: 0, itemsTouched: [g.petId], escrowRelease: [g.escrowId], value, reason: 'Nobody joined' });
    syncAccount(g.host);
    send(usernameToSocket.get(g.host), { type: 'toast', message: `Nobody joined your ${petCatalog[g.petId] ? petCatalog[g.petId].name : 'pet'} giveaway, so it came back to you.` });
    broadcast({ type: 'giveaway:ended', id, petId: g.petId, host: g.host, winner: null, entrants: 0 });
    broadcastGiveaways();
    return;
  }
  const winner = entrants[crypto.randomInt(entrants.length)];
  addItems(winner, [g.petId]);
  persistUser(winner, { type: 'giveaway_win', amount: 0, itemsTouched: [g.petId], escrowRelease: [g.escrowId], value, reason: `From ${g.host}` });
  syncAccount(winner);
  createNotification({ username: winner, type: 'giveaway_win', title: '🎁 You won a giveaway', message: `You won ${petCatalog[g.petId] ? petCatalog[g.petId].name : 'a pet'} from ${g.host}'s giveaway!`, refId: id });
  broadcast({ type: 'giveaway:ended', id, petId: g.petId, host: g.host, winner, entrants: entrants.length });
  broadcastGiveaways();
}

async function handleIgnoreAdd(username, msg) {
  const errTo = usernameToSocket.get(username);
  const raw = String(msg.username || '').trim();
  if (!isValidUsername(raw)) return send(errTo, { type: 'error', message: "That isn't a valid username." });
  const target = resolveExistingUsername(raw);
  if (!target) return send(errTo, { type: 'error', message: 'No player with that name.' });
  if (target === username) return send(errTo, { type: 'error', message: "You can't ignore yourself." });
  const set = ignoreLists.get(username) || new Set();
  if (set.has(target)) return syncAccount(username);
  if (set.size >= MAX_IGNORES) return send(errTo, { type: 'error', message: `You can ignore at most ${MAX_IGNORES} players.` });
  await db.addIgnore(username, target);
  set.add(target);
  ignoreLists.set(username, set);
  syncAccount(username);
  send(errTo, { type: 'toast', message: `Ignoring ${target}. You won't see their chat messages.` });
}

async function handleIgnoreRemove(username, msg) {
  const raw = String(msg.username || '').trim().toLowerCase();
  const set = ignoreLists.get(username);
  // Match without caring about upper/lower case - a name clicked in chat or
  // on a profile can be spelled with different casing than the stored one,
  // and an exact match used to make Unignore silently do nothing.
  const matches = set ? [...set].filter((n) => n.toLowerCase() === raw) : [];
  if (!raw || matches.length === 0) return syncAccount(username);
  await db.removeIgnore(username, raw);
  for (const n of matches) set.delete(n);
  const target = matches[0];
  syncAccount(username);
  send(usernameToSocket.get(username), { type: 'toast', message: `${target} is no longer ignored.` });
}

// ---------------------------------------------------------------------------
// Tipping - coins and/or items, direct account-to-account transfer.
// ---------------------------------------------------------------------------
function handleTipSend(username, msg) {
  const rawTo = String(msg.toUsername || '').trim();
  const coins = Math.max(0, Math.floor(Number(msg.coins) || 0));
  const errTo = usernameToSocket.get(username);
  const items = msg.items == null ? [] : sanitizeItems(msg.items, 100);
  if (!items) return send(errTo, { type: 'error', message: 'Invalid items.' });

  if (!rawTo) return send(errTo, { type: 'error', message: 'Pick someone else to tip.' });
  if (!isValidUsername(rawTo)) return send(errTo, { type: 'error', message: "That isn't a valid Roblox username." });
  // Route to a real, already-known account if one matches case-insensitively
  // (e.g. typing "alice" when the real account is "Alice") - otherwise this
  // silently creates a brand-new phantom account under whatever casing was
  // typed, the coins/items land there instead of the real person, and it
  // looks to the sender like the tip "went through" while the intended
  // recipient's balance never actually changes. Only fall back to creating
  // a fresh account if genuinely nobody matches - that's the intentional
  // "tip a friend before they've ever logged in" case.
  const toUsername = resolveExistingUsername(rawTo) || rawTo;
  if (toUsername.toLowerCase() === username.toLowerCase()) return send(errTo, { type: 'error', message: 'Pick someone else to tip.' });
  if (coins <= 0 && !items.length) return send(errTo, { type: 'error', message: 'Add some coins or items to tip.' });

  const u = ensureUser(username);
  const target = ensureUser(toUsername); // creates their account if they haven't logged in yet this session - the tip just waits for them
  if (coins > 0 && u.coins < coins) return send(errTo, { type: 'error', message: "You don't have that many coins." });
  if (items.length && !ownsAll(username, items)) return send(errTo, { type: 'error', message: "You don't own all of those items." });

  const senderBefore = u.coins;
  const targetBefore = target.coins;
  if (coins > 0) { u.coins -= coins; target.coins += coins; }
  let senderItemsTouched = [];
  if (items.length) {
    // The giveaway account's stock never runs out - only credit the
    // recipient, don't deduct from the sender, so it stays at 99x forever.
    if (username.toLowerCase() !== GIVEAWAY_USERNAME.toLowerCase()) { removeItems(username, items); senderItemsTouched = [...new Set(items)]; }
    addItems(toUsername, items);
  }

  const tipValue = coins + itemsValue(items);
  persistUser(username, { type: 'tip_send', amount: -coins, balanceBefore: senderBefore, balanceAfter: u.coins, itemsTouched: senderItemsTouched, reason: `To ${toUsername}`, value: -tipValue });
  persistUser(toUsername, { type: 'tip_receive', amount: coins, balanceBefore: targetBefore, balanceAfter: target.coins, itemsTouched: [...new Set(items)], reason: `From ${username}`, value: tipValue });
  syncAccount(username);
  syncAccount(toUsername);

  const parts = [];
  if (coins > 0) parts.push(`${coins.toLocaleString()} coins`);
  if (items.length) parts.push(`${items.length} item${items.length > 1 ? 's' : ''}`);
  pushChatMessage({ username: 'System', text: `${username} tipped ${toUsername} ${parts.join(' + ')}`, timestamp: Date.now(), system: true });
}

async function fetchWithRetry(url, attempts = 3, options = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    // Without a hard timeout here, a slow/unresponsive Roblox API can hang
    // this call indefinitely - and with no response ever written, Railway's
    // own edge eventually gives up waiting and substitutes its own generic
    // error page (an empty 404 with no trace of anything this file ever
    // wrote), which looks nothing like the actual 502 this function's own
    // caller would otherwise send. Bounding each attempt means we always
    // get the chance to respond with a real, informative error ourselves.
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000);
    try {
      const r = await fetch(url, { ...options, signal: controller.signal });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r;
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1) await new Promise((resolve) => setTimeout(resolve, 500 * (i + 1)));
    } finally {
      clearTimeout(timeoutId);
    }
  }
  throw lastErr;
}

// Exact username -> {id, name} lookup. Uses Roblox's dedicated exact-match
// endpoint instead of the fuzzy /users/search endpoint, which is heavily
// rate-limited and was the main reason profile pictures sometimes never
// loaded (and why short names could fail to resolve at all).
async function robloxLookupUsernames(usernames){
  const names = [...new Set(usernames.map((n) => String(n || '').trim()).filter(Boolean))].slice(0, 100);
  if (!names.length) return new Map();
  const r = await fetchWithRetry('https://users.roblox.com/v1/usernames/users', 3, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    body: JSON.stringify({ usernames: names, excludeBannedUsers: false }),
  });
  const data = await r.json();
  const out = new Map(); // lowercase requested name -> {id, name}
  for (const u of (data.data || [])) {
    const key = String(u.requestedUsername || u.name || '').toLowerCase();
    if (key && u.id) out.set(key, { id: u.id, name: u.name });
  }
  return out;
}
async function robloxResolveUsername(username){
  const map = await robloxLookupUsernames([username]);
  return map.get(String(username || '').trim().toLowerCase()) || null;
}

// Headshots for many user ids at once. Roblox sometimes answers "Pending"
// (picture still being generated) with no URL - those get one retry
// instead of being treated as "no avatar".
async function robloxHeadshots(userIds){
  const out = new Map();
  let todo = [...new Set(userIds)].slice(0, 100);
  for (let attempt = 0; attempt < 2 && todo.length; attempt++) {
    if (attempt > 0) await new Promise((r) => setTimeout(r, 1500));
    const r = await fetchWithRetry(`https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds=${todo.join(',')}&size=150x150&format=Png&isCircular=false`);
    const data = await r.json();
    const pending = [];
    for (const entry of (data.data || [])) {
      if (entry.imageUrl && entry.state === 'Completed') out.set(entry.targetId, entry.imageUrl);
      else if (entry.state === 'Pending') pending.push(entry.targetId);
    }
    todo = pending;
  }
  return out;
}

// Avatar requests from every browser are coalesced into small batches, so a
// page showing 30 names makes 2 Roblox calls instead of 60.
const AVATAR_NOTFOUND_TTL_MS = 10 * 60 * 1000;
const avatarQueue = new Map(); // lowercase -> { name, waiters: [] }
let avatarQueueTimer = null;
function requestAvatar(username){
  return new Promise((resolve, reject) => {
    const key = username.toLowerCase();
    let entry = avatarQueue.get(key);
    if (!entry) { entry = { name: username, waiters: [] }; avatarQueue.set(key, entry); }
    entry.waiters.push({ resolve, reject });
    if (!avatarQueueTimer) avatarQueueTimer = setTimeout(flushAvatarQueue, 40);
  });
}
async function flushAvatarQueue(){
  avatarQueueTimer = null;
  const batch = [...avatarQueue.values()].slice(0, 100);
  for (const e of batch) avatarQueue.delete(e.name.toLowerCase());
  if (avatarQueue.size) avatarQueueTimer = setTimeout(flushAvatarQueue, 40);
  try {
    const idsByName = await robloxLookupUsernames(batch.map((e) => e.name));
    const ids = [...idsByName.values()].map((m) => m.id);
    const urls = ids.length ? await robloxHeadshots(ids) : new Map();
    for (const e of batch) {
      const match = idsByName.get(e.name.toLowerCase());
      const result = { userId: match ? match.id : null, imageUrl: match ? (urls.get(match.id) || null) : null, found: !!match };
      if (result.imageUrl || !result.found) {
        avatarCache.set(e.name.toLowerCase(), { ...result, cachedAt: Date.now(), ttl: result.found ? AVATAR_CACHE_TTL_MS : AVATAR_NOTFOUND_TTL_MS });
      }
      e.waiters.forEach((w) => w.resolve(result));
    }
  } catch (err) {
    for (const e of batch) e.waiters.forEach((w) => w.reject(err));
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  // These are read-only public Roblox lookups triggered by the client's own
  // pages (GitHub Pages, wherever it's hosted) - nothing sensitive, so allow
  // any origin to call them.
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if(req.method === 'OPTIONS'){ res.writeHead(204); res.end(); return; }

  if(url.pathname === '/roblox/resolve'){
    const username = url.searchParams.get('username') || '';
    try{
      const match = await robloxResolveUsername(username);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ found: !!match, userId: match ? match.id : null }));
    } catch(e){
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ found: false, userId: null, error: 'roblox_unreachable' }));
    }
    return;
  }

  if(url.pathname === '/roblox/avatar'){
    const username = url.searchParams.get('username') || '';
    const cacheKey = username.toLowerCase();
    if (!/^[A-Za-z0-9_ ]{1,40}$/.test(username)) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ imageUrl: null, found: false }));
      return;
    }
    const cached = avatarCache.get(cacheKey);
    if (cached && Date.now() - cached.cachedAt < (cached.ttl || AVATAR_CACHE_TTL_MS)) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=600' });
      res.end(JSON.stringify({ imageUrl: cached.imageUrl, userId: cached.userId, found: cached.found !== false }));
      return;
    }
    try{
      const result = await requestAvatar(username);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': result.imageUrl ? 'public, max-age=600' : 'no-store' });
      res.end(JSON.stringify(result));
    } catch(e){
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ imageUrl: null, error: 'roblox_unreachable' }));
    }
    return;
  }

  if(url.pathname === '/roblox/verify'){
    const username = url.searchParams.get('username') || '';
    const code = url.searchParams.get('code') || '';
    try{
      const match = await robloxResolveUsername(username);
      if(!match){ res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: `Couldn't find a Roblox account named "${username}".` })); return; }
      const profileR = await fetchWithRetry(`https://users.roblox.com/v1/users/${match.id}`);
      const profileData = await profileR.json();
      const bio = profileData.description || '';
      if(!bio.includes(code)){ res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: "Didn't find that code in your bio yet. Make sure you saved it, then try again." })); return; }
      // Use Roblox's own canonical username (match.name), not whatever
      // casing the person happened to type - otherwise "CoolGuy" and
      // "coolguy" silently become two different accounts with separate
      // balances/inventories. The client is told this name back so it logs
      // in (and saves its session) under the same canonical string too.
      const canonicalUsername = match.name;
      const token = issueSessionToken(canonicalUsername);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, userId: match.id, username: canonicalUsername, token }));
    } catch(e){
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, reason: "Couldn't reach Roblox right now — try again in a few seconds." }));
    }
    return;
  }

  // TEMPORARY — testing-only bypass matching the client's skip-verify button.
  // Issues a real session token with zero Roblox check. Gated behind
  // ENABLE_DEV_BYPASS (off by default) rather than reachable in production -
  // see the flag's definition near the top of this file. Still fully
  // removable later (Phase 3) once you're confident nothing depends on it.
  // ---------------------------------------------------------------------
  // Roblox OAuth 2.0 ("Sign in with Roblox") - an addition alongside the
  // existing bio-verification flow above, not a replacement for it. Uses
  // the standard authorization code flow WITHOUT PKCE, which Roblox's own
  // official sample app does too - PKCE is only required for "public"
  // clients that can't hold a secret safely (e.g. a mobile app or SPA);
  // this server holds ROBLOX_OAUTH_CLIENT_SECRET itself and never exposes
  // it to the browser, making it a "confidential" client, same as Roblox's
  // own reference implementation.
  //
  // Requires three environment variables to actually work:
  //   ROBLOX_OAUTH_CLIENT_ID, ROBLOX_OAUTH_CLIENT_SECRET - from Roblox's
  //     Creator Dashboard -> OAuth 2.0 Apps (create an app, add the
  //     'openid' and 'profile' scopes).
  //   PUBLIC_BASE_URL - this server's own real public URL (e.g.
  //     https://your-app.up.railway.app, no trailing slash) - used to
  //     build the redirect_uri consistently. Register
  //     PUBLIC_BASE_URL + '/roblox/oauth/callback' as the app's exact
  //     Redirect URL in Roblox's dashboard. Keeping this in an env var
  //     instead of hardcoded in the client means a Railway domain change
  //     only ever needs this one variable updated, not a code change.
  // If any of these aren't set, the routes below fail cleanly with a
  // clear error redirect instead of crashing.
  // ---------------------------------------------------------------------
  if(url.pathname === '/discord/oauth/callback'){
    const back = (params) => {
      const dest = new URL(PUBLIC_BASE_URL || `http://${req.headers.host}/`);
      for (const [k, v] of Object.entries(params)) dest.searchParams.set(k, v);
      res.writeHead(302, { Location: dest.toString() });
      res.end();
    };
    const code = url.searchParams.get('code');
    const state = url.searchParams.get('state');
    const pending = state && pendingDiscordStates.get(state);
    if (state) pendingDiscordStates.delete(state); // one-time use
    if (url.searchParams.get('error')) return back({ discord: 'cancelled' });
    if (!discordConfigured() || !code || !pending || pending.expires < Date.now()) return back({ discord: 'expired' });
    try {
      const tokenRes = await fetchWithRetry('https://discord.com/api/oauth2/token', 2, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          client_id: DISCORD_CLIENT_ID, client_secret: DISCORD_CLIENT_SECRET,
          grant_type: 'authorization_code', code, redirect_uri: `${PUBLIC_BASE_URL}/discord/oauth/callback`,
        }).toString(),
      });
      const tokenData = await tokenRes.json();
      if (!tokenData.access_token) return back({ discord: 'failed' });
      const meRes = await fetchWithRetry('https://discord.com/api/users/@me', 2, { headers: { Authorization: `Bearer ${tokenData.access_token}` } });
      const me = await meRes.json();
      if (!me || !me.id) return back({ discord: 'failed' });
      const link = { id: String(me.id), username: String(me.global_name || me.username || 'Discord user').slice(0, 64), avatar: me.avatar || null };
      const ok = await db.setDiscordLink(pending.username, link);
      if (!ok) return back({ discord: 'taken' });
      const u = ensureUser(pending.username);
      u.discord = link;
      discordRoleSyncedLevel.delete(pending.username);
      scheduleDiscordRoleSync(pending.username, 500);
      syncAccount(pending.username);
      return back({ discord: 'linked' });
    } catch (err) {
      console.error('[discord] OAuth callback failed:', err.message);
      return back({ discord: 'failed' });
    }
  }

  if(url.pathname === '/roblox/oauth/login'){
    if (!ROBLOX_OAUTH_CLIENT_ID || !PUBLIC_BASE_URL) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Roblox OAuth is not configured on this server yet (missing ROBLOX_OAUTH_CLIENT_ID / PUBLIC_BASE_URL).');
      return;
    }
    // A one-time, short-lived nonce, checked again on callback - standard
    // CSRF protection for the redirect-based OAuth flow, without needing
    // any session/cookie system (this app doesn't have one).
    const state = crypto.randomBytes(16).toString('hex');
    pendingOAuthStates.set(state, Date.now() + 10 * 60 * 1000);
    const redirectUri = `${PUBLIC_BASE_URL}/roblox/oauth/callback`;
    const authorizeUrl = new URL('https://apis.roblox.com/oauth/v1/authorize');
    authorizeUrl.searchParams.set('client_id', ROBLOX_OAUTH_CLIENT_ID);
    authorizeUrl.searchParams.set('redirect_uri', redirectUri);
    authorizeUrl.searchParams.set('scope', 'openid profile');
    authorizeUrl.searchParams.set('response_type', 'code');
    authorizeUrl.searchParams.set('state', state);
    res.writeHead(302, { Location: authorizeUrl.toString() });
    res.end();
    return;
  }

  if(url.pathname === '/roblox/oauth/callback'){
    const redirectHome = (params) => {
      const dest = new URL(PUBLIC_BASE_URL || '/');
      for (const [k, v] of Object.entries(params)) dest.searchParams.set(k, v);
      res.writeHead(302, { Location: dest.toString() });
      res.end();
    };
    const code = url.searchParams.get('code');
    const state = url.searchParams.get('state');
    const stateExpiry = state && pendingOAuthStates.get(state);
    if (state) pendingOAuthStates.delete(state); // one-time use either way
    if (!code || !stateExpiry || stateExpiry < Date.now()) {
      return redirectHome({ oauth_error: 'state' });
    }
    if (!ROBLOX_OAUTH_CLIENT_ID || !ROBLOX_OAUTH_CLIENT_SECRET || !PUBLIC_BASE_URL) {
      return redirectHome({ oauth_error: 'config' });
    }
    try {
      const redirectUri = `${PUBLIC_BASE_URL}/roblox/oauth/callback`;
      const tokenRes = await fetchWithRetry('https://apis.roblox.com/oauth/v1/token', 2, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          client_id: ROBLOX_OAUTH_CLIENT_ID,
          client_secret: ROBLOX_OAUTH_CLIENT_SECRET,
          grant_type: 'authorization_code',
          code,
          redirect_uri: redirectUri,
        }).toString(),
      });
      const tokenData = await tokenRes.json();
      if (!tokenData.access_token) return redirectHome({ oauth_error: 'token' });

      const userRes = await fetchWithRetry('https://apis.roblox.com/oauth/v1/userinfo', 2, {
        headers: { Authorization: `Bearer ${tokenData.access_token}` },
      });
      const userInfo = await userRes.json();
      // Prefer the standard OIDC username claim, with reasonable fallbacks
      // in case the exact claim name differs from what's documented.
      const canonicalUsername = userInfo.preferred_username || userInfo.nickname || userInfo.name;
      if (!canonicalUsername) return redirectHome({ oauth_error: 'userinfo' });

      const token = issueSessionToken(canonicalUsername);
      redirectHome({ oauth_token: token, oauth_username: canonicalUsername });
    } catch (e) {
      redirectHome({ oauth_error: 'network' });
    }
    return;
  }

  if (url.pathname === '/discord/interactions' && req.method === 'POST') return handleDiscordInteraction(req, res);

  if(url.pathname === '/dev/skip-login'){
    if (!ENABLE_DEV_BYPASS) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false }));
      return;
    }
    const username = (url.searchParams.get('username') || '').slice(0, 40) || 'testuser';
    const token = issueSessionToken(username);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, token }));
    return;
  }

  // Public privacy policy page - no login/session required, works from a
  // direct link, and survives a refresh since it's a real server route
  // rather than client-side SPA state. Intentionally checked before the
  // catch-all index.html fallback below.
  if(url.pathname === '/privacy'){
    return servePrivacyHtml(req, res);
  }

  if(url.pathname.startsWith('/assets/img/')){
    return serveImageAsset(url.pathname, res);
  }

  serveIndexHtml(req, res);
});

// Images used to be base64-embedded directly in index.html (see the ~99%
// of that file's size that used to be base64 image data before the egress
// audit that led to this). They're now separate files on disk instead,
// named by a hash of their own content - which means two things for free:
// identical images naturally collapse onto the same filename (no explicit
// dedup bookkeeping needed), and it's always safe to cache a given
// filename forever, since the *only* way its content could ever change is
// for it to get a new name.
const ASSET_IMG_DIR = path.join(__dirname, 'assets', 'img');
// Filenames are always exactly a hex hash + .png/.webp (see how they were
// generated) - this is the only thing ever allowed to reach fs.readFile
// below, so a request can never escape this directory via '..', absolute
// paths, or anything else.
const SAFE_ASSET_FILENAME = /^[a-f0-9]{16,64}\.(png|webp)$/;
// Pet pictures for the new game live in assets/img/bgsi/<pet-slug>.webp
// (slug = lowercase letters, digits and dashes only - still no way out of
// the folder).
const SAFE_PET_FILENAME = /^bgsi\/[a-z0-9]+(?:-[a-z0-9]+)*\.webp$/;

function serveImageAsset(pathname, res) {
  let filename;
  try { filename = decodeURIComponent(pathname.slice('/assets/img/'.length)); } catch (e) { filename = ''; }
  if (!SAFE_ASSET_FILENAME.test(filename) && !SAFE_PET_FILENAME.test(filename)) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Bad asset request.');
    return;
  }
  fs.readFile(path.join(ASSET_IMG_DIR, filename), (err, data) => {
    if (err) {
      // Never let a browser or cache in between remember a failure - the
      // file may well be there on the next try (e.g. right after a deploy).
      const missing = err.code === 'ENOENT';
      if (!missing) console.error('[assets] could not read', filename, err.code);
      res.writeHead(missing ? 404 : 503, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
      res.end(missing ? 'Not found.' : 'Busy, try again.');
      return;
    }
    res.writeHead(200, {
      'Content-Type': filename.endsWith('.webp') ? 'image/webp' : 'image/png',
      // Safe to cache forever, unconditionally - see the comment above on
      // why a given filename's content can never change.
      // Pet pictures keep their name if they're ever replaced, so those
      // only cache for a day.
      'Cache-Control': filename.startsWith('bgsi/') ? 'public, max-age=86400' : 'public, max-age=31536000, immutable',
    });
    res.end(data);
  });
}

// index.html used to be tens of MB (every pet's artwork was base64-embedded
// directly in it - see the egress audit that led to both this and the
// separate /assets/img/ static files above). It's now a normal-sized file,
// but this caching/compression setup is still worth keeping: it costs
// nothing, and it means a refresh can come back as a ~0-byte 304 instead
// of re-sending the file at all.
//   - gzip further shrinks what's left (plain HTML/JS/CSS text now,
//     instead of mostly-incompressible base64 image data).
//   - An ETag (a hash of the file's own bytes) lets a browser that already
//     has this exact version just get back a 304 Not Modified with no body
//     at all, instead of re-downloading anything, on every refresh.
// Restarting the server (e.g. after deploying an updated index.html)
// naturally invalidates this cache and recomputes it fresh, so this can
// never serve stale content after a real deploy.
let cachedIndexGzip = null;
let cachedIndexRaw = null;
let cachedIndexEtag = null;

function loadIndexHtmlCache() {
  const indexPath = path.join(__dirname, 'index.html');
  const raw = fs.readFileSync(indexPath);
  cachedIndexRaw = raw;
  cachedIndexGzip = zlib.gzipSync(raw, { level: zlib.constants.Z_BEST_COMPRESSION });
  cachedIndexEtag = '"' + crypto.createHash('sha1').update(raw).digest('hex') + '"';
  console.log(`[boot] Cached index.html: ${raw.length.toLocaleString()} bytes raw -> ${cachedIndexGzip.length.toLocaleString()} bytes gzipped.`);
}

function serveIndexHtml(req, res) {
  try {
    if (!cachedIndexGzip) loadIndexHtmlCache(); // lazy fallback if boot-time load somehow didn't run yet
  } catch (err) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Could not load website.');
    return;
  }

  if (req.headers['if-none-match'] === cachedIndexEtag) {
    res.writeHead(304);
    res.end();
    return;
  }

  const acceptEncoding = req.headers['accept-encoding'] || '';
  const canGzip = acceptEncoding.includes('gzip');
  res.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'ETag': cachedIndexEtag,
    // max-age=0 + must-revalidate: the browser always checks back in with
    // the server (a cheap ETag check, see above) rather than assuming an
    // old copy is still fine - important since this file does change
    // across deploys, and we never want someone stuck looking at a stale
    // version just because a longer cache time hadn't expired yet.
    'Cache-Control': 'public, max-age=0, must-revalidate',
    ...(canGzip ? { 'Content-Encoding': 'gzip' } : {}),
  });
  res.end(canGzip ? cachedIndexGzip : cachedIndexRaw);
}

// Same idea as index.html's cache above, for the standalone public privacy
// policy page. Kept as its own separate static file (not part of the SPA)
// so it works from a direct URL, survives a refresh, and needs no login or
// client-side routing to render - a plain GET that always returns the same
// self-contained HTML page.
let cachedPrivacyGzip = null;
let cachedPrivacyRaw = null;
let cachedPrivacyEtag = null;

function loadPrivacyHtmlCache() {
  const privacyPath = path.join(__dirname, 'privacy.html');
  const raw = fs.readFileSync(privacyPath);
  cachedPrivacyRaw = raw;
  cachedPrivacyGzip = zlib.gzipSync(raw, { level: zlib.constants.Z_BEST_COMPRESSION });
  cachedPrivacyEtag = '"' + crypto.createHash('sha1').update(raw).digest('hex') + '"';
  console.log(`[boot] Cached privacy.html: ${raw.length.toLocaleString()} bytes raw -> ${cachedPrivacyGzip.length.toLocaleString()} bytes gzipped.`);
}

function servePrivacyHtml(req, res) {
  try {
    if (!cachedPrivacyGzip) loadPrivacyHtmlCache(); // lazy fallback if boot-time load somehow didn't run yet
  } catch (err) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Could not load privacy policy.');
    return;
  }

  if (req.headers['if-none-match'] === cachedPrivacyEtag) {
    res.writeHead(304);
    res.end();
    return;
  }

  const acceptEncoding = req.headers['accept-encoding'] || '';
  const canGzip = acceptEncoding.includes('gzip');
  res.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'ETag': cachedPrivacyEtag,
    'Cache-Control': 'public, max-age=0, must-revalidate',
    ...(canGzip ? { 'Content-Encoding': 'gzip' } : {}),
  });
  res.end(canGzip ? cachedPrivacyGzip : cachedPrivacyRaw);
}

const wss = new WebSocketServer({ server });

const HEARTBEAT_INTERVAL_MS = 30000;
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      // Didn't respond to the last ping - it's dead, but nothing would
      // otherwise have noticed (no clean close handshake ever arrives from
      // a connection that just silently dropped). terminate() forces the
      // 'close' event to fire, which runs the existing username/socket
      // cleanup below exactly the same as a normal disconnect would.
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === 'login') {
      const username = String(msg.username || '').slice(0, 40);
      if (!username) return;
      if (!checkSessionToken(username, msg.token)) {
        send(ws, { type: 'login:fail', reason: 'Your session expired or is invalid — please log in again.' });
        return;
      }
      if (siteLockdown && username !== ADMIN_USERNAME) {
        send(ws, { type: 'login:fail', reason: '🔒 The site is temporarily locked down for maintenance. Please try again later.' });
        return;
      }
      socketToUsername.set(ws, username);
      usernameToSocket.set(username, ws);
      // Re-check Discord roles on login (at most every 10 min per player) -
      // catches players who linked before joining the Discord server.
      if (discordRolesConfigured()) {
        const last = discordLoginCheckAt.get(username) || 0;
        if (Date.now() - last > 10 * 60 * 1000) {
          discordLoginCheckAt.set(username, Date.now());
          discordRoleSyncedLevel.delete(username);
          scheduleDiscordRoleSync(username, 2000);
        }
      }
      const u = ensureUser(username);
      const myPendingWithdrawals = myPendingRequests(username);
      send(ws, {
        type: 'login:ok',
        user: {
          username, coins: u.coins, inventory: u.inventory, stats: u.stats,
          specialRole: u.specialRole || null, mutedUntil: u.mutedUntil || 0,
          lockedItems: lockedItemsForUser(username),
          pendingWithdrawals: myPendingWithdrawals,
          discord: discordPublic(u.discord),
          ignored: ignoredListFor(username),
          vault: vaultPublic(u),
        },
      });
      send(ws, { type: 'giveaway:list', giveaways: giveawayPublicList() });
      send(ws, { type: 'catalog:values', values: liveValueDiff(), at: valuesUpdatedAt, full: true });
      send(ws, { type: 'exchange:stock', stock: shopStock });
      db.loadNotificationsForUser(username, NOTIFICATION_LOAD_LIMIT)
        .then((notifications) => send(ws, { type: 'notifications:list', notifications }))
        .catch((err) => console.error(`[notifications] Failed to load for ${username}:`, err.message));
      send(ws, { type: 'rain:state', ...rainPublicState() });
      send(ws, { type: 'chat:history', messages: chatHistory });
      if (username === ADMIN_USERNAME) {
        send(ws, { type: 'admin:withdrawList', requests: withdrawRequests.filter((r) => r.status === 'pending') });
      }
      return;
    }

    const username = socketToUsername.get(ws);
    if (!username) return; // must login first

    if (siteLockdown && username !== ADMIN_USERNAME && LOCKDOWN_BLOCKED_TYPES.has(msg.type)) {
      return send(ws, { type: 'error', message: '🔒 The site is temporarily locked down for maintenance. Please try again later.' });
    }

    // Any error inside a handler used to escape this callback and crash the
    // whole process. Now it's contained to
    // the one request, logged, and the player gets a message.
    try {
      const result = dispatchMessage(ws, username, msg);
      if (result && typeof result.catch === 'function') {
        result.catch((err) => {
          console.error(`[ws] async handler for ${msg.type} (${username}) failed:`, err);
          send(ws, { type: 'error', message: 'Something went wrong - please try again.' });
        });
      }
    } catch (err) {
      console.error(`[ws] handler for ${msg.type} (${username}) threw:`, err);
      send(ws, { type: 'error', message: 'Something went wrong - please try again.' });
    }
  });

  function dispatchMessage(ws, username, msg) {
    switch (msg && msg.type) {
      case 'rain:deposit': return handleRainDeposit(username, msg);
      case 'rain:claim': return handleRainClaim(username);
      case 'exchange:sell': return handleExchangeSell(username, msg);
      case 'exchange:buy': return handleExchangeBuy(username, msg);
      case 'account:reset': return handleAccountReset(username);
      case 'admin:lookup': return handleAdminLookup(username, msg);
      case 'admin:addCoins': return handleAdminAdjustCoins(username, msg, 1);
      case 'admin:removeCoins': return handleAdminAdjustCoins(username, msg, -1);
      // The client requests this whenever its tab becomes visible again
      // after being backgrounded - without an explicit resync, a client
      // that missed a broadcast while backgrounded had no way to recover
      // other than waiting for the next unrelated state change, which
      // could leave it computing the multiplier from a very stale
      // activeStartAt in the meantime. This just re-sends the exact same
      // payload login already sends, so it's always safe to call.
      // Both of these use `username` - the identity this connection
      // authenticated as via login/checkSessionToken earlier, never
      // anything read from `msg` - so there's no way for a client to mark
      // (or even address) another user's notifications.
      case 'notifications:markRead':
        if (msg.id != null) db.markNotificationRead(username, msg.id).catch((err) => console.error('[notifications] markRead failed:', err.message));
        return;
      case 'notifications:markAllRead':
        db.markAllNotificationsRead(username).catch((err) => console.error('[notifications] markAllRead failed:', err.message));
        return;
      case 'withdraw:request': return handleWithdrawRequest(username, msg);
      case 'withdraw:cancel': return handleWithdrawCancel(username, msg);
      case 'admin:withdrawList': return handleAdminWithdrawList(username);
      case 'admin:withdrawFulfill': return handleAdminWithdrawFulfill(username, msg);
      case 'admin:withdrawReject': return handleAdminWithdrawReject(username, msg);
      case 'admin:requestClaim': return handleAdminRequestClaim(username, msg);
      case 'deposit:request': return handleDepositRequest(username, msg);
      case 'chat:send': return handleChatSend(username, msg, ws);
      case 'tip:send': return handleTipSend(username, msg);
      case 'profile:request': return handleProfileRequest(username, msg);
      case 'admin:setRole': return handleAdminSetRole(username, msg);
      case 'discord:linkStart': return handleDiscordLinkStart(username);
      case 'discord:unlink': return handleDiscordUnlink(username);
      case 'ignore:add': return handleIgnoreAdd(username, msg);
      case 'ignore:remove': return handleIgnoreRemove(username, msg);
      case 'history:request': return handleHistoryRequest(username, msg);
      case 'vault:deposit': return handleVaultCoins(username, msg, +1);
      case 'vault:withdraw': return handleVaultCoins(username, msg, -1);
      case 'vault:depositItems': return handleVaultItems(username, msg, +1);
      case 'vault:withdrawItems': return handleVaultItems(username, msg, -1);
      case 'vault:lock': return handleVaultLock(username, msg);
      case 'promo:redeem': return handlePromoRedeem(username, msg);
      case 'admin:promoCreate': return handleAdminPromoCreate(username, msg);
      case 'admin:promoList': return handleAdminPromoList(username);
      case 'admin:promoToggle': return handleAdminPromoToggle(username, msg);
      case 'admin:wipeAll': return handleAdminWipeAll(username, msg);
      case 'giveaway:create': return handleGiveawayCreate(username, msg);
      case 'giveaway:join': return handleGiveawayJoin(username, msg);
    }
  }

  ws.on('close', () => {
    const username = socketToUsername.get(ws);
    socketToUsername.delete(ws);
    if (username && usernameToSocket.get(username) === ws) usernameToSocket.delete(username);
  });
});

const PORT = process.env.PORT || 8080;

// Last-resort safety net: a stray promise rejection must never take the
// whole server down (Node exits on unhandled rejections by default).
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingDiscordStates) if (v.expires < now) pendingDiscordStates.delete(k);
}, 60 * 1000);

process.on('unhandledRejection', (err) => {
  console.error('[process] Unhandled promise rejection (kept running):', err);
});

// Boot sequence: schema first, then load every persisted user + their
// inventory into the in-memory `users` Map (same shape it's always had -
// nothing downstream needs to know this came from a database instead of
// starting empty), then pending withdrawal requests, and only THEN start
// accepting connections. If any of this fails, exit loudly rather than
// silently starting with an empty in-memory state that looks fine but
// quietly isn't backed by anything real.
(async () => {
  try {
    await db.initSchema();

    // One-time (but safe-to-repeat) cleanup: find any accounts that only
    // differ by capitalization (e.g. "DTN_bgsi" vs "DTN_BGSI" - created
    // before logins were normalized to Roblox's canonical username) and
    // merge them into the real canonical account. Runs every boot but is a
    // no-op once there's nothing left to merge, so it's safe to leave in
    // permanently rather than needing to remember to run it once and
    // remove it.
    try {
      const clusters = await db.findDuplicateCaseClusters();
      if (clusters.length) {
        console.log(`[boot] Found ${clusters.length} username cluster(s) differing only by capitalization - resolving canonical names via Roblox...`);
      }
      for (const cluster of clusters) {
        let canonical;
        try {
          const match = await robloxResolveUsername(cluster[0]);
          canonical = match ? match.name : null;
        } catch (err) {
          console.warn(`[boot] Couldn't reach Roblox to resolve canonical name for [${cluster.join(', ')}] - skipping this cluster for now, will retry next boot:`, err.message);
          continue;
        }
        if (!canonical) {
          console.warn(`[boot] Roblox has no account matching [${cluster.join(', ')}] anymore (renamed/deleted?) - leaving these accounts separate, review manually if needed.`);
          continue;
        }
        try {
          const result = await db.mergeCasedDuplicates(canonical, cluster);
          if (result) {
            console.log(`[boot] Merged [${result.mergedFrom.join(', ')}] into "${result.canonicalUsername}" - final balance ${result.coins} coins, ${result.itemCount} distinct item(s).`);
          }
        } catch (err) {
          console.error(`[boot] Failed to merge cluster [${cluster.join(', ')}]:`, err.message);
        }
      }
    } catch (err) {
      console.error('[boot] Duplicate-account check failed (continuing to boot normally):', err.message);
    }

    const loadedUsers = await db.loadAllUsers();
    for (const [uname, u] of loadedUsers) users.set(uname, u);
    console.log(`[boot] Loaded ${loadedUsers.size} user account(s) from Postgres.`);

    // Giveaways hold pets in escrow. If the server stopped mid-giveaway, give
    // the pets back to the host. Each refund + escrow release is one DB
    // transaction, so it can never be paid back twice.
    const leftoverEscrows = await db.loadEscrows();
    for (const e of leftoverEscrows) {
      const u = ensureUser(e.username);
      const before = u.coins;
      if (e.coins > 0) u.coins += e.coins;
      if (e.items.length) addItems(e.username, e.items);
      await db.persistUserState(e.username, u, {
        type: 'escrow_refund', amount: e.coins, balanceBefore: before, balanceAfter: u.coins,
        itemsTouched: [...new Set(e.items)], reason: `Refund: ${e.kind} was interrupted by a server restart`,
        escrowRelease: [e.id],
      });
    }
    if (leftoverEscrows.length) console.log(`[boot] Refunded ${leftoverEscrows.length} interrupted giveaway(s).`);

    Object.assign(shopStock, await db.loadShopStock());
    console.log(`[boot] Loaded exchange stock (${Object.keys(shopStock).length} pet type(s)).`);
    try {
      const savedValues = await db.loadServerState('liveValues');
      if (savedValues && savedValues.values && typeof savedValues.values === 'object') {
        let n = 0;
        for (const [id, pair] of Object.entries(savedValues.values)) {
          if (!MANUAL_VALUE_IDS.has(id) && Array.isArray(pair) && applyLiveValue(id, Number(pair[0]) || 0, !!pair[1])) n++;
        }
        valuesUpdatedAt = Number(savedValues.at) || 0;
        console.log(`[boot] Restored ${n} live pet value(s) from the last demand.gg update.`);
      }
    } catch (err) { console.error('[boot] Could not load live values:', err.message); }
    const savedRain = await db.loadServerState('rainPool');
    if (savedRain && Number.isFinite(Number(savedRain.pool))) rainPool = Math.max(RAIN_BASE, Math.floor(Number(savedRain.pool)));

    ignoreLists = await db.loadAllIgnores();

    const loadedRequests = await db.loadPendingWithdrawRequests();
    withdrawRequests = loadedRequests;
    console.log(`[boot] Loaded ${loadedRequests.length} pending withdrawal request(s) from Postgres.`);

    if (discordRolesConfigured()) {
      const linked = [...users.entries()].filter(([, u]) => u.discord && u.discord.id).map(([name]) => name);
      console.log(`[discord-roles] ON - ${DISCORD_LEVEL_ROLES.length} level role(s); catching up ${linked.length} linked player(s).`);
      (async () => {
        let done = 0;
        for (const name of linked) {
          try { const r = await syncDiscordLevelRoles(name, { force: true }); if (r === 'synced') done++; } catch (err) { console.error('[discord-roles] boot sync failed for', name, err.message); }
          await new Promise((r) => setTimeout(r, 300));
        }
        console.log(`[discord-roles] boot catch-up finished (${done}/${linked.length} in the server).`);
      })();
    }

    loadIndexHtmlCache();
    loadPrivacyHtmlCache();
    server.listen(PORT, () => console.log(`BloxyVault server listening on :${PORT}`));
    if (VALUE_SYNC_ON) {
      setTimeout(syncValuesFromDemand, 15000);
      setInterval(syncValuesFromDemand, VALUE_SYNC_MS);
      console.log(`[values] Live values from demand.gg ON (every ${VALUE_SYNC_MS / 60e3} min).`);
    } else console.log('[values] Live values from demand.gg OFF (VALUE_SYNC=off).');
  } catch (err) {
    console.error('[boot] Failed to start - could not initialize/load from Postgres:', err);
    process.exit(1);
  }
})();
