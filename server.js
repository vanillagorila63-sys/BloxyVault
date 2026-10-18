// BloxyVault multiplayer server
//
// Speaks the exact same message protocol the client already expects (it was
// originally built against a local mock that used this same shape, so the
// client needs zero changes beyond pointing window.BLOXYVAULT_SERVER_URL at
// this server's wss:// URL).
//
// State is entirely in-memory - it resets whenever this process restarts.
// That's fine for a practice/hobby multiplayer server; if you want accounts
// to survive restarts/deploys, swap the `users` Map for a real database
// later (the shape is simple: username -> {coins, inventory}).

const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const db = require('./db');

const gameData = JSON.parse(fs.readFileSync(path.join(__dirname, 'gameData.json'), 'utf8'));
const { petCatalog, caseData, caseItems, casePools, modeConfigs, botNamesBattles } = gameData;
// Group mode (case battles): 2-4 players open the same cases together and the
// whole pot is split equally between everyone, no matter who pulled what.
modeConfigs['group-2'] = { count: 2, isTeam: false, isGroup: true };
modeConfigs['group-3'] = { count: 3, isTeam: false, isGroup: true };
modeConfigs['group-4'] = { count: 4, isTeam: false, isGroup: true };

const STARTING_BALANCE = 0; // normal accounts start with nothing - no free coins on creation
const CASEBATTLE_ABANDON_MS = 5 * 60 * 1000; // if a creator walks away without calling bots or getting joined, clean it up after 5 minutes
const CASEBATTLE_FINISHED_DISPLAY_MS = 20 * 1000; // how long a finished battle stays visible in the list before it's removed
const ROUND_PACE_MS = 4300;        // must match the client's MOCK_ROUND_PACE_MS
const CF_MATCH_TOLERANCE = 0.10;   // coinflip stakes must be within ±10% of each other, matching the client's own join-range check

// Site owner - gets the coinflip rake (see resolveCoinflip below) and is the
// only account allowed to use the admin:lookup inventory viewer. Whoever
// this is effectively runs the house, so keep this accurate.
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

// Successful Roblox avatar lookups are cached here for a while - avatars
// don't change often, and without this, every single render of every
// username anywhere on the site (Coinflip lobbies, Case Battle player
// cards, etc.) across every connected browser was triggering its own
// fresh outbound call to Roblox for the same handful of usernames, which
// both wastes requests and raises the odds of tripping Roblox's own rate
// limits - a likely contributor to the intermittent PFP failures this was
// built to fix. Deliberately does NOT cache failures/nulls - a username
// that briefly failed to resolve should be retried on the next request,
// not permanently remembered as "no avatar" (see /roblox/avatar below).
const AVATAR_CACHE_TTL_MS = 30 * 60 * 1000;
const avatarCache = new Map(); // username(lowercase) -> {imageUrl, userId, cachedAt}

const NOTIFICATION_LOAD_LIMIT = 50; // matches the client's own history cap

// Creates a notification for one user - persists it (with a DB-level
// unique-constraint safety net against duplicates, see db.js), and pushes
// it straight to their active connection if they're online right now, so
// the bell updates without needing a refresh. Every call site below is
// already exactly-once on its own (an atomic pending->resolved DB
// transition for withdrawals, a single-execution battle resolution for
// Case Battles) - this never trusts anything client-supplied about who
// the notification is for or what it says, since it's only ever called
// from server-side logic with server-computed values.
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
    users.set(username, { coins: startingCoins, inventory: startingInventory, stats: { wagered: 0, won: 0, lost: 0 }, specialRole: null, mutedUntil: 0 });
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
      db.insertNewUser(username, startingCoins, { wagered: 0, won: 0, lost: 0 }).catch((err) => {
        console.error(`[db] insertNewUser failed for ${username}:`, err.message);
      });
    }
  }
  const u = users.get(username);
  if (!u.stats) u.stats = { wagered: 0, won: 0, lost: 0 }; // backfill for accounts created before stats existed
  if (u.specialRole === undefined) u.specialRole = null; // backfill for accounts created before manually-grantable roles existed
  if (u.mutedUntil === undefined) u.mutedUntil = 0; // backfill for accounts created before chat muting existed
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
function trackWager(username, amount) {
  if (!(amount > 0)) return;
  const u = ensureUser(username);
  const before = levelForWagered(u.stats.wagered);
  u.stats.wagered += amount;
  if (levelForWagered(u.stats.wagered) !== before) scheduleDiscordRoleSync(username);
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
// A cancelled coinflip lobby never actually played out, so it shouldn't
// keep counting toward "Played" / Most Played - this undoes the trackWager
// that ran when the lobby was first created.
function untrackWager(username, amount) {
  if (!(amount > 0)) return;
  const u = ensureUser(username);
  const before = levelForWagered(u.stats.wagered);
  u.stats.wagered = Math.max(0, u.stats.wagered - amount);
  if (levelForWagered(u.stats.wagered) !== before) scheduleDiscordRoleSync(username);
}
function trackWin(username, amount) { if (amount > 0) ensureUser(username).stats.won += amount; }
function trackLoss(username, amount) { if (amount > 0) ensureUser(username).stats.lost += amount; }

// ---------------------------------------------------------------------------
// Level curve - mirrors the client's exact formula (see wageredForLevel /
// levelForWagered in the frontend) so a player's level, and therefore their
// Rain Pool share, is computed identically on both sides.
// ---------------------------------------------------------------------------
const LEVEL_CURVE_C = 2230808.28;
const LEVEL_CURVE_K = 1.353373;
const MAX_LEVEL = 500;
function levelForWagered(wagered) {
  const w = Math.max(0, wagered || 0);
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
function discordApi(method, path) {
  const run = async () => {
    for (let attempt = 0; attempt < 4; attempt++) {
      const res = await fetch(`${DISCORD_API_BASE}${path}`, {
        method, headers: { Authorization: `Bot ${DISCORD_BOT_TOKEN}`, 'X-Audit-Log-Reason': 'BloxyVault level sync' },
      });
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
// Debounced: a burst of bets only produces one sync per player.
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
  const level = levelForWagered(u.stats.wagered);
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
      const w = levelForWagered(ensureUser(username).stats.wagered);
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
// Every duped pet's catalog id is exactly "dup_" + the original pet's id
// (confirmed by inspecting gameData.json - 587 duped entries, every single
// one has a real corresponding non-duped entry). This makes identifying
// normal vs duped, and finding a pet's counterpart, a pure string check -
// no name-matching or extra catalog lookups needed, and no way for a
// client to lie about it since it's just the id itself.
function isDupedItemId(id) { return typeof id === 'string' && id.startsWith('dup_'); }
function normalizeItemId(id) { return isDupedItemId(id) ? id.slice(4) : id; }

// Used by handleCoinflipJoin for a Locked/Unlocked lobby (see
// handleCoinflipCreate). The ONLY thing Locked actually restricts is
// whether a DUPED item can be staked - it is NOT a "must be the same pet
// as the creator" rule (an earlier version of this wrongly required that;
// any normal pet is fine, it doesn't need to relate to the creator's pet
// at all). A lobby with `locked === undefined` was persisted from before
// this feature existed - exempt entirely, so an in-flight lobby from
// before a deploy doesn't suddenly become unjoinable under a restriction
// it was never created with.
function joinItemsMatchLockedLobby(lobby, joinItems) {
  if (lobby.locked === undefined || !lobby.locked) return true; // Unlocked (or legacy): normal or duped, no restriction
  return joinItems.every((id) => !isDupedItemId(id)); // Locked: normal pets only
}

function stakeValue(items) {
  return items.reduce((sum, id) => sum + itemValue(id), 0);
}

// Finds the subset of `items` (with at most `maxItems` items in it) whose
// combined value is closest to `target`. Used to pick out exactly which
// item(s) make up the "excess" on the larger side of an uneven-but-within-
// range coinflip stake, so that excess can be carved out as the house rake
// while the rest flips normally.
// Exhaustive search - fine since a single stake realistically has a small
// handful of items (2^n), but capped defensively just in case. The item
// cap is enforced WHILE searching (only subsets of size <= maxItems are
// ever considered), not as an after-the-fact filter - otherwise the search
// happily finds some large, far-better-fitting combination and then the
// caller has to throw the whole thing away for being too big, which is
// exactly what was happening before this fix.
function pickClosestSubset(items, target, maxItems){
  if(!items.length || target <= 0) return [];
  const n = Math.min(items.length, 20);
  const cap = maxItems && maxItems > 0 ? Math.min(maxItems, n) : n;
  const withValues = items.slice(0, n).map(id => ({ id, value: itemValue(id) }));
  let best = [], bestDiff = Infinity;
  for(let mask = 1; mask < (1 << n); mask++){
    let sum = 0; let count = 0; const subset = [];
    for(let i = 0; i < n; i++) if(mask & (1 << i)){ sum += withValues[i].value; subset.push(withValues[i].id); count++; }
    if(count > cap) continue; // respect the item-count cap during the search, not after
    const diff = Math.abs(sum - target);
    if(diff < bestDiff){ bestDiff = diff; best = subset; }
  }
  return best;
}
function ownsAll(username, items) {
  const inv = ensureUser(username).inventory;
  const locked = lockedItemsForUser(username);
  const need = {};
  for (const id of items) need[id] = (need[id] || 0) + 1;
  return Object.entries(need).every(([id, qty]) => ((inv[id] || 0) - (locked[id] || 0)) >= qty);
}
// Items sitting inside a player's own PENDING withdrawal request(s) are
// "locked" - still physically in their inventory (nothing is actually
// removed until an admin fulfills the request), but unavailable for
// wagering, staking, or a second withdrawal request, so a player can't
// submit a withdrawal and then gamble the same pets away before the admin
// gets to it. Unlocks automatically the moment the request resolves
// (fulfilled/rejected/cancelled), since this is just derived from whatever
// is still 'pending' right now - no separate unlock bookkeeping needed.
function lockedItemsForUser(username) {
  const locked = {};
  for (const req of withdrawRequests) {
    if (req.username !== username || req.status !== 'pending') continue;
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

// ---------------------------------------------------------------------------
// Live feed - a rolling site-wide log of the last FEED_MAX things that just
// happened across every game (Coinflip, Case Battles, Jackpot, Mines,
// Cases), so the home page can show real activity instead of just your own.
// Kept in memory only (not persisted) - a fresh server restart just starts
// the feed empty again, which is fine for something this ephemeral.
// ---------------------------------------------------------------------------
const FEED_MAX = 10;
let liveFeed = []; // newest first

function pushFeedEvent(entry) {
  const full = { id: 'feed_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8), at: Date.now(), ...entry };
  liveFeed.unshift(full);
  if (liveFeed.length > FEED_MAX) liveFeed.length = FEED_MAX;
  broadcast({ type: 'feed:event', entry: full });
}

// Chat history - purely in-memory (resets on restart, same as the rest of
// this file's live state), but at least survives a client-side page
// refresh now: previously the chat log was rebuilt ONLY from live
// chat:message broadcasts received while connected, so refreshing (or
// reconnecting) wiped it back to completely empty even though the
// conversation was still very much ongoing for everyone else. Kept oldest-
// first (chronological), capped at CHAT_HISTORY_MAX - once full, the
// oldest message drops off the front to make room, same "1st one
// disappears" behavior the client's own CHAT_MAX_MESSAGES cap already had.
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
  const myPendingWithdrawals = withdrawRequests
    .filter((r) => r.username === username && r.status === 'pending')
    .map((r) => ({ id: r.id, items: r.items, requestedAt: r.requestedAt }));
  send(ws, {
    type: 'account',
    user: {
      username, coins: u.coins, inventory: u.inventory, stats: u.stats,
      specialRole: u.specialRole || null, mutedUntil: u.mutedUntil || 0,
      lockedItems: lockedItemsForUser(username),
      pendingWithdrawals: myPendingWithdrawals,
      discord: discordPublic(u.discord),
      ignored: ignoredListFor(username),
    },
  });
}

// Fire-and-forget persistence: memory has already been updated synchronously
// (same as before this feature existed) by the time this is called, so this
// just mirrors that already-decided state into Postgres afterward. Errors
// are logged, not thrown - a slow/hiccupping DB write should never crash a
// live game round or block the response the player already got.
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
// whole server - taking every in-progress game's escrowed pets with it).
function sanitizeItems(raw, max = 100) {
  if (!Array.isArray(raw) || raw.length > max) return null;
  for (const id of raw) {
    if (typeof id !== 'string' || !Object.prototype.hasOwnProperty.call(petCatalog, id)) return null;
  }
  return raw.slice();
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
// Shared game math (mirrors the client's weightedPick/getPool exactly)
// ---------------------------------------------------------------------------
function weightedPick(pool) {
  const total = pool.reduce((s, it) => s + it.chance, 0);
  let roll = Math.random() * total;
  for (const it of pool) {
    roll -= it.chance;
    if (roll <= 0) return it;
  }
  return pool[pool.length - 1];
}
function getPool(caseIndex) {
  const items = caseItems[caseIndex];
  if (items && items.length) return items;
  const c = caseData[caseIndex];
  return [{ name: 'Mystery Prize', value: c.price, chance: 100 }];
}
function pickBotNames(n, pool) {
  const shuffled = pool.slice().sort(() => Math.random() - 0.5);
  return shuffled.slice(0, n);
}

// ---------------------------------------------------------------------------
// Coinflip
// ---------------------------------------------------------------------------
let cfLobbies = []; // { id, creator, side, items }

function broadcastCfLobbies() {
  broadcast({ type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items, locked: l.locked })) });
}


// Rake target: 7.5% of the TOTAL pot value, taken from the winner's full
// haul AFTER the flip resolves (not carved from either side beforehand).
// Since items are discrete, the actual amount taken will land near 7.5% but
// not exactly on it - CF_RAKE_MAX_MULTIPLE caps how far over that's allowed
// to drift; if nothing in the winner's haul comes reasonably close without
// blowing way past the target (e.g. the only candidate is one huge pet),
// no rake is taken at all rather than grabbing something oversized.
// Perfectly even 1-for-1 stakes (both sides worth exactly the same) are
// exempt from the rake entirely, regardless of the above.
const CF_RAKE_TARGET_PCT = 0.075;
const CF_RAKE_MAX_MULTIPLE = 1.5; // never take more than ~1.5x target (~11.25% of pot) - skip entirely past that
const CF_RAKE_MAX_ITEMS = 2; // hard cap: never take more than 2 individual items as rake, no matter what the value math says

function subtractItemList(full, toRemove){
  const remaining = [...full];
  for(const id of toRemove){
    const idx = remaining.indexOf(id);
    if(idx !== -1) remaining.splice(idx, 1);
  }
  return remaining;
}

function resolveCoinflip(creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems) {
  const result = Math.random() < 0.5 ? 'H' : 'T';
  const winner = result === creatorSide ? creator : joiner;
  const loser = winner === creator ? joiner : creator;
  const winnerOwnItems = winner === creator ? creatorItems : joinerItems;
  const loserItems = winner === creator ? joinerItems : creatorItems;
  const fullHaul = [...winnerOwnItems, ...loserItems]; // winner's own stake back + everything the loser staked

  const creatorValue = stakeValue(creatorItems);
  const joinerValue = stakeValue(joinerItems);
  const totalPotValue = creatorValue + joinerValue;
  const targetRake = totalPotValue * CF_RAKE_TARGET_PCT;
  let rakeItems = [];
  // Perfectly even 1-for-1 stakes (both sides worth exactly the same) are
  // exempt from the rake entirely - only imbalanced-or-uneven pots get raked.
  if (creatorValue !== joinerValue && targetRake > 0 && fullHaul.length > 0) {
    const candidate = pickClosestSubset(fullHaul, targetRake, CF_RAKE_MAX_ITEMS);
    const candidateValue = stakeValue(candidate);
    // Two independent safety limits, both must pass: the value can't drift
    // too far past the target (CF_RAKE_MAX_MULTIPLE), AND the item COUNT is
    // hard-capped (CF_RAKE_MAX_ITEMS) regardless of value - so even if the
    // value-matching logic ever picks a surprising combination, it physically
    // cannot take more than a couple of items no matter what.
    if (candidateValue > 0 && candidate.length <= CF_RAKE_MAX_ITEMS && candidateValue <= targetRake * CF_RAKE_MAX_MULTIPLE) {
      rakeItems = candidate;
    }
    console.log(`[cf-rake] pot=${totalPotValue} target=${Math.round(targetRake)} candidate=${JSON.stringify(candidate)} candidateValue=${candidateValue} candidateCount=${candidate.length} taken=${JSON.stringify(rakeItems)}`);
  }
  const winnerFinalItems = rakeItems.length ? subtractItemList(fullHaul, rakeItems) : fullHaul;

  // Both sides are always real players now - bots never participate in
  // Coinflip anymore, so this always tracks stats/persists for both.
  const wonAmount = stakeValue(loserItems);
  addItems(winner, winnerFinalItems);
  trackWin(winner, wonAmount);
  persistUser(winner, { type: 'coinflip_win', amount: 0, itemsTouched: [...new Set(winnerFinalItems)], game: { game: 'coinflip', wager: stakeValue(winnerOwnItems), payout: stakeValue(winnerFinalItems) } });
  trackLoss(loser, wonAmount);
  persistUser(loser, { type: 'coinflip_loss', amount: 0, game: { game: 'coinflip', wager: stakeValue(loserItems), payout: 0 } });
  if (rakeItems.length) {
    addItems(ADMIN_USERNAME, rakeItems);
    persistUser(ADMIN_USERNAME, { type: 'coinflip_rake', amount: 0, itemsTouched: [...new Set(rakeItems)] });
    syncAccount(ADMIN_USERNAME);
  }

  syncAccount(creator);
  syncAccount(joiner);
  broadcast({ type: 'coinflip:result', creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems, winner, result });
  pushFeedEvent({ game: 'coinflip', username: winner, amount: wonAmount });
  db.insertCoinflipHistory({ creator, joiner, creatorSide, joinerSide, creatorItems, joinerItems, creatorValue, joinerValue, result, winner }).catch((err) => {
    console.error('[db] Failed to persist coinflip history:', err.message);
  });

  broadcastCfLobbies();
}

function handleCoinflipCreate(username, msg) {
  const items = sanitizeItems(msg.items, 50) || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  const locked = !!msg.locked;
  // Duped pets are only disallowed as the creator's wager in Locked mode -
  // Unlocked allows either, on both sides (see joinItemsMatchLockedLobby
  // for the join-side equivalent of this same rule).
  if (locked && items.some(isDupedItemId)) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'A Locked Coinflip can only be created with a normal (non-duped) pet.' });
  }
  removeItems(username, items); // escrow
  trackWager(username, stakeValue(items));
  persistUser(username, { type: 'coinflip_create_escrow', amount: 0, itemsTouched: [...new Set(items)] });
  const id = 'cf_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const lobby = { id, creator: username, side: msg.side === 'T' ? 'T' : 'H', items, locked };
  cfLobbies.push(lobby);
  // Persisted so a server restart while this lobby is still waiting for an
  // opponent doesn't lose track of it - the items were already escrowed out
  // of the creator's inventory above (see removeItems), and without this
  // the lobby (the only record of who those items belong to) would simply
  // vanish from memory on restart with no way to ever return them.
  db.insertCoinflipLobby(lobby).catch((err) => {
    console.error(`[db] Failed to persist coinflip lobby ${id}:`, err.message);
  });
  syncAccount(username);
  broadcastCfLobbies();
  // The lobby just sits here now, waiting for a real player to join via
  // handleCoinflipJoin below - no automatic bot opponent gets spawned for
  // it anymore, no matter how long it waits.
}

function handleCoinflipJoin(username, msg) {
  const lobby = cfLobbies.find((l) => l.id === msg.lobbyId);
  if (!lobby) return send(usernameToSocket.get(username), { type: 'error', message: 'That lobby is no longer available.' });
  // Joining your own lobby used to be allowed: you'd flip against yourself
  // and the house rake would silently take some of your pets.
  if (lobby.creator === username) return send(usernameToSocket.get(username), { type: 'error', message: "You can't join your own lobby." });
  const items = sanitizeItems(msg.items, 50) || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  const lobbyValue = stakeValue(lobby.items);
  const joinValue = stakeValue(items);
  const lo = lobbyValue * (1 - CF_MATCH_TOLERANCE), hi = lobbyValue * (1 + CF_MATCH_TOLERANCE);
  if (joinValue < lo || joinValue > hi) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Your stake is outside the allowed ±10% range.' });
  }
  if (!joinItemsMatchLockedLobby(lobby, items)) {
    // Only Locked can actually land here now - Unlocked never fails this
    // check (any normal or duped pet is fine there).
    return send(usernameToSocket.get(username), { type: 'error', message: 'This lobby is Locked - you can only join with a normal (non-duped) pet.' });
  }
  removeItems(username, items); // escrow
  trackWager(username, joinValue);
  persistUser(username, { type: 'coinflip_join_escrow', amount: 0, itemsTouched: [...new Set(items)] });
  cfLobbies = cfLobbies.filter((l) => l.id !== msg.lobbyId);
  db.deleteCoinflipLobby(msg.lobbyId).catch((err) => {
    console.error(`[db] Failed to delete resolved coinflip lobby ${msg.lobbyId}:`, err.message);
  });
  broadcastCfLobbies();
  const joinerSide = lobby.side === 'H' ? 'T' : 'H';
  resolveCoinflip(lobby.creator, lobby.side, lobby.items, username, joinerSide, items);
}

function handleCoinflipCancel(username, msg) {
  const lobby = cfLobbies.find((l) => l.id === msg.lobbyId);
  if (!lobby || lobby.creator !== username) return;
  addItems(username, lobby.items); // return escrow
  untrackWager(username, stakeValue(lobby.items)); // never actually played - shouldn't count as "played"
  persistUser(username, { type: 'coinflip_cancel_refund', amount: 0, itemsTouched: [...new Set(lobby.items)] });
  cfLobbies = cfLobbies.filter((l) => l.id !== msg.lobbyId);
  db.deleteCoinflipLobby(msg.lobbyId).catch((err) => {
    console.error(`[db] Failed to delete cancelled coinflip lobby ${msg.lobbyId}:`, err.message);
  });
  syncAccount(username);
  broadcastCfLobbies();
}

// ---------------------------------------------------------------------------
// Dice Duel - 1v1, same escrow/ownership/±10%-match/7.5%-rake patterns as
// Coinflip above (reusing ownsAll/removeItems/addItems/stakeValue/
// pickClosestSubset/subtractItemList/CF_RAKE_* directly rather than
// duplicating that logic), but unlike Coinflip this doesn't resolve
// instantly on join - it keeps rolling a die every ROLL_PACE_MS until it
// lands on either player's chosen number, so a duel is a small live "battle"
// (much closer in shape to Case Battles' multi-round runBattle() than to
// Coinflip's single synchronous flip). Each duel lives in `diceDuels` for
// its whole lifecycle (waiting -> running -> finished) and the full list is
// broadcast on every change, the same way casebattle:list works - so late
// joiners/spectators can see an in-progress duel's roll history, not just
// open lobbies waiting for a second player.
// ---------------------------------------------------------------------------
const DICE_DUEL_ROLL_PACE_MS = 1100;
const DICE_DUEL_FINISHED_DISPLAY_MS = 12000;
let diceDuels = new Map(); // id -> duel

function diceDuelPublicList() {
  return [...diceDuels.values()].map((d) => ({
    id: d.id, status: d.status,
    creator: d.creator, creatorItems: d.creatorItems, creatorValue: d.creatorValue, creatorNumber: d.creatorNumber,
    joiner: d.joiner || null, joinerItems: d.joinerItems || [], joinerValue: d.joinerValue || 0, joinerNumber: d.joinerNumber || null,
    rolls: d.rolls, winner: d.winner || null, winnerValue: d.winnerValue || 0,
  }));
}
function broadcastDiceDuels() {
  broadcast({ type: 'diceduel:list', duels: diceDuelPublicList() });
}

function isValidDiceNumber(n) {
  return Number.isInteger(n) && n >= 1 && n <= 6;
}

function handleDiceDuelCreate(username, msg) {
  const items = sanitizeItems(msg.items, 50) || [];
  const number = Number(msg.number);
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  if (!isValidDiceNumber(number)) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Pick a number from 1 to 6.' });
  }
  removeItems(username, items); // escrow
  const creatorValue = stakeValue(items);
  trackWager(username, creatorValue);
  const creatorEscrowId = newEscrowId('dd');
  persistUser(username, { type: 'diceduel_create_escrow', amount: 0, itemsTouched: [...new Set(items)], escrowAdd: [{ id: creatorEscrowId, kind: 'diceduel', items, coins: 0 }] });
  const id = 'dd_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  diceDuels.set(id, {
    id, status: 'waiting', creatorEscrowId, joinerEscrowId: null,
    creator: username, creatorItems: items, creatorValue, creatorNumber: number,
    joiner: null, joinerItems: [], joinerValue: 0, joinerNumber: null,
    rolls: [], winner: null, winnerValue: 0,
  });
  syncAccount(username);
  broadcastDiceDuels();
}

function handleDiceDuelJoin(username, msg) {
  const duel = diceDuels.get(msg.duelId);
  if (!duel || duel.status !== 'waiting') {
    return send(usernameToSocket.get(username), { type: 'error', message: 'That duel is no longer available.' });
  }
  if (duel.creator === username) return; // can't join your own duel
  const items = sanitizeItems(msg.items, 50) || [];
  const number = Number(msg.number);
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  if (!isValidDiceNumber(number)) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Pick a number from 1 to 6.' });
  }
  // Never trust the client to grey out the creator's number client-side only
  // - re-checked here regardless of what the join request claims.
  if (number === duel.creatorNumber) {
    return send(usernameToSocket.get(username), { type: 'error', message: `${duel.creatorNumber} is already taken - pick a different number.` });
  }
  const joinValue = stakeValue(items);
  const lo = duel.creatorValue * (1 - CF_MATCH_TOLERANCE), hi = duel.creatorValue * (1 + CF_MATCH_TOLERANCE);
  if (joinValue < lo || joinValue > hi) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Your stake is outside the allowed ±10% range.' });
  }

  removeItems(username, items); // escrow
  trackWager(username, joinValue);
  const joinerEscrowId = newEscrowId('dd');
  persistUser(username, { type: 'diceduel_join_escrow', amount: 0, itemsTouched: [...new Set(items)], escrowAdd: [{ id: joinerEscrowId, kind: 'diceduel', items, coins: 0 }] });
  duel.joinerEscrowId = joinerEscrowId;

  duel.status = 'running';
  duel.joiner = username;
  duel.joinerItems = items;
  duel.joinerValue = joinValue;
  duel.joinerNumber = number;
  syncAccount(username);
  broadcastDiceDuels();
  runDiceDuelRolls(duel.id);
}

function handleDiceDuelCancel(username, msg) {
  const duel = diceDuels.get(msg.duelId);
  if (!duel || duel.creator !== username || duel.status !== 'waiting') return;
  cancelDiceDuelInternal(duel);
}

// Shared refund/cleanup, used both by an explicit cancel request and by the
// disconnect handler below - a duel only ever gets refunded this one way,
// so there's a single place that can return the escrow exactly once.
function cancelDiceDuelInternal(duel) {
  addItems(duel.creator, duel.creatorItems); // return escrow
  untrackWager(duel.creator, duel.creatorValue); // never actually played - shouldn't count as "played"
  persistUser(duel.creator, { type: 'diceduel_cancel_refund', amount: 0, itemsTouched: [...new Set(duel.creatorItems)], escrowRelease: [duel.creatorEscrowId].filter(Boolean) });
  diceDuels.delete(duel.id);
  syncAccount(duel.creator);
  broadcastDiceDuels();
}

async function runDiceDuelRolls(duelId) {
  const duel = diceDuels.get(duelId);
  if (!duel) return;
  while (true) {
    const current = diceDuels.get(duelId);
    if (!current || current.status !== 'running') return; // duel vanished/already resolved - stop
    const result = 1 + Math.floor(Math.random() * 6); // server-generated; the client only ever receives this and animates it
    current.rolls.push(result);
    broadcast({ type: 'diceduel:roll', duelId, result, rollIdx: current.rolls.length - 1 });
    if (result === current.creatorNumber || result === current.joinerNumber) {
      resolveDiceDuel(current, result);
      return;
    }
    await new Promise((r) => setTimeout(r, DICE_DUEL_ROLL_PACE_MS));
  }
}

// Parallels resolveCoinflip's payout/rake logic (same CF_RAKE_* constants,
// same pickClosestSubset/subtractItemList helpers) without touching
// resolveCoinflip itself - Coinflip's own behavior stays exactly as it was.
function resolveDiceDuel(duel, finalRoll) {
  const winner = finalRoll === duel.creatorNumber ? duel.creator : duel.joiner;
  const loser = winner === duel.creator ? duel.joiner : duel.creator;
  const winnerOwnItems = winner === duel.creator ? duel.creatorItems : duel.joinerItems;
  const loserItems = winner === duel.creator ? duel.joinerItems : duel.creatorItems;
  const fullHaul = [...winnerOwnItems, ...loserItems];

  const totalPotValue = duel.creatorValue + duel.joinerValue;
  const targetRake = totalPotValue * CF_RAKE_TARGET_PCT;
  let rakeItems = [];
  if (duel.creatorValue !== duel.joinerValue && targetRake > 0 && fullHaul.length > 0) {
    const candidate = pickClosestSubset(fullHaul, targetRake, CF_RAKE_MAX_ITEMS);
    const candidateValue = stakeValue(candidate);
    if (candidateValue > 0 && candidate.length <= CF_RAKE_MAX_ITEMS && candidateValue <= targetRake * CF_RAKE_MAX_MULTIPLE) {
      rakeItems = candidate;
    }
  }
  const winnerFinalItems = rakeItems.length ? subtractItemList(fullHaul, rakeItems) : fullHaul;

  const wonAmount = stakeValue(loserItems);
  addItems(winner, winnerFinalItems);
  trackWin(winner, wonAmount);
  persistUser(winner, { type: 'diceduel_win', amount: 0, itemsTouched: [...new Set(winnerFinalItems)], escrowRelease: [duel.creatorEscrowId, duel.joinerEscrowId].filter(Boolean), game: { game: 'diceduel', wager: stakeValue(winnerOwnItems), payout: stakeValue(winnerFinalItems) } });
  trackLoss(loser, wonAmount);
  persistUser(loser, { type: 'diceduel_loss', amount: 0, game: { game: 'diceduel', wager: stakeValue(loserItems), payout: 0 } });
  if (rakeItems.length) {
    addItems(ADMIN_USERNAME, rakeItems);
    persistUser(ADMIN_USERNAME, { type: 'diceduel_rake', amount: 0, itemsTouched: [...new Set(rakeItems)] });
    syncAccount(ADMIN_USERNAME);
  }

  syncAccount(duel.creator);
  syncAccount(duel.joiner);

  duel.status = 'finished';
  duel.winner = winner;
  duel.winnerValue = totalPotValue - stakeValue(rakeItems);
  broadcast({ type: 'diceduel:finished', duelId: duel.id, winner, winnerValue: duel.winnerValue, rolls: duel.rolls });
  pushFeedEvent({ game: 'diceduel', username: winner, amount: wonAmount });
  broadcastDiceDuels();

  // Same lifecycle as a finished Case Battle - stay visible briefly, then
  // drop out of the shared list so it doesn't pile up forever.
  // The client doesn't reveal "X won" in the shared lobby list until it's
  // paced through every one of this duel's rolls at ~6s each, matching the
  // battle modal's own animation pace (see DD_ROLL_MS client-side). With a
  // flat 12s window here, any duel with more than 2 rolls - which is most
  // of them, since it typically takes ~3 rolls for either number to hit -
  // got pruned from the list before the client's own delay ever elapsed.
  // The row would just vanish, having never actually shown the winner.
  // Scale this duel's window to its own roll count instead, with enough
  // buffer afterward to actually see the result before it clears out.
  const revealDelayMs = duel.rolls.length * 6000;
  const displayMs = Math.max(DICE_DUEL_FINISHED_DISPLAY_MS, revealDelayMs + 8000);
  setTimeout(() => {
    const stillThere = diceDuels.get(duel.id);
    if (stillThere && stillThere.status === 'finished') {
      diceDuels.delete(duel.id);
      broadcastDiceDuels();
    }
  }, displayMs);
}

// ---------------------------------------------------------------------------
// Jackpot - one shared round site-wide (not per-lobby): everyone who enters
// within the 60s window throws coins and/or items into the same pot, and a
// single weighted-random winner takes it all. This used to be simulated
// entirely client-side with fake bots, which is why two real players never
// saw each other's entries - now the round itself lives here, and every
// connected client just renders whatever state gets broadcast to it.
// ---------------------------------------------------------------------------
const JACKPOT_ROUND_MS = 60 * 1000;
const JACKPOT_MIN_ENTRANTS = 2; // a round must not resolve/pay out with only one participant
// A lone entrant used to wait FOREVER with their pets locked in the pot if
// nobody else joined. After this long alone, their stake is handed back.
const JACKPOT_SOLO_REFUND_MS = 10 * 60 * 1000;
function refundSoloJackpot() {
  const round = jackpotRound;
  if (!round || round.timer || round.entrants.length !== 1) return;
  const e = round.entrants[0];
  const u = ensureUser(e.username);
  const before = u.coins;
  if (e.coinsStaked > 0) u.coins += e.coinsStaked;
  if (e.items.length) addItems(e.username, e.items);
  untrackWager(e.username, e.value);
  persistUser(e.username, { type: 'jackpot_solo_refund', amount: e.coinsStaked, balanceBefore: before, balanceAfter: u.coins, itemsTouched: [...new Set(e.items)], escrowRelease: [e.escrowId].filter(Boolean) });
  jackpotRound = null;
  syncAccount(e.username);
  send(usernameToSocket.get(e.username), { type: 'toast', message: 'Nobody else joined the Jackpot in time - your entry was returned.' });
  broadcastJackpot();
}
let jackpotRound = null; // { entrants: [{username, value, items, coinsStaked}], endsAt, timer }

function jackpotPublicState() {
  if (!jackpotRound) return { open: false, entrants: [], endsAt: 0 };
  return {
    open: true,
    endsAt: jackpotRound.endsAt,
    entrants: jackpotRound.entrants.map((e) => ({ username: e.username, value: e.value, items: e.items })),
  };
}
function broadcastJackpot() {
  broadcast({ type: 'jackpot:state', ...jackpotPublicState() });
}

function handleJackpotEnter(username, msg) {
  const amount = Math.max(0, Math.floor(Number(msg.amount) || 0));
  const items = msg.items == null ? [] : sanitizeItems(msg.items, 50);
  if (!items) return send(usernameToSocket.get(username), { type: 'error', message: 'Invalid items.' });
  if (amount <= 0 && items.length === 0) return;

  if (jackpotRound && jackpotRound.entrants.some((e) => e.username === username)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You've already entered this round." });
  }

  const u = ensureUser(username);
  if (amount > 0 && amount > u.coins) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Not enough coins.' });
  }
  if (items.length && !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }

  if (amount > 0) u.coins -= amount;
  if (items.length) removeItems(username, items); // escrow
  const value = amount + stakeValue(items);
  trackWager(username, value);
  const escrowId = newEscrowId('jp');
  persistUser(username, { type: 'jackpot_enter', amount: -amount, itemsTouched: [...new Set(items)], escrowAdd: [{ id: escrowId, kind: 'jackpot', items, coins: amount }] });
  syncAccount(username);

  if (!jackpotRound) {
    // The round exists as soon as the first person's in it, but the
    // countdown itself does NOT start yet (endsAt stays 0 / no timer
    // scheduled) - a lone entrant just waits indefinitely with the clock
    // frozen instead of watching a countdown tick toward a resolution that
    // can't actually happen yet with only one participant.
    jackpotRound = { entrants: [], endsAt: 0, timer: null, soloTimer: setTimeout(refundSoloJackpot, JACKPOT_SOLO_REFUND_MS) };
  }
  jackpotRound.entrants.push({ username, value, items, coinsStaked: amount, escrowId });

  if (jackpotRound.entrants.length >= JACKPOT_MIN_ENTRANTS && !jackpotRound.timer) {
    if (jackpotRound.soloTimer) { clearTimeout(jackpotRound.soloTimer); jackpotRound.soloTimer = null; }
    // The 2nd entrant just joined - only now does the real countdown begin,
    // and it's the same endsAt broadcast to every client, so everyone sees
    // an identical clock rather than each client running its own timer.
    jackpotRound.endsAt = Date.now() + JACKPOT_ROUND_MS;
    jackpotRound.timer = setTimeout(resolveJackpot, JACKPOT_ROUND_MS);
  }
  broadcastJackpot();
}

function resolveJackpot() {
  const round = jackpotRound;
  if (!round || round.entrants.length === 0) { jackpotRound = null; broadcastJackpot(); return; }
  if (round.entrants.length < JACKPOT_MIN_ENTRANTS) {
    // Shouldn't happen now that the timer is only ever scheduled once a 2nd
    // entrant joins (see handleJackpotEnter) - kept as a defensive fallback
    // in case that invariant is ever broken by a future change. Re-freezes
    // the clock rather than resolving/discarding a lone entrant's stake.
    round.endsAt = 0;
    round.timer = null;
    broadcastJackpot();
    return;
  }
  jackpotRound = null;

  const total = round.entrants.reduce((s, e) => s + e.value, 0);
  let r = Math.random() * total;
  let winner = round.entrants[round.entrants.length - 1];
  let cum = 0;
  for (const e of round.entrants) { cum += e.value; if (r <= cum) { winner = e; break; } }

  const coinsPortion = round.entrants.reduce((s, e) => s + e.coinsStaked, 0);
  const allItems = round.entrants.reduce((arr, e) => arr.concat(e.items), []);
  const u = ensureUser(winner.username);
  const before = u.coins;
  if (coinsPortion > 0) u.coins += coinsPortion;
  if (allItems.length) addItems(winner.username, allItems);
  trackWin(winner.username, total);
  // Someone can enter more than once in a round - history shows one row per player.
  const jpStakeBy = new Map();
  for (const e of round.entrants) jpStakeBy.set(e.username, (jpStakeBy.get(e.username) || 0) + e.value);
  persistUser(winner.username, { type: 'jackpot_win', amount: coinsPortion, balanceBefore: before, balanceAfter: u.coins, itemsTouched: [...new Set(allItems)], escrowRelease: round.entrants.map((e) => e.escrowId).filter(Boolean), game: { game: 'jackpot', wager: jpStakeBy.get(winner.username) || 0, payout: total } });
  const jpLossRecorded = new Set();
  for (const e of round.entrants) {
    if (e.username !== winner.username) {
      trackLoss(e.username, e.value);
      const first = !jpLossRecorded.has(e.username);
      jpLossRecorded.add(e.username);
      persistUser(e.username, { type: 'jackpot_loss', amount: 0, ...(first ? { game: { game: 'jackpot', wager: jpStakeBy.get(e.username) || 0, payout: 0 } } : {}) });
    }
  }
  // trackLoss above only touches the server's copy of each loser's stats -
  // without syncing them individually too, only the winner's own client
  // ever finds out anything happened, so everyone else's Profile tab (and
  // their own contribution to the leaderboard) just sits stale forever.
  for (const e of round.entrants) syncAccount(e.username);

  broadcast({
    type: 'jackpot:result',
    winner: winner.username,
    total,
    entrants: round.entrants.map((e) => ({ username: e.username, value: e.value })),
  });
  pushFeedEvent({ game: 'jackpot', username: winner.username, amount: total });
  broadcastJackpot();
}

// ---------------------------------------------------------------------------
// Case Battles
// ---------------------------------------------------------------------------
const battles = new Map(); // id -> battle

// Crazy/Jackpot/Terminal are winner-selection modifiers layered on top of the
// existing FFA/Team configs - not new modeConfigs entries. The client already
// simulates all three offline (mockRunBattle) for when it's disconnected from
// the server; runBattle() below mirrors that exact math so the animation the
// player already tested against matches the real, server-authoritative result.
const WIN_MODES = new Set(['normal', 'crazy', 'jackpot', 'terminal', 'coinflip']);
// Coinflip decides between exactly two sides, so it's only allowed in 1v1 and 2v2.
const COINFLIP_MODES = new Set(['ffa-2', 'team-2v2']);

function battleCost(caseQueue) {
  return caseQueue.reduce((sum, idx) => sum + caseData[idx].price, 0);
}
// Borrow mode: the creator picks 20-80% (steps of 5). Every player pays that
// much LESS for the cases, and every pull is worth that much less too - so the
// pot, the payout and the odds all scale together. 0 = off.
const BORROW_MIN = 20, BORROW_MAX = 80, BORROW_STEP = 5;
function parseBorrow(v) {
  const n = Math.round(Number(v));
  if (!Number.isFinite(n) || n < BORROW_MIN || n > BORROW_MAX) return 0;
  return Math.round(n / BORROW_STEP) * BORROW_STEP;
}
function borrowMultiplier(borrow) { return (100 - (borrow || 0)) / 100; }
// What one seat actually costs (rounded UP so borrowing never undercharges).
function battleEntryCost(caseQueue, borrow) {
  const full = battleCost(caseQueue);
  return borrow ? Math.max(1, Math.ceil(full * borrowMultiplier(borrow))) : full;
}
// Picks a team for a new auto-assigned player (a bot, or a join that didn't
// request a specific side) based on ACTUAL current occupancy - never blind
// alternation. Team choice (see handleCaseBattleJoin) means a team can fill
// up "out of turn" relative to simple join order, so anything that used to
// assume team === players.length % 2 could end up assigning into an
// already-full team once that happened - this is what actually caused a
// 3-vs-1 battle after a manually-chosen join left the teams uneven partway
// through. Always fills whichever team genuinely has room, preferring the
// less-filled side when both do.
function pickAutoTeam(b) {
  if (!b.cfg.isTeam) return 0;
  const teamSize = b.cfg.teamSize || Math.floor(b.cfg.count / 2);
  const teamACount = b.players.filter((p) => p.team === 0).length;
  const teamBCount = b.players.filter((p) => p.team === 1).length;
  if (teamACount < teamSize && teamACount <= teamBCount) return 0;
  if (teamBCount < teamSize) return 1;
  return 0; // both full - shouldn't be reachable since callers already check b.players.length < b.cfg.count first
}
function broadcastBattles() {
  broadcast({
    type: 'casebattle:list',
    battles: [...battles.values()].map((b) => ({
      id: b.id, creator: b.creator, status: b.status, mode: b.mode, winMode: b.winMode, borrow: b.borrow || 0,
      caseQueue: b.caseQueue, caseNames: b.caseNames,
      players: b.players.map((p) => ({ username: p.username, isBot: p.isBot, team: p.team, total: p.total || 0 })),
      winner: b.winner, winnerValue: b.winnerValue,
    })),
  });
}

const CASEBATTLE_MAX_CASES = 25;
function isValidCaseQueue(q) {
  return Array.isArray(q)
    && q.length >= 1
    && q.length <= CASEBATTLE_MAX_CASES
    && q.every((i) => Number.isInteger(i) && i >= 0 && i < caseData.length && caseData[i] && caseData[i].price > 0);
}

function handleCaseBattleCreate(username, msg) {
  if (!isValidCaseQueue(msg.caseQueue)) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Invalid case selection (1-' + CASEBATTLE_MAX_CASES + ' cases).' });
  }
  if (!modeConfigs[msg.mode]) msg.mode = 'ffa-2';
  const cfg = modeConfigs[msg.mode];
  let winMode = WIN_MODES.has(msg.winMode) ? msg.winMode : 'normal';
  if (cfg.isGroup) winMode = 'group'; // group battles have no winner - always an equal split
  if (winMode === 'coinflip' && !COINFLIP_MODES.has(msg.mode)) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Coinflip battles can only be 1v1 or 2v2.' });
  }
  const borrow = parseBorrow(msg.borrow);
  const cost = battleEntryCost(msg.caseQueue, borrow);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: "You don't have enough coins for this battle." });
  const before = u.coins;
  u.coins -= cost;
  trackWager(username, cost);
  const creatorEscrowId = newEscrowId('cb');
  persistUser(username, { type: 'casebattle_create_wager', amount: -cost, balanceBefore: before, balanceAfter: u.coins, escrowAdd: [{ id: creatorEscrowId, kind: 'casebattle', items: [], coins: cost }] });
  syncAccount(username);

  const id = 'battle_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const caseNames = msg.caseQueue.map((i) => caseData[i].name);
  battles.set(id, {
    id, creator: username, status: 'waiting', mode: msg.mode, winMode, borrow,
    caseQueue: msg.caseQueue, caseNames, cfg,
    players: [{ username, isBot: false, team: 0, total: 0, lastPull: 0, escrowId: creatorEscrowId }],
  });
  broadcastBattles();

  // Waiting is now player-driven (the creator clicks "Call Bots" whenever
  // they want, or waits for a real join) rather than a fixed countdown. This
  // is just a cleanup safety net for a battle that got truly abandoned -
  // e.g. the creator closed the tab and never came back.
  setTimeout(() => {
    const b = battles.get(id);
    if (b && b.status === 'waiting') startBattle(b);
  }, CASEBATTLE_ABANDON_MS);
}

function handleCaseBattleJoin(username, msg) {
  const b = battles.get(msg.battleId);
  if (!b || b.status !== 'waiting' || b.players.length >= b.cfg.count) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'That battle already started or is full.' });
  }
  if (b.players.some((p) => p.username === username)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You're already in this battle." });
  }
  const cost = battleEntryCost(b.caseQueue, b.borrow);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: "You don't have enough coins for this battle." });

  // The joining player can request which team to sit on - never trusted
  // blindly: it's only honored if it's actually a valid team index for this
  // battle's config AND that team still has room, exactly the same capacity
  // check that already governs the automatic/fallback assignment below.
  // Anything invalid (missing, out of range, or a full team) just falls
  // back to the original alternating auto-assignment, same as before this
  // feature existed.
  let team = pickAutoTeam(b);
  if (b.cfg.isTeam && msg.team !== undefined) {
    const requested = Number(msg.team);
    const teamCount = b.cfg.teamSize || Math.floor(b.cfg.count / 2);
    if (Number.isInteger(requested) && requested >= 0 && requested < 2) {
      const currentOnTeam = b.players.filter((p) => p.team === requested).length;
      if (currentOnTeam < teamCount) {
        team = requested;
      } else {
        return send(usernameToSocket.get(username), { type: 'error', message: 'That team is already full.' });
      }
    }
  }

  const before = u.coins;
  u.coins -= cost;
  trackWager(username, cost);
  const escrowId = newEscrowId('cb');
  persistUser(username, { type: 'casebattle_join_wager', amount: -cost, balanceBefore: before, balanceAfter: u.coins, escrowAdd: [{ id: escrowId, kind: 'casebattle', items: [], coins: cost }] });
  syncAccount(username);

  b.players.push({ username, isBot: false, team, total: 0, lastPull: 0, escrowId });
  broadcastBattles();

  if (b.players.length >= b.cfg.count) startBattle(b); // filled up early - start right away
}

function handleCaseBattleStart(username, msg) {
  const b = battles.get(msg.battleId);
  if (!b || b.status !== 'waiting' || b.creator !== username) return; // only the creator can force-start their own battle early
  startBattle(b);
}

// Fills exactly ONE empty seat with a bot (rather than the whole battle at
// once) so the waiting room can show per-seat "Call Bot" buttons, matching
// how real joins fill seats one at a time. Auto-starts once every seat is
// full, same as a real join filling the last spot.
function handleCaseBattleCallBot(username, msg) {
  const b = battles.get(msg.battleId);
  if (!b || b.status !== 'waiting' || b.creator !== username) return;
  if (b.players.length >= b.cfg.count) return;
  const usedNames = new Set(b.players.filter((p) => p.isBot).map((p) => p.username));
  const availablePool = botNamesBattles.filter((n) => !usedNames.has(n));
  const bots = pickBotNames(1, availablePool.length ? availablePool : botNamesBattles);
  // Team battles: the bot goes to the team whose seat was tapped, if that
  // team still has room; otherwise the usual automatic pick.
  let team = pickAutoTeam(b);
  if (b.cfg.isTeam && msg.team !== undefined) {
    const requested = Number(msg.team);
    const teamCount = b.cfg.teamSize || Math.floor(b.cfg.count / 2);
    if ((requested === 0 || requested === 1) && b.players.filter((p) => p.team === requested).length < teamCount) team = requested;
  }
  b.players.push({ username: bots[0], isBot: true, team, total: 0, lastPull: 0 });
  broadcastBattles();
  if (b.players.length >= b.cfg.count) startBattle(b);
}

// Lets the creator back out of their own battle before anyone/anything else
// has taken a seat - refunds their case cost and removes the battle. Once
// ANYONE else has a seat (a real player who has already staked their own
// coins, OR a bot the creator called in themselves), cancelling is blocked -
// previously only a real player joining blocked it, so a creator who called
// in even one bot could still cancel and effectively undo that bot seat,
// which is no longer allowed.
function handleCaseBattleCancel(username, msg) {
  const b = battles.get(msg.battleId);
  if (!b || b.creator !== username || b.status !== 'waiting') return;
  const anyoneElseJoined = b.players.length > 1;
  if (anyoneElseJoined) return;
  const cost = battleEntryCost(b.caseQueue, b.borrow);
  const u = ensureUser(username);
  const before = u.coins;
  u.coins += cost;
  untrackWager(username, cost);
  persistUser(username, { type: 'casebattle_cancel_refund', amount: cost, balanceBefore: before, balanceAfter: u.coins, escrowRelease: b.players.map((p) => p.escrowId).filter(Boolean) });
  syncAccount(username);
  battles.delete(b.id);
  broadcastBattles();
}

function startBattle(b) {
  if (b.status !== 'waiting') return;
  b.status = 'running';
  const needed = b.cfg.count - b.players.length;
  if (needed > 0) {
    const bots = pickBotNames(needed, botNamesBattles);
    bots.forEach((name) => {
      const team = pickAutoTeam(b);
      b.players.push({ username: name, isBot: true, team, total: 0, lastPull: 0 });
    });
  }
  broadcastBattles();
  runBattle(b);
}

async function runBattle(b) {
  const pullMult = borrowMultiplier(b.borrow);
  for (let roundIdx = 0; roundIdx < b.caseQueue.length; roundIdx++) {
    const pool = getPool(b.caseQueue[roundIdx]);
    const isFinalRound = roundIdx === b.caseQueue.length - 1;
    const roundPlayers = b.players.map((p) => {
      const rawPull = weightedPick(pool);
      // Borrow mode: every pull is worth borrow% less (same cut as the price).
      const pulled = pullMult === 1 ? rawPull : { ...rawPull, value: Math.floor(rawPull.value * pullMult) };
      p.total = (p.total || 0) + pulled.value;
      if (isFinalRound) p.lastPull = pulled.value; // Terminal only cares about this final-round pull
      return { username: p.username, pulled, total: p.total };
    });
    broadcast({ type: 'casebattle:round', battleId: b.id, roundIdx, players: roundPlayers });
    if (roundIdx < b.caseQueue.length - 1) await new Promise((r) => setTimeout(r, ROUND_PACE_MS));
  }

  const battleTotal = b.players.reduce((s, p) => s + p.total, 0); // winner takes ALL pulled value, not just their own side
  const perPlayerCost = battleEntryCost(b.caseQueue, b.borrow);
  const winMode = b.cfg.isGroup ? 'group' : (WIN_MODES.has(b.winMode) ? b.winMode : 'normal');

  // Group players the same way regardless of mode - teams for Team battles,
  // one group per player for FFA/Group - then each mode below just picks
  // which group wins. This mirrors the client's offline mock exactly, but
  // every number here is computed server-side; the client only animates it.
  let groups;
  if (b.cfg.isTeam) {
    const teamMap = new Map();
    b.players.forEach((p) => {
      if (!teamMap.has(p.team)) {
        teamMap.set(p.team, { key: String(p.team), label: `Team ${String.fromCharCode(65 + Number(p.team))}`, total: 0, lastPull: 0, members: [] });
      }
      const g = teamMap.get(p.team);
      g.total += p.total;
      g.lastPull += (p.lastPull || 0);
      g.members.push(p);
    });
    groups = [...teamMap.values()];
  } else {
    groups = b.players.map((p) => ({ key: p.username, label: p.username, total: p.total, lastPull: p.lastPull || 0, members: [p] }));
  }

  let winningGroups, jackpotInfo = null, coinflipInfo = null;
  if (winMode === 'group') {
    // Everyone "wins": the pot is split equally between every seat.
    winningGroups = groups;
  } else if (winMode === 'coinflip' && groups.length === 2) {
    // First side (creator / Team A) is Heads, the other Tails. The flip is
    // decided here on the server; the client only animates the coin.
    const result = crypto.randomInt(2) === 0 ? 'H' : 'T';
    const sides = groups.map((g, i) => ({ key: g.key, label: g.label, side: i === 0 ? 'H' : 'T' }));
    winningGroups = [groups[result === 'H' ? 0 : 1]];
    coinflipInfo = { result, sides };
  } else if (winMode === 'crazy') {
    // Crazy inverts the normal rule: lowest total value wins. A tie for
    // lowest splits the pot across every tied group's members.
    const best = Math.min(...groups.map((g) => g.total));
    winningGroups = groups.filter((g) => g.total === best);
  } else if (winMode === 'terminal') {
    // Terminal ignores the running total entirely - only the single biggest
    // pull from the final case/round decides it. A tie on that final pull
    // splits the pot the same way.
    const best = Math.max(...groups.map((g) => g.lastPull));
    winningGroups = groups.filter((g) => g.lastPull === best);
  } else if (winMode === 'jackpot') {
    // Weighted-random by each group's share of the total pot. The roll and
    // the resulting percentages are both computed here, server-side, so the
    // client's spin animation is just a replay of an already-decided result.
    // A random roll always lands on exactly one group, so there's no tie
    // case to split here even if two groups have identical odds.
    const totalForOdds = groups.reduce((s, g) => s + g.total, 0) || 1;
    let roll = Math.random() * totalForOdds;
    let winningGroup = groups[groups.length - 1];
    let cum = 0;
    for (const g of groups) { cum += g.total; if (roll <= cum) { winningGroup = g; break; } }
    winningGroups = [winningGroup];
    jackpotInfo = groups.map((g) => ({ key: g.key, label: g.label, percent: Math.round((g.total / totalForOdds) * 1000) / 10 }));
  } else {
    // Normal: highest total value wins. A tie for highest splits the pot
    // across every tied group's members.
    const best = Math.max(...groups.map((g) => g.total));
    winningGroups = groups.filter((g) => g.total === best);
  }

  const winner = winningGroups.map((g) => g.label).join(' & '); // e.g. "Team A & Team B" or "alice & bob" on a tie, otherwise just the one label
  const winnerValue = battleTotal; // winning side(s) take the full combined pot, same as before
  const winningMembers = winningGroups.flatMap((g) => g.members);
  const share = Math.floor(battleTotal / Math.max(1, winningMembers.length));
  // Every real player's stake is released atomically with the FIRST payout
  // write below (or the first loss record if only bots won).
  let pendingEscrowRelease = b.players.filter((p) => !p.isBot && p.escrowId).map((p) => p.escrowId);
  const takeEscrowRelease = () => { const ids = pendingEscrowRelease; pendingEscrowRelease = []; return ids; };

  winningMembers.filter((p) => !p.isBot).forEach((p) => {
    const u = ensureUser(p.username);
    const before = u.coins;
    u.coins += share;
    trackWin(p.username, share);
    persistUser(p.username, { type: 'casebattle_win', amount: share, balanceBefore: before, balanceAfter: u.coins, escrowRelease: takeEscrowRelease(), game: { game: 'battles', wager: perPlayerCost, payout: share } });
    syncAccount(p.username);
    createNotification({
      username: p.username,
      type: 'casebattle_win',
      title: '🏆 Case Battle won',
      message: `You won ${share.toLocaleString()} from the Case Battle.`,
      refId: b.id,
    });
  });
  b.players.filter((p) => !winningMembers.includes(p) && !p.isBot).forEach((p) => {
    trackLoss(p.username, perPlayerCost);
    persistUser(p.username, { type: 'casebattle_loss', amount: 0, escrowRelease: takeEscrowRelease(), game: { game: 'battles', wager: perPlayerCost, payout: 0 } });
  });

  b.status = 'finished';
  b.winner = winner;
  b.winnerValue = winnerValue;
  broadcastBattles();
  broadcast({
    type: 'casebattle:finished', battleId: b.id, winner, winnerValue, winMode, jackpotInfo, coinflipInfo,
    // Explicit per-winner personal earnings for the end-of-battle summary
    // screen - `share` here is the exact same value each winning member's
    // account was actually credited with above, not a client-side estimate
    // recomputed from totals. Bots are included (isBot flag) so the client
    // can still show a complete team breakdown for 2v2 without crediting
    // them a real payout.
    winners: winningMembers.map((p) => ({ username: p.username, earnings: share, isBot: !!p.isBot })),
    players: b.players.map((p) => ({ username: p.username, total: p.total, lastPull: p.lastPull || 0 })),
  });
  pushFeedEvent({ game: 'battle', username: winner, amount: winnerValue });

  // Keep the finished result visible for a short window, then drop it from
  // the shared list so the battles view doesn't fill up with stale rows.
  setTimeout(() => {
    const current = battles.get(b.id);
    if (current && current.status === 'finished') {
      battles.delete(b.id);
      broadcastBattles();
    }
  }, CASEBATTLE_FINISHED_DISPLAY_MS);
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

// Practice-mode convenience: resets your OWN account back to the starting
// balance. This is intentionally a "give yourself money" cheat - fine for a
// solo hobby server, but if you ever have real friends playing against each
// other here for keeps, you may want to remove this handler so balances
// actually mean something in head-to-head games.
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
// Ranks every account the server has data for by lifetime stats. Bots
// aren't real accounts (never touched via ensureUser), so they naturally
// never appear here - only real players.
const LEADERBOARD_SIZE = 25;
// Built at most once every few seconds and shared by everyone who asks,
// instead of re-sorting every account on every single request.
let leaderboardCache = null;
let leaderboardCacheAt = 0;
const LEADERBOARD_CACHE_MS = 5000;
function handleLeaderboardRequest(username) {
  if (leaderboardCache && Date.now() - leaderboardCacheAt < LEADERBOARD_CACHE_MS) {
    return send(usernameToSocket.get(username), leaderboardCache);
  }
  const rows = [...users.entries()]
    // DTN_BGSI (the hand-out/giveaway + trading account) isn't a real
    // player - it exists purely to fund tips/trades, and its stats can run
    // up huge numbers just from moving stock around. It's deliberately
    // excluded from ranking here, same as it's excluded from the account
    // reset script - this only hides it from the leaderboard, its actual
    // stats are untouched.
    .filter(([uname]) => uname.toLowerCase() !== GIVEAWAY_USERNAME.toLowerCase())
    .map(([uname, u]) => ({
      username: uname,
      wagered: (u.stats && u.stats.wagered) || 0,
      won: (u.stats && u.stats.won) || 0,
      lost: (u.stats && u.stats.lost) || 0,
    }));
  const topBy = (key) => rows
    .filter((r) => r[key] > 0)
    .sort((a, b) => b[key] - a[key])
    .slice(0, LEADERBOARD_SIZE)
    .map((r) => ({ username: r.username, value: r[key] }));

  leaderboardCache = {
    type: 'leaderboard:data',
    mostPlayed: topBy('wagered'),
    mostLost: topBy('lost'),
    mostWon: topBy('won'),
  };
  leaderboardCacheAt = Date.now();
  send(usernameToSocket.get(username), leaderboardCache);
}

// Mines, Jackpot, and single Cases still run their actual game logic
// (RNG, tile layout, odds) entirely client-side, same as before - this
// just mirrors the outcome to the server afterward so coins/stats stay
// authoritative and don't get silently overwritten by the next account
// sync. Trust-based (the client reports its own outcome) rather than
// server-verified - reasonable for a small friend-group server, not
// intended to resist a genuinely adversarial client.
//
// `items` is an optional array of catalogIds - Jackpot's pets-staking mode
// stakes items instead of (or alongside) coins, so those need to move in
// and out of inventory here too, not just the coin balance. Without this,
// a client that "removed" staked items locally would just watch them
// reappear on the very next syncAccount, since the server's copy never
// actually changed.
function handleGameWager(username, msg) {
  const amount = Math.max(0, Math.floor(Number(msg.amount) || 0));
  const items = Array.isArray(msg.items) ? msg.items.filter(id => typeof id === 'string') : [];
  if (amount <= 0 && items.length === 0) return;
  if (msg.game === 'mines' && amount > MINES_MAX_BET) {
    return send(usernameToSocket.get(username), { type: 'error', message: `Mines is capped at ${MINES_MAX_BET} coins per round.` });
  }
  const u = ensureUser(username);
  if (amount > u.coins) return; // can't wager more than they actually have
  // Real ownership check before removing anything - without this, staking
  // an item you don't own would silently no-op the removal (clamped at 0)
  // but still count at full catalog value toward the wagered stat, letting
  // anyone inflate their leaderboard standing with items they never
  // actually risked.
  if (items.length && !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  const before = u.coins;
  if (amount > 0) u.coins -= amount;
  if (items.length) removeItems(username, items);
  trackWager(username, amount > 0 ? amount : stakeValue(items));
  persistUser(username, { type: `${msg.game || 'game'}_wager`, amount: -amount, balanceBefore: before, balanceAfter: u.coins, itemsTouched: items });
  syncAccount(username);
}

const MINES_MAX_BET = 500000; // must match the client's MINES_MAX_BET
const MINES_VALID_GRID_SIZES = new Set([25, 36, 49, 64]); // 5x5, 6x6, 7x7, 8x8 - must match the client's grid-size buttons

// Mirrors the client's combinations()/minesMultiplier() exactly, so the
// server can independently work out the mathematically maximum legitimate
// payout for a claimed (grid size, mine count, tiles revealed) combination
// and reject anything that claims more than that's actually possible -
// rather than trusting whatever number the client reports outright.
function combinations(n, k) {
  if (k < 0 || k > n) return 0;
  k = Math.min(k, n - k);
  let result = 1;
  for (let i = 0; i < k; i++) result = (result * (n - i)) / (i + 1);
  return result;
}
function minesMultiplier(n, m, picks) {
  if (picks === 0) return 1;
  const HOUSE_EDGE = 0.03;
  const fair = combinations(n, picks) / combinations(n - m, picks);
  return fair * (1 - HOUSE_EDGE);
}

// ---------------------------------------------------------------------------
// Mines - fully server-authoritative. The bomb layout is generated and held
// here only, keyed by username; the client never receives bomb positions
// until the exact moment a tile revealing one is actually clicked (a hit),
// or the round ends and every remaining bomb is revealed for the board
// animation. This replaces the old design where the client generated its
// own bombs and simply self-reported the outcome via a generic
// game:wager/game:resolve pair - that only had a mathematical ceiling on
// the claimed payout, not real verification of what tiles were actually
// safe. game:wager/game:resolve are no longer used by Mines at all now
// (nothing else in the client sends them either), so those two handlers
// above are effectively unused dead code now - left in place rather than
// removed since touching them isn't necessary and isn't worth the risk.
//
// ---------------------------------------------------------------------------
// SINK — a Crash-style rising-multiplier game (there's no pre-existing
// "Crash" implementation in this file to reuse - this is a fresh build,
// following the same server-authoritative model as everything else here:
// the server alone determines the multiplier, the sink point, and every
// payout; the client only ever renders what it's told).
//
// CF coins only - no pets/items accepted as wagers. Capped at
// SINK_MAX_BET per player per round, enforced here server-side regardless
// of what a client sends.
//
// Rounds loop continuously and are shared by everyone (unlike Mines, which
// is one board per player): a 30s joining phase where anyone can place one
// wager, then an active phase of unpredictable length where the multiplier
// climbs from 1.00x until the server's secretly pre-chosen sink point is
// reached, then a short pause showing the result before the next round's
// joining phase begins automatically.
//
// Fairness: the sink point is generated fresh each round the moment the
// active phase begins (generateSinkPoint), using the same "plain
// server-side Math.random(), never revealed until it matters" approach the
// rest of this file already uses for Mines' bomb layout, Dice Duel's
// rolls, etc. - not a cryptographic commit-reveal scheme, consistent with
// how every other game here already works.
//
// The multiplier is never pushed to clients on a timer. Instead, the
// server sends one timestamp (activeStartAt) plus the fixed growth rate
// when the active phase begins, and every client computes and animates
// the climbing number locally from elapsed time - exactly like Jackpot's
// countdown already works. The server independently recomputes the
// multiplier from elapsed time whenever it actually matters (a cashout
// request), so a client can never claim a multiplier it hasn't earned.
// ---------------------------------------------------------------------------
const SINK_MAX_BET = 500000;
const SINK_JOIN_MS = 30000;
const SINK_SUNK_PAUSE_MS = 6000; // how long the result stays up before the next round's joining phase starts
const SINK_GROWTH_K = 0.13; // multiplier(t) = e^(SINK_GROWTH_K * t), t in seconds - reaches ~5x around t=12-13s

let sinkRound = null; // set by startSinkJoiningPhase() at boot - see server startup below
// Where the last few rounds sank (newest first), shown above the game. Saved
// so it survives restarts.
const SINK_HISTORY_SIZE = 10;
let sinkHistory = [];
let sinkTimer = null; // whichever timeout is currently driving the round forward

function sinkMultiplierAt(elapsedSeconds) {
  return Math.exp(SINK_GROWTH_K * elapsedSeconds);
}

// ---------------------------------------------------------------------------
// SINK ODDS - edit this table to change how far the ship tends to go.
// Each row is [multiplier, % chance the ship gets PAST that multiplier]
// (= the chance a player who cashes out there wins). Rows must go up in
// multiplier and down in chance. Between rows the chance changes smoothly.
// Every round that doesn't get past the first row sinks at 1.00x.
// 1.00x-2.00x keep the previous odds; 5x/10x/20x/100x are set by the owner.
// ---------------------------------------------------------------------------
const SINK_ODDS = [
  [1.00, 95.21],
  [1.02, 93.49],
  [1.05, 90.89],
  [1.10, 86.36],
  [1.20, 78.25],
  [1.30, 71.14],
  [1.40, 64.84],
  [1.50, 59.18],
  [1.75, 50.78],
  [2.00, 44.46],
  [5.00, 2.9],
  [10.00, 0.7],
  [20.00, 0.01],
  [100.00, 0.001],
];
const SINK_MAX_POINT = 1000; // absolute ceiling for a round

function sinkChanceAbove(m) {
  // % chance the ship gets past multiplier m (used by tests / for reference).
  if (m < SINK_ODDS[0][0]) return 100;
  for (let i = 0; i < SINK_ODDS.length - 1; i++) {
    const [m1, p1] = SINK_ODDS[i], [m2, p2] = SINK_ODDS[i + 1];
    if (m <= m2) return p1 * Math.pow(p2 / p1, Math.log(m / m1) / Math.log(m2 / m1));
  }
  const [mA, pA] = SINK_ODDS[SINK_ODDS.length - 2], [mB, pB] = SINK_ODDS[SINK_ODDS.length - 1];
  return pB * Math.pow(m / mB, Math.log(pB / pA) / Math.log(mB / mA)); // keep the last slope past the end
}

function generateSinkPoint() {
  // u = % chance to beat the result. Walk the table to find where the ship sinks.
  const u = (crypto.randomInt(0, 2 ** 48 - 1) + 0.5) / 2 ** 48 * 100;
  if (u >= SINK_ODDS[0][1]) return 1.00;
  let point = null;
  for (let i = 0; i < SINK_ODDS.length - 1 && point == null; i++) {
    const [m1, p1] = SINK_ODDS[i], [m2, p2] = SINK_ODDS[i + 1];
    if (u >= p2) point = m1 * Math.pow(m2 / m1, Math.log(u / p1) / Math.log(p2 / p1));
  }
  if (point == null) {
    const [mA, pA] = SINK_ODDS[SINK_ODDS.length - 2], [mB, pB] = SINK_ODDS[SINK_ODDS.length - 1];
    point = mB * Math.pow(u / pB, Math.log(mB / mA) / Math.log(pB / pA));
  }
  // Round UP to the cent, so "gets past 2.00x" happens exactly as often as the table says.
  return Math.min(SINK_MAX_POINT, Math.max(1.00, Math.ceil(point * 100 - 1e-9) / 100));
}

function sinkPublicState(round) {
  const players = [...round.players.entries()].map(([username, p]) => ({
    username, wager: p.wager,
    cashedOutAt: p.cashedOutAt, // multiplier they cashed out at, or null if still in / didn't make it
    payout: p.payout, // null until they cash out
  }));
  // `now` lets clients measure their own clock offset against the server's,
  // since elapsed-time-based multiplier math (see sinkMultiplierAt) is only
  // correct if the client's Date.now() agrees with the server's - without
  // this, any clock drift between the two gets amplified exponentially into
  // the displayed multiplier (a client running behind the server's clock
  // shows a slower climb than what's actually being computed server-side).
  const base = { phase: round.phase, players, now: Date.now(), history: sinkHistory };
  if (round.phase === 'joining') return { ...base, joinEndsAt: round.joinEndsAt };
  if (round.phase === 'active') return { ...base, activeStartAt: round.activeStartAt, growthRate: SINK_GROWTH_K };
  // 'sunk' - safe to reveal the actual sink point now that the round is fully over
  return { ...base, activeStartAt: round.activeStartAt, growthRate: SINK_GROWTH_K, sunkMultiplier: round.sinkPoint };
}

function broadcastSinkState() {
  broadcast({ type: 'sink:state', ...sinkPublicState(sinkRound) });
}

function startSinkJoiningPhase() {
  if (sinkTimer) clearTimeout(sinkTimer);
  sinkRound = {
    phase: 'joining',
    joinEndsAt: Date.now() + SINK_JOIN_MS,
    players: new Map(), // username -> { wager, cashedOutAt, payout }
    activeStartAt: null,
    sinkPoint: null, // chosen (and kept secret) once the active phase starts
  };
  broadcastSinkState();
  sinkTimer = setTimeout(startSinkActivePhase, SINK_JOIN_MS);
}

function startSinkActivePhase() {
  const round = sinkRound;
  if (!round || round.phase !== 'joining') return;
  round.phase = 'active';
  round.activeStartAt = Date.now();
  round.sinkPoint = generateSinkPoint();
  broadcastSinkState();

  const tSinkSeconds = Math.log(round.sinkPoint) / SINK_GROWTH_K;
  sinkTimer = setTimeout(() => resolveSinkRound(round), Math.max(0, tSinkSeconds * 1000));
}

function resolveSinkRound(round) {
  if (!round || round.phase !== 'active' || round !== sinkRound) return;
  round.phase = 'sunk';
  sinkHistory = [round.sinkPoint, ...sinkHistory].slice(0, SINK_HISTORY_SIZE);
  db.saveServerState('sinkHistory', { points: sinkHistory }).catch((err) => console.error('[db] Failed to persist sink history:', err.message));

  for (const [username, p] of round.players.entries()) {
    if (p.cashedOutAt == null) {
      // Didn't cash out in time - the wager was already deducted at join
      // time, so this is purely recording the loss for stats/feed purposes.
      trackLoss(username, p.wager);
      persistUser(username, { type: 'sink_loss', amount: 0, escrowRelease: [p.escrowId].filter(Boolean), game: { game: 'sink', wager: p.wager, payout: 0 } });
      pushFeedEvent({ game: 'sink', username, amount: p.wager, won: false });
    }
  }

  broadcastSinkState();
  sinkTimer = setTimeout(startSinkJoiningPhase, SINK_SUNK_PAUSE_MS);
}

function handleSinkJoin(username, msg) {
  const errTo = usernameToSocket.get(username);
  if (!sinkRound || sinkRound.phase !== 'joining') {
    return send(errTo, { type: 'error', message: 'SINK is not accepting bets right now - wait for the next round.' });
  }
  if (sinkRound.players.has(username)) {
    return send(errTo, { type: 'error', message: 'You already joined this round.' });
  }

  const amount = Math.floor(Number(msg.amount) || 0);
  if (!amount || amount < 1) return send(errTo, { type: 'error', message: 'Enter a valid amount.' });
  if (amount > SINK_MAX_BET) return send(errTo, { type: 'error', message: `SINK is capped at ${SINK_MAX_BET.toLocaleString()} coins per round.` });

  const u = ensureUser(username);
  if (amount > u.coins) return send(errTo, { type: 'error', message: 'Not enough coins.' });

  const before = u.coins;
  u.coins -= amount;
  const sinkEscrowId = newEscrowId('sink');
  sinkRound.players.set(username, { wager: amount, cashedOutAt: null, payout: null, escrowId: sinkEscrowId });

  trackWager(username, amount);
  persistUser(username, { type: 'sink_wager', amount: -amount, balanceBefore: before, balanceAfter: u.coins, escrowAdd: [{ id: sinkEscrowId, kind: 'sink', items: [], coins: amount }] });
  syncAccount(username);
  broadcastSinkState();
}

function handleSinkCashout(username) {
  const errTo = usernameToSocket.get(username);
  const round = sinkRound;
  if (!round || round.phase !== 'active') {
    return send(errTo, { type: 'error', message: 'There is no active SINK round to cash out of.' });
  }
  const p = round.players.get(username);
  if (!p) return send(errTo, { type: 'error', message: "You didn't join this round." });
  if (p.cashedOutAt != null) return send(errTo, { type: 'error', message: 'You already cashed out this round.' });

  const elapsedSeconds = (Date.now() - round.activeStartAt) / 1000;
  const currentMultiplier = sinkMultiplierAt(elapsedSeconds);
  if (currentMultiplier >= round.sinkPoint) {
    // It sank right as this request arrived - nothing to pay out. The
    // resolve timer handles (or already handled) recording this as a loss.
    return send(errTo, { type: 'error', message: 'Too late - the ship already sank.' });
  }

  const payout = Math.floor(p.wager * currentMultiplier);
  p.cashedOutAt = currentMultiplier;
  p.payout = payout;

  const u = ensureUser(username);
  const before = u.coins;
  u.coins += payout;
  trackWin(username, payout);
  persistUser(username, { type: 'sink_cashout', amount: payout, balanceBefore: before, balanceAfter: u.coins, escrowRelease: [p.escrowId].filter(Boolean), game: { game: 'sink', wager: p.wager, payout } });
  syncAccount(username);
  pushFeedEvent({ game: 'sink', username, amount: payout, multiplier: currentMultiplier, won: true });
  broadcastSinkState();
}

// One active round per username at a time (matches how the client only
// ever showed one board anyway). Kept in memory only, same as every other
// live game state in this file - if the process restarts mid-round, that
// round is simply gone, same as it always would have been.
const activeMinesRounds = new Map(); // username -> { n, mCount, gridSize, amount, bombs:Set, revealed:Set, picks, active }

function handleMinesStart(username, msg) {
  const errTo = usernameToSocket.get(username);
  if (activeMinesRounds.has(username)) {
    return send(errTo, { type: 'error', message: 'You already have an active Mines round.' });
  }

  const amount = Math.max(0, Math.floor(Number(msg.amount) || 0));
  const mCount = Math.floor(Number(msg.mineCount));
  const gridSize = Math.floor(Number(msg.gridSize));
  const n = gridSize * gridSize;

  if (!amount || amount < 1) return send(errTo, { type: 'error', message: 'Enter a valid amount.' });
  if (amount > MINES_MAX_BET) return send(errTo, { type: 'error', message: `Mines is capped at ${MINES_MAX_BET} coins per round.` });
  if (!MINES_VALID_GRID_SIZES.has(n)) return send(errTo, { type: 'error', message: 'Invalid grid size.' });
  if (!mCount || mCount < 1 || mCount >= n) return send(errTo, { type: 'error', message: 'Invalid mine count.' });

  const u = ensureUser(username);
  if (amount > u.coins) return send(errTo, { type: 'error', message: 'Not enough coins.' });

  const before = u.coins;
  u.coins -= amount;

  // Fisher-Yates partial shuffle to pick mCount unique bomb tiles out of n -
  // simple, correct, and the result is never sent to the client up front.
  const positions = Array.from({ length: n }, (_, i) => i);
  for (let i = positions.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [positions[i], positions[j]] = [positions[j], positions[i]];
  }
  const bombs = new Set(positions.slice(0, mCount));

  const minesEscrowId = newEscrowId('mines');
  activeMinesRounds.set(username, { n, mCount, gridSize, amount, bombs, revealed: new Set(), picks: 0, active: true, escrowId: minesEscrowId });

  trackWager(username, amount);
  persistUser(username, { type: 'mines_wager', amount: -amount, balanceBefore: before, balanceAfter: u.coins, escrowAdd: [{ id: minesEscrowId, kind: 'mines', items: [], coins: amount }] });
  syncAccount(username);
  send(errTo, { type: 'mines:started', n, mCount, gridSize, amount });
}

function handleMinesReveal(username, msg) {
  const errTo = usernameToSocket.get(username);
  const round = activeMinesRounds.get(username);
  if (!round || !round.active) return send(errTo, { type: 'error', message: 'No active Mines round.' });

  const tileIndex = Math.floor(Number(msg.tileIndex));
  if (!Number.isInteger(tileIndex) || tileIndex < 0 || tileIndex >= round.n) {
    return send(errTo, { type: 'error', message: 'Invalid tile.' });
  }
  if (round.revealed.has(tileIndex)) {
    return send(errTo, { type: 'error', message: 'That tile is already revealed.' });
  }
  round.revealed.add(tileIndex);

  if (round.bombs.has(tileIndex)) {
    round.active = false;
    activeMinesRounds.delete(username);
    trackLoss(username, round.amount);
    persistUser(username, { type: 'mines_loss', amount: 0, escrowRelease: [round.escrowId].filter(Boolean), game: { game: 'mines', wager: round.amount, payout: 0 } });
    // The round is over - safe to reveal every bomb now for the board
    // animation, since none of these positions were ever sent before this.
    send(errTo, { type: 'mines:hit', tileIndex, bombs: [...round.bombs] });
    pushFeedEvent({ game: 'mines', username, amount: round.amount, won: false });
    return;
  }

  round.picks++;
  const multiplier = minesMultiplier(round.n, round.mCount, round.picks);
  const currentPayout = Math.floor(round.amount * multiplier);
  const safeTilesTotal = round.n - round.mCount;

  if (round.picks === safeTilesTotal) {
    // Every safe tile revealed - auto cash out, matching existing behavior.
    const u = ensureUser(username);
    const before = u.coins;
    u.coins += currentPayout;
    trackWin(username, currentPayout);
    round.active = false;
    activeMinesRounds.delete(username);
    persistUser(username, { type: 'mines_resolve', amount: currentPayout, balanceBefore: before, balanceAfter: u.coins, escrowRelease: [round.escrowId].filter(Boolean), game: { game: 'mines', wager: round.amount, payout: currentPayout } });
    syncAccount(username);
    send(errTo, { type: 'mines:revealed', tileIndex, multiplier, payout: currentPayout, picks: round.picks, fullClear: true });
    pushFeedEvent({ game: 'mines', username, amount: currentPayout, multiplier, won: true });
    return;
  }

  send(errTo, { type: 'mines:revealed', tileIndex, multiplier, payout: currentPayout, picks: round.picks, fullClear: false });
}

function handleMinesCashout(username) {
  const errTo = usernameToSocket.get(username);
  const round = activeMinesRounds.get(username);
  if (!round || !round.active) return send(errTo, { type: 'error', message: 'No active Mines round.' });
  if (round.picks === 0) return send(errTo, { type: 'error', message: 'Reveal at least one tile before cashing out.' });

  const multiplier = minesMultiplier(round.n, round.mCount, round.picks);
  const payout = Math.floor(round.amount * multiplier);

  round.active = false;
  activeMinesRounds.delete(username);

  const u = ensureUser(username);
  const before = u.coins;
  u.coins += payout;
  trackWin(username, payout);
  persistUser(username, { type: 'mines_resolve', amount: payout, balanceBefore: before, balanceAfter: u.coins, escrowRelease: [round.escrowId].filter(Boolean), game: { game: 'mines', wager: round.amount, payout } });
  syncAccount(username);
  send(errTo, { type: 'mines:cashedOut', payout, multiplier });
  pushFeedEvent({ game: 'mines', username, amount: payout, multiplier, won: true });
}

function handleGameResolve(username, msg) {
  const payout = Math.max(0, Math.floor(Number(msg.payout) || 0));
  const wager = Math.max(0, Math.floor(Number(msg.wager) || 0));

  // WHITELIST, not blacklist: game:resolve only ever pays out for games
  // explicitly listed here. Cases used to be accepted here too and paid out
  // whatever the client claimed it won - now cases go through the fully
  // server-authoritative case:open handler instead, and this endpoint
  // rejects anything that isn't on the list below, no matter what game
  // string is sent. If you're adding a new game here, it needs the same
  // server-side outcome verification mines has (the wager cap below) or
  // its own dedicated authoritative handler like case:open - never blindly
  // trust a client-reported payout.
  const ALLOWED_RESOLVE_GAMES = new Set(['mines']);
  if (!ALLOWED_RESOLVE_GAMES.has(msg.game)) return;

  // Belt-and-suspenders: the client already stops you from starting a Mines
  // round above this, but don't trust that alone - reject anything that
  // claims a bigger wager was involved.
  if (msg.game === 'mines' && wager > MINES_MAX_BET) {
    return send(usernameToSocket.get(username), { type: 'error', message: `Mines is capped at ${MINES_MAX_BET} coins per round.` });
  }
  // The actual reveal-by-reveal Mines round still plays out client-side
  // (no per-tile round trip to the server), so this can't verify the exact
  // tiles that were revealed - but it CAN verify the claim is mathematically
  // possible at all. A claimed win reports the grid size, mine count, and
  // how many safe tiles were revealed; the server recomputes the real
  // maximum multiplier for that exact shape using the same formula the
  // client uses, and rejects anything claiming more than that - so instead
  // of "type any number you want," the ceiling is now "the best possible
  // truthful outcome for whatever round shape you claim happened."
  if (msg.game === 'mines' && payout > 0) {
    const n = Math.floor(Number(msg.n));
    const mCount = Math.floor(Number(msg.mCount));
    const picks = Math.floor(Number(msg.picks));
    const validShape = MINES_VALID_GRID_SIZES.has(n) && mCount >= 1 && mCount <= n - 1 && picks >= 1 && picks <= n - mCount;
    if (!validShape) {
      return send(usernameToSocket.get(username), { type: 'error', message: 'Invalid Mines round - payout rejected.' });
    }
    const maxMultiplier = minesMultiplier(n, mCount, picks);
    const maxPayout = wager * maxMultiplier;
    if (payout > maxPayout * 1.0001) { // tiny tolerance for floating-point rounding, not a real gap
      return send(usernameToSocket.get(username), { type: 'error', message: 'That payout is higher than mathematically possible for this round - rejected.' });
    }
  }
  const u = ensureUser(username);
  const before = u.coins;
  if (payout > 0) {
    u.coins += payout;
    trackWin(username, payout);
  } else if (wager > 0) {
    trackLoss(username, wager);
  }
  // Mines (the only game left on this endpoint) never legitimately awards
  // items, only coins - so items from the client are never trusted or
  // credited here at all, closing off what would otherwise be another
  // "just add whatever catalog ids you want" gap even after the payout
  // itself got locked down above.
  persistUser(username, { type: `${msg.game || 'game'}_resolve`, amount: payout, balanceBefore: before, balanceAfter: u.coins });
  syncAccount(username);

  // Feed it to the site-wide live feed too (Coinflip/Battles/Jackpot log
  // their own feed entries directly at the point they resolve).
  if (payout > 0) {
    const multiplier = Number(msg.multiplier) || (wager > 0 ? payout / wager : 0);
    pushFeedEvent({ game: 'mines', username, amount: payout, multiplier, won: true });
  } else if (wager > 0) {
    pushFeedEvent({ game: 'mines', username, amount: wager, won: false });
  }
}

// Server-authoritative single case opening. This exists specifically to
// close a real exploit: the previous flow let the client run its own
// weightedPick() locally and simply report the "result" via game:resolve,
// which the server credited unconditionally - anyone could fabricate a
// win of the rarest item in the catalog via devtools with no actual roll.
// Here the price, the roll, and the resulting payout are all decided
// server-side against the real catalog pool (casePools) - the client only
// gets told what it won after the fact, purely for the reel animation.
// Matches the existing game design: pulls convert straight to coins, no
// inventory item is ever actually added.
function handleCaseOpen(username, msg) {
  const errTo = usernameToSocket.get(username);
  const caseIndex = Math.floor(Number(msg.caseIndex));
  const c = caseData[caseIndex];
  if (!c) return send(errTo, { type: 'error', message: 'That case does not exist.' });
  const pool = casePools['b' + caseIndex];
  if (!pool || !pool.length) return send(errTo, { type: 'error', message: 'That case has no items configured yet.' });

  const u = ensureUser(username);
  if (u.coins < c.price) return send(errTo, { type: 'error', message: "You don't have enough coins for this case." });

  const before = u.coins;
  u.coins -= c.price;

  const total = pool.reduce((s, e) => s + e.chance, 0);
  let roll = Math.random() * total;
  let wonEntry = pool[pool.length - 1];
  for (const entry of pool) {
    roll -= entry.chance;
    if (roll <= 0) { wonEntry = entry; break; }
  }
  const wonId = wonEntry.id;
  const wonPet = petCatalog[wonId];
  if (!wonPet) {
    // Data integrity problem (a pool entry pointing at a catalog id that
    // doesn't exist) - refund rather than silently eat their coins.
    u.coins = before;
    return send(errTo, { type: 'error', message: 'Something went wrong opening that case - try again.' });
  }

  // A pool entry can carry its own "value" that overrides the pet's real
  // catalog value - lets a case reuse existing pet art/names at a payout
  // specific to that case (same design as Case Battles' caseItems), without
  // touching the pet's value everywhere else it appears.
  const payout = (typeof wonEntry.value === 'number') ? wonEntry.value : wonPet.value;

  u.coins += payout; // pulls convert straight to coins, matching the existing "no inventory needed" case design
  trackWager(username, c.price);
  persistUser(username, { type: 'case_open', amount: payout - c.price, balanceBefore: before, balanceAfter: u.coins, game: { game: 'cases', wager: c.price, payout } });
  syncAccount(username);
  send(errTo, {
    type: 'case:opened',
    caseIndex,
    wonId,
    name: wonPet.name,
    value: payout,
    rarity: wonPet.rarity,
    pool: pool.map((e) => ({ id: e.id, chance: e.chance, value: e.value })),
  });
  pushFeedEvent({ game: 'cases', username, amount: payout, itemName: wonPet.name });
}

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
    transactions,
  });
}

// Admin-only balance adjustment - covers both "add coins" and "remove
// coins" (removeCoins is just addCoins with a negated, clamped amount).
// Every call writes a real transaction row via the same persistUser() path
// every other coin-mutating action already uses, so this shows up in the
// user's transaction history exactly like a game payout or a tip would,
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

  const req = { id: 'wd_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8), username, items, requestedAt: Date.now(), status: 'pending' };
  try {
    await db.insertWithdrawRequest(req);
  } catch (err) {
    console.error(`[db] Failed to persist withdraw request for ${username}:`, err.message);
    return send(errTo, { type: 'error', message: 'Something went wrong submitting that - try again in a moment.' });
  }
  withdrawRequests.push(req);
  send(errTo, { type: 'withdraw:requested', id: req.id });
  // Immediately pushes the now-locked items out of their available
  // inventory client-side (My Items, Coinflip/Jackpot/Dice Duel/Exchange
  // pickers, etc.) so they physically can't select and gamble away pets
  // that are sitting in a pending withdrawal request.
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
  if (!req || req.resolving) return;
  req.resolving = true; // blocks a double-click from running this twice while we wait on the DB

  // Work out what will be removed (capped by what they still own), but DON'T
  // touch the inventory until the DB has confirmed the pending->fulfilled
  // transition. The old order removed pets from memory first, so if the DB
  // call failed (or lost a race) the pets were gone from the player's
  // inventory without the request ever being marked fulfilled.
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
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'fulfilled', plannedRemoval);
  } catch (err) {
    req.resolving = false;
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return send(usernameToSocket.get(username), { type: 'error', message: 'Database error - nothing was removed. Try again.' });
  }
  if (!ok) { req.resolving = false; return; } // someone/something else already resolved this request

  // Apply now (items were locked the whole time, so they're still there).
  const actuallyRemoved = {};
  for (const [id, qty] of Object.entries(plannedRemoval)) {
    const owned = target.inventory[id] || 0;
    const removeQty = Math.min(qty, owned);
    if (removeQty > 0) { target.inventory[id] = owned - removeQty; actuallyRemoved[id] = removeQty; }
  }
  req.status = 'fulfilled';
  req.actuallyRemoved = actuallyRemoved;
  const withdrawValue = Object.entries(actuallyRemoved).reduce((sum, [id, q]) => sum + (petCatalog[id] ? petCatalog[id].value : 0) * q, 0);
  persistUser(req.username, { type: 'withdraw_fulfilled', amount: 0, itemsTouched: Object.keys(actuallyRemoved), reason: `Fulfilled by ${ADMIN_USERNAME}`, adminUsername: ADMIN_USERNAME, value: -withdrawValue });
  syncAccount(req.username);
  send(usernameToSocket.get(req.username), { type: 'chat:message', username: 'System', text: `Your withdrawal request was fulfilled by ${ADMIN_USERNAME}.`, timestamp: Date.now(), system: true });
  const removedNames = Object.keys(actuallyRemoved)
    .map((id) => (petCatalog[id] ? petCatalog[id].name : id))
    .join(', ');
  createNotification({
    username: req.username,
    type: 'withdraw_fulfilled',
    title: '✅ Withdrawal fulfilled',
    message: removedNames ? `Your withdrawal was fulfilled: ${removedNames}.` : 'Your withdrawal request was fulfilled.',
    refId: req.id,
  });
  broadcastPendingWithdrawalsToAdmin();
}

async function handleAdminWithdrawReject(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req || req.resolving) return;
  req.resolving = true;
  let ok;
  try {
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'rejected', null);
  } catch (err) {
    req.resolving = false;
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return;
  }
  if (!ok) { req.resolving = false; return; } // already resolved
  req.status = 'rejected';
  persistUser(req.username, { type: 'withdraw_rejected', amount: 0, reason: `Rejected by ${ADMIN_USERNAME}`, adminUsername: ADMIN_USERNAME });
  syncAccount(req.username); // unlocks the items back into their available inventory
  send(usernameToSocket.get(req.username), { type: 'error', message: 'Your withdrawal request was declined.' });
  createNotification({
    username: req.username,
    type: 'withdraw_rejected',
    title: '❌ Withdrawal rejected',
    message: 'Your withdrawal request was rejected.',
    refId: req.id,
  });
  broadcastPendingWithdrawalsToAdmin();
}

async function handleWithdrawCancel(username, msg) {
  // Cancelling has been disabled - once submitted, a withdrawal request is
  // locked in for admin review one way or the other (fulfilled/rejected),
  // rather than something a player can pull back at will. The client no
  // longer offers a Cancel button, but this stays as a hard no-op (instead
  // of just removing the case entirely) so a direct/tampered
  // withdraw:cancel message can't do it either.
  const errTo = usernameToSocket.get(username);
  if (errTo) send(errTo, { type: 'error', message: "Withdrawal requests can't be cancelled once submitted." });
}


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

// ---------------------------------------------------------------------------
// EMERGENCY LOCKDOWN — an instant, admin-toggleable kill switch (see the
// /lockdown chat command in handleChatSend) for "something's badly wrong,
// freeze everything right now" situations, without needing to redeploy or
// even restart the server. While active:
//   - Non-admin logins are rejected outright (see the login handler above).
//   - Every message type in LOCKDOWN_BLOCKED_TYPES is rejected for anyone
//     already connected (defense in depth, in case someone was mid-session
//     when lockdown was triggered).
// Purely in-memory - resets to off on restart, same as everything else
// here. Chat, the leaderboard, and viewing a profile still work during
// lockdown so people aren't left completely in the dark about what's
// happening; every action that actually touches coins/items/escrow does not.
let siteLockdown = false;
const LOCKDOWN_BLOCKED_TYPES = new Set([
  'rain:deposit', 'rain:claim',
  'coinflip:create', 'coinflip:join', 'coinflip:cancel',
  'diceduel:create', 'diceduel:join', 'diceduel:cancel',
  'jackpot:enter',
  'casebattle:create', 'casebattle:join', 'casebattle:start', 'casebattle:callBot', 'casebattle:cancel',
  'exchange:sell', 'exchange:buy',
  'account:reset',
  'game:wager', 'game:resolve',
  'case:open',
  'mines:start', 'mines:reveal', 'mines:cashout',
  'withdraw:request', 'withdraw:cancel',
  'tip:send',
  'sink:join', 'sink:cashout',
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
      timestamp: now, wagered: 0, system: true,
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
      pushChatMessage({ username: 'System', text: `${targetName} was muted for ${label}.`, timestamp: now, wagered: 0, system: true });
    } else {
      const targetName = resolveExistingUsername(unmuteMatch[1]);
      if (!targetName) return send(usernameToSocket.get(username), { type: 'error', message: 'No user with that name.' });
      const targetUser = ensureUser(targetName);
      targetUser.mutedUntil = 0;
      send(usernameToSocket.get(username), { type: 'toast', message: `Unmuted ${targetName}.` });
      const targetWs = usernameToSocket.get(targetName);
      if (targetWs) send(targetWs, { type: 'toast', message: `You've been unmuted.` });
      pushChatMessage({ username: 'System', text: `${targetName} was unmuted.`, timestamp: now, wagered: 0, system: true });
    }
    return;
  }

  const u = ensureUser(username);
  if (u.mutedUntil && u.mutedUntil > now) {
    const remainingLabel = formatMuteDuration(u.mutedUntil - now);
    return send(usernameToSocket.get(username), { type: 'error', message: `You're muted for another ${remainingLabel}.` });
  }

  const wagered = u.stats.wagered || 0;
  pushChatMessage({ username, text, timestamp: now, wagered, specialRole: u.specialRole || null });
}

// ---------------------------------------------------------------------------
// Public profile lookup - clicking any avatar site-wide requests this for
// the clicked user. Only ever returns PUBLIC game stats (wagered/won/lost)
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
  const u = users.get(targetName) || { stats: { wagered: 0, won: 0, lost: 0 }, specialRole: null };
  send(usernameToSocket.get(username), {
    type: 'profile:data',
    username: targetName,
    stats: { wagered: u.stats.wagered || 0, won: u.stats.won || 0, lost: u.stats.lost || 0 },
    isAdmin: targetName === ADMIN_USERNAME,
    specialRole: u.specialRole || null,
  });
}

const GRANTABLE_ROLES = new Set(['vip', 'moderator']);
// Manually-assigned roles (VIP / Moderator) - separate from the automatic
// wagered-based milestone roles, which are never stored anywhere since
// they're always derived live from stats.wagered (so they can never be
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
  send(usernameToSocket.get(username), { type: 'profile:data', username: targetName, stats: { wagered: u.stats.wagered || 0, won: u.stats.won || 0, lost: u.stats.lost || 0 }, isAdmin: targetName === ADMIN_USERNAME, specialRole: u.specialRole || null });
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
// Account history (Game History + Transaction History). Always the requesting
// player's own rows - the username comes from the logged-in connection, never
// from the message.
// ---------------------------------------------------------------------------
const HISTORY_PAGE_SIZE = 10;
const HISTORY_GAMES = new Set(['battles', 'cases', 'coinflip', 'diceduel', 'mines', 'jackpot', 'sink']);
const TXN_CATEGORIES = {
  deposit: ['tip_receive'],          // split into Deposit / Tip Received below
  tip: ['tip_send', 'tip_receive'],
  withdraw: ['withdraw_fulfilled', 'withdraw_rejected'],
  exchange: ['exchange_buy', 'exchange_sell'],
  rain: ['rain_deposit', 'rain_payout'],
  other: ['admin_adjust', 'escrow_refund'],
};
// Every non-game type that is allowed to show up at all.
const TXN_VISIBLE = [...new Set(Object.values(TXN_CATEGORIES).flat())];
let historyLastAt = new Map();

async function handleHistoryRequest(username, msg) {
  const ws = usernameToSocket.get(username);
  // Light flood guard - each request is a DB query.
  const now = Date.now();
  if (now - (historyLastAt.get(username) || 0) < 250) return;
  historyLastAt.set(username, now);
  const kind = msg.kind === 'transactions' ? 'transactions' : 'games';
  const page = Math.min(100000, Math.max(1, Math.floor(Number(msg.page) || 1)));
  const offset = (page - 1) * HISTORY_PAGE_SIZE;
  try {
    if (kind === 'games') {
      const filter = HISTORY_GAMES.has(msg.filter) ? msg.filter : null;
      const { total, rows } = await db.loadGameHistory(username, filter, HISTORY_PAGE_SIZE, offset);
      return send(ws, { type: 'history:result', kind, filter: filter || 'all', page, pageSize: HISTORY_PAGE_SIZE, total, rows });
    }
    const filter = Object.prototype.hasOwnProperty.call(TXN_CATEGORIES, msg.filter) ? msg.filter : null;
    let types = filter ? TXN_CATEGORIES[filter] : TXN_VISIBLE;
    let result;
    if (filter === 'deposit' || filter === 'tip') {
      // Deposits are pets/coins sent from the stock account; anything else
      // received is a normal tip (told apart by the reason text).
      result = await db.loadTransactionHistory(username, types, null, 1000, 0);
      const fromStock = (r) => r.type === 'tip_receive' && String(r.reason || '').toLowerCase() === `from ${STOCK_USERNAME.toLowerCase()}`;
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
      const fromStock = r.type === 'tip_receive' && String(r.reason || '').toLowerCase() === `from ${STOCK_USERNAME.toLowerCase()}`;
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

  const tipValue = coins + stakeValue(items);
  persistUser(username, { type: 'tip_send', amount: -coins, balanceBefore: senderBefore, balanceAfter: u.coins, itemsTouched: senderItemsTouched, reason: `To ${toUsername}`, value: -tipValue });
  persistUser(toUsername, { type: 'tip_receive', amount: coins, balanceBefore: targetBefore, balanceAfter: target.coins, itemsTouched: [...new Set(items)], reason: `From ${username}`, value: tipValue });
  syncAccount(username);
  syncAccount(toUsername);

  const parts = [];
  if (coins > 0) parts.push(`${coins.toLocaleString()} coins`);
  if (items.length) parts.push(`${items.length} item${items.length > 1 ? 's' : ''}`);
  pushChatMessage({ username: 'System', text: `${username} tipped ${toUsername} ${parts.join(' + ')}`, timestamp: Date.now(), system: true });
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Roblox lookups - resolves username/avatar/bio-verification server-side.
// This exists because browsers can't call Roblox's API directly (no CORS
// headers on Roblox's end), and public CORS-relay services turned out to be
// unreliable in practice (ad blockers and privacy tools commonly block
// exactly this kind of proxy domain). A server has no CORS restrictions at
// all, so this is both simpler and far more reliable than relay-hopping.
// ---------------------------------------------------------------------------
// Small retry wrapper for outbound Roblox API calls. Unlike the browser,
// the server has no CORS restriction talking to Roblox directly - so a
// failed call here is a genuine transient network blip, not a
// browser-security limitation, and the right fix is just to retry a couple
// of times before giving up, not to hand verification off to the client.
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

function serveImageAsset(pathname, res) {
  const filename = decodeURIComponent(pathname.slice('/assets/img/'.length));
  if (!SAFE_ASSET_FILENAME.test(filename)) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Bad asset request.');
    return;
  }
  fs.readFile(path.join(ASSET_IMG_DIR, filename), (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found.');
      return;
    }
    res.writeHead(200, {
      'Content-Type': filename.endsWith('.webp') ? 'image/webp' : 'image/png',
      // Safe to cache forever, unconditionally - see the comment above on
      // why a given filename's content can never change.
      'Cache-Control': 'public, max-age=31536000, immutable',
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
      const myPendingWithdrawals = withdrawRequests
        .filter((r) => r.username === username && r.status === 'pending')
        .map((r) => ({ id: r.id, items: r.items, requestedAt: r.requestedAt }));
      send(ws, {
        type: 'login:ok',
        user: {
          username, coins: u.coins, inventory: u.inventory, stats: u.stats,
          specialRole: u.specialRole || null, mutedUntil: u.mutedUntil || 0,
          lockedItems: lockedItemsForUser(username),
          pendingWithdrawals: myPendingWithdrawals,
          discord: discordPublic(u.discord),
          ignored: ignoredListFor(username),
        },
      });
      send(ws, { type: 'exchange:stock', stock: shopStock });
      db.loadCoinflipHistoryForUser(username, 10).then((history) => {
        send(ws, { type: 'coinflip:history', history });
      }).catch((err) => console.error(`[db] Failed to load coinflip history for ${username}:`, err.message));
      send(ws, { type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
      send(ws, { type: 'diceduel:list', duels: diceDuelPublicList() });
      send(ws, { type: 'jackpot:state', ...jackpotPublicState() });
      send(ws, { type: 'sink:state', ...sinkPublicState(sinkRound) });
      db.loadNotificationsForUser(username, NOTIFICATION_LOAD_LIMIT)
        .then((notifications) => send(ws, { type: 'notifications:list', notifications }))
        .catch((err) => console.error(`[notifications] Failed to load for ${username}:`, err.message));
      send(ws, { type: 'rain:state', ...rainPublicState() });
      send(ws, { type: 'feed:recent', entries: liveFeed });
      send(ws, { type: 'chat:history', messages: chatHistory });
      const myMinesRound = activeMinesRounds.get(username);
      if (myMinesRound) {
        // Rebuilds the board on the client exactly where it left off -
        // the round itself already survives a refresh fine (it's keyed by
        // username in server memory, not tied to the socket), but without
        // this the client has no way to know it exists, tries to start a
        // fresh one, and just gets stuck on "you already have an active
        // round" with nothing to actually click. Bomb positions are never
        // sent, same as at every other point in a live round - only which
        // tiles are already safely revealed.
        const resumeMultiplier = minesMultiplier(myMinesRound.n, myMinesRound.mCount, myMinesRound.picks);
        send(ws, {
          type: 'mines:resume',
          n: myMinesRound.n, mCount: myMinesRound.mCount, gridSize: myMinesRound.gridSize,
          amount: myMinesRound.amount, picks: myMinesRound.picks,
          revealed: [...myMinesRound.revealed],
          multiplier: resumeMultiplier,
          payout: Math.floor(myMinesRound.amount * resumeMultiplier),
        });
      }
      broadcastBattlesTo(ws);
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
    // whole process (dropping every in-progress game). Now it's contained to
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
      case 'coinflip:create': return handleCoinflipCreate(username, msg);
      case 'coinflip:join': return handleCoinflipJoin(username, msg);
      case 'coinflip:cancel': return handleCoinflipCancel(username, msg);
      case 'diceduel:create': return handleDiceDuelCreate(username, msg);
      case 'diceduel:join': return handleDiceDuelJoin(username, msg);
      case 'diceduel:cancel': return handleDiceDuelCancel(username, msg);
      case 'jackpot:enter': return handleJackpotEnter(username, msg);
      case 'casebattle:create': return handleCaseBattleCreate(username, msg);
      case 'casebattle:join': return handleCaseBattleJoin(username, msg);
      case 'casebattle:start': return handleCaseBattleStart(username, msg);
      case 'casebattle:callBot': return handleCaseBattleCallBot(username, msg);
      case 'casebattle:cancel': return handleCaseBattleCancel(username, msg);
      case 'exchange:sell': return handleExchangeSell(username, msg);
      case 'exchange:buy': return handleExchangeBuy(username, msg);
      case 'account:reset': return handleAccountReset(username);
      case 'admin:lookup': return handleAdminLookup(username, msg);
      case 'admin:addCoins': return handleAdminAdjustCoins(username, msg, 1);
      case 'admin:removeCoins': return handleAdminAdjustCoins(username, msg, -1);
      // SECURITY: game:wager / game:resolve are intentionally NOT routed.
      // Mines is fully server-authoritative via mines:start/reveal/cashout,
      // and these legacy endpoints trusted client-reported payouts/wagers
      // (free-coin exploit). Do not re-add them.
      case 'case:open': return handleCaseOpen(username, msg);
      case 'mines:start': return handleMinesStart(username, msg);
      case 'mines:reveal': return handleMinesReveal(username, msg);
      case 'mines:cashout': return handleMinesCashout(username);
      case 'sink:join': return handleSinkJoin(username, msg);
      case 'sink:cashout': return handleSinkCashout(username);
      // The client requests this whenever its tab becomes visible again
      // after being backgrounded - without an explicit resync, a client
      // that missed a broadcast while backgrounded had no way to recover
      // other than waiting for the next unrelated state change, which
      // could leave it computing the multiplier from a very stale
      // activeStartAt in the meantime. This just re-sends the exact same
      // payload login already sends, so it's always safe to call.
      case 'sink:resync': return send(ws, { type: 'sink:state', ...sinkPublicState(sinkRound) });
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
      case 'leaderboard:request': return handleLeaderboardRequest(username);
      case 'withdraw:request': return handleWithdrawRequest(username, msg);
      case 'withdraw:cancel': return handleWithdrawCancel(username, msg);
      case 'admin:withdrawList': return handleAdminWithdrawList(username);
      case 'admin:withdrawFulfill': return handleAdminWithdrawFulfill(username, msg);
      case 'admin:withdrawReject': return handleAdminWithdrawReject(username, msg);
      case 'chat:send': return handleChatSend(username, msg, ws);
      case 'tip:send': return handleTipSend(username, msg);
      case 'profile:request': return handleProfileRequest(username, msg);
      case 'admin:setRole': return handleAdminSetRole(username, msg);
      case 'discord:linkStart': return handleDiscordLinkStart(username);
      case 'discord:unlink': return handleDiscordUnlink(username);
      case 'ignore:add': return handleIgnoreAdd(username, msg);
      case 'ignore:remove': return handleIgnoreRemove(username, msg);
      case 'history:request': return handleHistoryRequest(username, msg);
    }
  }

  ws.on('close', () => {
    const username = socketToUsername.get(ws);
    socketToUsername.delete(ws);
    if (username && usernameToSocket.get(username) === ws) usernameToSocket.delete(username);
    // A duel that's still waiting for an opponent has only the creator's
    // items locked up, and only the creator was ever able to cancel it - if
    // they leave first, nothing else can free that escrow, so it's released
    // here instead of sitting locked forever. Once a 2nd player has joined
    // (status 'running'), rolls keep happening on the server's own timers
    // regardless of either connection - same as Case Battles already do -
    // so there's nothing to clean up there; the duel resolves and pays out
    // on schedule either way.
    if (username) {
      // Collected first rather than cancelled while iterating - nothing
      // stops someone from having more than one open duel at once (same as
      // Coinflip lobbies), and mutating the Map mid-iteration via delete()
      // inside cancelDiceDuelInternal isn't safe to do directly.
      const toCancel = [...diceDuels.values()].filter((d) => d.status === 'waiting' && d.creator === username);
      toCancel.forEach(cancelDiceDuelInternal);
    }
  });
});

function broadcastBattlesTo(ws) {
  send(ws, {
    type: 'casebattle:list',
    battles: [...battles.values()].map((b) => ({
      id: b.id, creator: b.creator, status: b.status, mode: b.mode, winMode: b.winMode, borrow: b.borrow || 0,
      caseQueue: b.caseQueue, caseNames: b.caseNames,
      players: b.players.map((p) => ({ username: p.username, isBot: p.isBot, team: p.team, total: p.total || 0 })),
      winner: b.winner, winnerValue: b.winnerValue,
    })),
  });
}

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

    // Refund anything a game was holding when the server last stopped
    // (crash, deploy, restart). Each refund + escrow release is one DB
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
    if (leftoverEscrows.length) console.log(`[boot] Refunded ${leftoverEscrows.length} interrupted game stake(s).`);

    Object.assign(shopStock, await db.loadShopStock());
    console.log(`[boot] Loaded exchange stock (${Object.keys(shopStock).length} pet type(s)).`);
    try {
      const savedSink = await db.loadServerState('sinkHistory');
      if (savedSink && Array.isArray(savedSink.points)) {
        sinkHistory = savedSink.points.map(Number).filter((n) => Number.isFinite(n) && n >= 1).slice(0, SINK_HISTORY_SIZE);
      }
    } catch (err) { console.error('[boot] Could not load sink history:', err.message); }
    const savedRain = await db.loadServerState('rainPool');
    if (savedRain && Number.isFinite(Number(savedRain.pool))) rainPool = Math.max(RAIN_BASE, Math.floor(Number(savedRain.pool)));

    ignoreLists = await db.loadAllIgnores();

    const loadedRequests = await db.loadPendingWithdrawRequests();
    withdrawRequests = loadedRequests;
    console.log(`[boot] Loaded ${loadedRequests.length} pending withdrawal request(s) from Postgres.`);

    const loadedCfLobbies = await db.loadPendingCoinflipLobbies();
    cfLobbies = loadedCfLobbies;
    console.log(`[boot] Loaded ${loadedCfLobbies.length} pending coinflip lobby(ies) from Postgres.`);

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
    startSinkJoiningPhase();
    server.listen(PORT, () => console.log(`BloxyVault server listening on :${PORT}`));
  } catch (err) {
    console.error('[boot] Failed to start - could not initialize/load from Postgres:', err);
    process.exit(1);
  }
})();
