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
const db = require('./db');

const gameData = JSON.parse(fs.readFileSync(path.join(__dirname, 'gameData.json'), 'utf8'));
const { petCatalog, caseData, caseItems, modeConfigs, botNamesCf, botNamesBattles } = gameData;

const STARTING_BALANCE = 1000000; // bumped from 5000 - the cheapest case alone costs 375k, old value made the game unplayable for new accounts
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

// Gates every testing-only, no-real-verification path in this file:
// /dev/skip-login (logs in as anyone with zero Roblox check), the
// debug:addTestCoins cheat, and account:reset. All three exist purely for
// local development and are OFF by default - Railway doesn't reliably set
// NODE_ENV on its own, so this deliberately requires an explicit opt-in
// rather than trusting an environment flag that might not be set. Only add
// ENABLE_DEV_BYPASS=true in Railway's Variables tab if you actually want
// these reachable (e.g. a separate staging deploy) - never on the real one.
const ENABLE_DEV_BYPASS = process.env.ENABLE_DEV_BYPASS === 'true';
if (ENABLE_DEV_BYPASS) {
  console.warn('[security] ENABLE_DEV_BYPASS is ON - /dev/skip-login, debug:addTestCoins, and account:reset are reachable. Do not leave this on in production.');
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

function ensureUser(username) {
  if (!users.has(username)) {
    users.set(username, { coins: STARTING_BALANCE, inventory: {}, stats: { wagered: 0, won: 0, lost: 0 } });
    // Fire-and-forget: this username has never been seen before in this
    // process. It's already usable in memory immediately (no need to wait
    // on the DB round-trip before the player can act) - Postgres just needs
    // to catch up with a matching row.
    db.insertNewUser(username, STARTING_BALANCE, { wagered: 0, won: 0, lost: 0 }).catch((err) => {
      console.error(`[db] insertNewUser failed for ${username}:`, err.message);
    });
  }
  const u = users.get(username);
  if (!u.stats) u.stats = { wagered: 0, won: 0, lost: 0 }; // backfill for accounts created before stats existed
  if (username === GIVEAWAY_USERNAME) {
    // Keep this hand-out account topped up every time it's touched, not
    // just on first creation - covers accounts that already existed
    // in memory before this feature was added, and any pets added to
    // the catalog later (they'll get topped up to 99x automatically too).
    // Only the items that actually needed correcting get persisted - once
    // stable at 99x, later calls change nothing and skip the DB write
    // entirely, so this never turns into hundreds of writes per message.
    const toppedUp = [];
    if (u.coins < GIVEAWAY_STARTING_BALANCE) u.coins = GIVEAWAY_STARTING_BALANCE;
    for (const id of Object.keys(petCatalog)) {
      if (!u.inventory[id] || u.inventory[id] < 99) { u.inventory[id] = 99; toppedUp.push(id); }
    }
    if (toppedUp.length) {
      persistUser(username, { type: 'giveaway_topup', amount: 0, balanceBefore: u.coins, balanceAfter: u.coins, itemsTouched: toppedUp, reason: 'Self-healing top-up to 99x' });
    }
  }
  return u;
}
function trackWager(username, amount) { if (amount > 0) ensureUser(username).stats.wagered += amount; }
// A cancelled coinflip lobby never actually played out, so it shouldn't
// keep counting toward "Played" / Most Played - this undoes the trackWager
// that ran when the lobby was first created.
function untrackWager(username, amount) { if (amount > 0) { const u = ensureUser(username); u.stats.wagered = Math.max(0, u.stats.wagered - amount); } }
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
  const need = {};
  for (const id of items) need[id] = (need[id] || 0) + 1;
  return Object.entries(need).every(([id, qty]) => (inv[id] || 0) >= qty);
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

function syncAccount(username) {
  const ws = usernameToSocket.get(username);
  if (!ws) return; // not currently connected - they'll get fresh state on next login
  const u = ensureUser(username);
  send(ws, { type: 'account', user: { username, coins: u.coins, inventory: u.inventory, stats: u.stats } });
}

// Fire-and-forget persistence: memory has already been updated synchronously
// (same as before this feature existed) by the time this is called, so this
// just mirrors that already-decided state into Postgres afterward. Errors
// are logged, not thrown - a slow/hiccupping DB write should never crash a
// live game round or block the response the player already got.
//
// `txn` (optional) - see db.js's persistUserState doc comment for shape.
// Pass it whenever coins or stats changed, so a real audit row gets written.
function persistUser(username, txn) {
  const u = users.get(username);
  if (!u) return;
  db.persistUserState(username, u, txn).catch((err) => {
    console.error(`[db] persistUser failed for ${username} (${txn ? txn.type : 'no-txn'}):`, err.message);
  });
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
const botNameSet = new Set([...botNamesCf, ...botNamesBattles]); // never treated as real accounts

function broadcastCfLobbies() {
  broadcast({ type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
}

// Picks a stake of real catalog items landing near targetValue (or fully
// random if no target), same approach the old local practice-mode used.
//
// Bots have no inventory limits, so unlike a real player's owned-quantity
// cap, they can stack as many copies of the same pet as needed. That's
// required for big targets (300m+) that no single-of-each combination can
// reach - without allowing repeats here, the search below would always
// fail for those and silently fall back to one mismatched item, which is
// what made it look like bots just weren't joining high-value flips.
function pickBotStake(targetValue) {
  const pool = Object.entries(petCatalog).filter(([, p]) => p.value > 0);
  if (!targetValue) {
    const count = 1 + Math.floor(Math.random() * 2);
    const picked = [];
    for (let i = 0; i < count; i++) picked.push(pool[Math.floor(Math.random() * pool.length)][0]);
    return picked;
  }
  const lo = targetValue * 0.9, hi = targetValue * 1.1;
  for (let attempt = 0; attempt < 200; attempt++) {
    const shuffled = pool.slice().sort(() => Math.random() - 0.5);
    let sum = 0, combo = [], guard = 0;
    while (sum < lo && guard < 500) {
      guard++;
      const fits = shuffled.filter(([, p]) => sum + p.value <= hi);
      if (!fits.length) break;
      const [id, p] = fits[Math.floor(Math.random() * fits.length)];
      combo.push(id); sum += p.value;
    }
    if (sum >= lo && sum <= hi) return combo;
  }
  // Fallback: stack copies of the single closest-value pet to land as
  // near the target as possible, instead of handing back one item that
  // could be wildly under the requested stake.
  const closestId = pool.reduce((best, [id, p]) => Math.abs(p.value - targetValue) < Math.abs(petCatalog[best].value - targetValue) ? id : best, pool[0][0]);
  const closestValue = petCatalog[closestId].value;
  if (closestValue > 0) {
    const count = Math.max(1, Math.min(50, Math.round(targetValue / closestValue)));
    return Array(count).fill(closestId);
  }
  return [closestId];
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

  // Bots aren't real accounts, so skip touching `users` (including stats) for them entirely.
  const wonAmount = stakeValue(loserItems);
  if (!botNameSet.has(winner)) {
    addItems(winner, winnerFinalItems);
    trackWin(winner, wonAmount);
    persistUser(winner, { type: 'coinflip_win', amount: 0, itemsTouched: [...new Set(winnerFinalItems)] });
  }
  if (!botNameSet.has(loser)) {
    trackLoss(loser, wonAmount);
    persistUser(loser, { type: 'coinflip_loss', amount: 0 });
  }
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
  const items = msg.items || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  removeItems(username, items); // escrow
  trackWager(username, stakeValue(items));
  persistUser(username, { type: 'coinflip_create_escrow', amount: 0, itemsTouched: [...new Set(items)] });
  const id = 'cf_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  cfLobbies.push({ id, creator: username, side: msg.side, items });
  syncAccount(username);
  broadcastCfLobbies();

  // If no real player challenges this lobby in a few seconds, a bot will.
  const stakeVal = stakeValue(items);
  setTimeout(() => {
    const lobby = cfLobbies.find((l) => l.id === id);
    if (!lobby) return; // already resolved or cancelled
    const botName = botNamesCf[Math.floor(Math.random() * botNamesCf.length)];
    const botItems = pickBotStake(stakeVal);
    const botSide = lobby.side === 'H' ? 'T' : 'H';
    cfLobbies = cfLobbies.filter((l) => l.id !== id);
    resolveCoinflip(lobby.creator, lobby.side, lobby.items, botName, botSide, botItems);
  }, 4000 + Math.random() * 4000);
}

function handleCoinflipJoin(username, msg) {
  const lobby = cfLobbies.find((l) => l.id === msg.lobbyId);
  if (!lobby) return send(usernameToSocket.get(username), { type: 'error', message: 'That lobby is no longer available.' });
  const items = msg.items || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  const lobbyValue = stakeValue(lobby.items);
  const joinValue = stakeValue(items);
  const lo = lobbyValue * (1 - CF_MATCH_TOLERANCE), hi = lobbyValue * (1 + CF_MATCH_TOLERANCE);
  if (joinValue < lo || joinValue > hi) {
    return send(usernameToSocket.get(username), { type: 'error', message: 'Your stake is outside the allowed ±10% range.' });
  }
  removeItems(username, items); // escrow
  trackWager(username, joinValue);
  persistUser(username, { type: 'coinflip_join_escrow', amount: 0, itemsTouched: [...new Set(items)] });
  cfLobbies = cfLobbies.filter((l) => l.id !== msg.lobbyId);
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
  syncAccount(username);
  broadcastCfLobbies();
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
  const items = Array.isArray(msg.items) ? msg.items.filter((id) => typeof id === 'string') : [];
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
  persistUser(username, { type: 'jackpot_enter', amount: -amount, itemsTouched: [...new Set(items)] });
  syncAccount(username);

  if (!jackpotRound) {
    jackpotRound = { entrants: [], endsAt: Date.now() + JACKPOT_ROUND_MS };
    jackpotRound.timer = setTimeout(resolveJackpot, JACKPOT_ROUND_MS);
  }
  jackpotRound.entrants.push({ username, value, items, coinsStaked: amount });
  broadcastJackpot();
}

function resolveJackpot() {
  const round = jackpotRound;
  jackpotRound = null;
  if (!round || round.entrants.length === 0) { broadcastJackpot(); return; }

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
  persistUser(winner.username, { type: 'jackpot_win', amount: coinsPortion, balanceBefore: before, balanceAfter: u.coins, itemsTouched: [...new Set(allItems)] });
  for (const e of round.entrants) {
    if (e.username !== winner.username) {
      trackLoss(e.username, e.value);
      persistUser(e.username, { type: 'jackpot_loss', amount: 0 });
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

function battleCost(caseQueue) {
  return caseQueue.reduce((sum, idx) => sum + caseData[idx].price, 0);
}
function broadcastBattles() {
  broadcast({
    type: 'casebattle:list',
    battles: [...battles.values()].map((b) => ({
      id: b.id, creator: b.creator, status: b.status, mode: b.mode,
      caseQueue: b.caseQueue, caseNames: b.caseNames,
      players: b.players.map((p) => ({ username: p.username, isBot: p.isBot, team: p.team, total: p.total || 0 })),
      winner: b.winner, winnerValue: b.winnerValue,
    })),
  });
}

function handleCaseBattleCreate(username, msg) {
  const cfg = modeConfigs[msg.mode] || modeConfigs['ffa-2'];
  const cost = battleCost(msg.caseQueue);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: "You don't have enough coins for this battle." });
  const before = u.coins;
  u.coins -= cost;
  trackWager(username, cost);
  persistUser(username, { type: 'casebattle_create_wager', amount: -cost, balanceBefore: before, balanceAfter: u.coins });
  syncAccount(username);

  const id = 'battle_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const caseNames = msg.caseQueue.map((i) => caseData[i].name);
  battles.set(id, {
    id, creator: username, status: 'waiting', mode: msg.mode,
    caseQueue: msg.caseQueue, caseNames, cfg,
    players: [{ username, isBot: false, team: 0, total: 0 }],
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
  const cost = battleCost(b.caseQueue);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: "You don't have enough coins for this battle." });
  const before = u.coins;
  u.coins -= cost;
  trackWager(username, cost);
  persistUser(username, { type: 'casebattle_join_wager', amount: -cost, balanceBefore: before, balanceAfter: u.coins });
  syncAccount(username);

  const team = b.cfg.isTeam ? b.players.length % 2 : 0;
  b.players.push({ username, isBot: false, team, total: 0 });
  broadcastBattles();

  if (b.players.length >= b.cfg.count) startBattle(b); // filled up early - start right away
}

function handleCaseBattleStart(username, msg) {
  const b = battles.get(msg.battleId);
  if (b && b.status === 'waiting') startBattle(b);
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
  const team = b.cfg.isTeam ? b.players.length % 2 : 0;
  b.players.push({ username: bots[0], isBot: true, team, total: 0 });
  broadcastBattles();
  if (b.players.length >= b.cfg.count) startBattle(b);
}

// Lets the creator back out of their own battle before anyone else has
// staked in - refunds their case cost and removes the battle. Once a real
// player joins, cancelling is blocked since their coins are already at
// stake (bots never pay in, so their presence alone doesn't block this).
function handleCaseBattleCancel(username, msg) {
  const b = battles.get(msg.battleId);
  if (!b || b.creator !== username || b.status !== 'waiting') return;
  const otherReal = b.players.some((p) => p.username !== username && !p.isBot);
  if (otherReal) return;
  const cost = battleCost(b.caseQueue);
  const u = ensureUser(username);
  const before = u.coins;
  u.coins += cost;
  untrackWager(username, cost);
  persistUser(username, { type: 'casebattle_cancel_refund', amount: cost, balanceBefore: before, balanceAfter: u.coins });
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
      const team = b.cfg.isTeam ? b.players.length % 2 : 0;
      b.players.push({ username: name, isBot: true, team, total: 0 });
    });
  }
  broadcastBattles();
  runBattle(b);
}

async function runBattle(b) {
  for (let roundIdx = 0; roundIdx < b.caseQueue.length; roundIdx++) {
    const pool = getPool(b.caseQueue[roundIdx]);
    const roundPlayers = b.players.map((p) => {
      const pulled = weightedPick(pool);
      p.total = (p.total || 0) + pulled.value;
      return { username: p.username, pulled, total: p.total };
    });
    broadcast({ type: 'casebattle:round', battleId: b.id, roundIdx, players: roundPlayers });
    if (roundIdx < b.caseQueue.length - 1) await new Promise((r) => setTimeout(r, ROUND_PACE_MS));
  }

  let winner, winnerValue;
  const battleTotal = b.players.reduce((s, p) => s + p.total, 0); // winner takes ALL pulled value, not just their own side
  const perPlayerCost = battleCost(b.caseQueue);
  if (b.cfg.isTeam) {
    const teamTotals = {};
    b.players.forEach((p) => { teamTotals[p.team] = (teamTotals[p.team] || 0) + p.total; });
    const winningTeam = Object.entries(teamTotals).sort((a, c) => c[1] - a[1])[0][0];
    winner = `Team ${String.fromCharCode(65 + Number(winningTeam))}`;
    winnerValue = battleTotal; // full combined pot, matching how "Team A won X" is displayed as one number
    const winners = b.players.filter((p) => String(p.team) === winningTeam && !p.isBot);
    const share = Math.floor(battleTotal / Math.max(1, b.players.filter((p) => String(p.team) === winningTeam).length));
    winners.forEach((p) => {
      const u = ensureUser(p.username);
      const before = u.coins;
      u.coins += share;
      trackWin(p.username, share);
      persistUser(p.username, { type: 'casebattle_win', amount: share, balanceBefore: before, balanceAfter: u.coins });
      syncAccount(p.username);
    });
    b.players.filter((p) => String(p.team) !== winningTeam && !p.isBot).forEach((p) => {
      trackLoss(p.username, perPlayerCost);
      persistUser(p.username, { type: 'casebattle_loss', amount: 0 });
    });
  } else {
    const top = [...b.players].sort((a, c) => c.total - a.total)[0];
    winner = top.username;
    winnerValue = battleTotal; // the winner takes everyone's pulls, so the banner should reflect that, not just their own
    if (!top.isBot) {
      const u = ensureUser(top.username);
      const before = u.coins;
      u.coins += battleTotal;
      trackWin(top.username, battleTotal);
      persistUser(top.username, { type: 'casebattle_win', amount: battleTotal, balanceBefore: before, balanceAfter: u.coins });
      syncAccount(top.username);
    }
    b.players.filter((p) => p !== top && !p.isBot).forEach((p) => {
      trackLoss(p.username, perPlayerCost);
      persistUser(p.username, { type: 'casebattle_loss', amount: 0 });
    });
  }

  b.status = 'finished';
  b.winner = winner;
  b.winnerValue = winnerValue;
  broadcastBattles();
  broadcast({
    type: 'casebattle:finished', battleId: b.id, winner, winnerValue,
    players: b.players.map((p) => ({ username: p.username, total: p.total })),
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
  const requested = {};
  for (const id of (msg.items || [])) requested[id] = (requested[id] || 0) + 1;
  const u = ensureUser(username);
  let rawTotal = 0;
  let stockChanged = false;
  const itemsTouched = [];
  for (const [id, reqQty] of Object.entries(requested)) {
    const owned = u.inventory[id] || 0;
    const sellQty = Math.min(reqQty, owned);
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
  if (stockChanged) broadcast({ type: 'exchange:stock', stock: shopStock });
}

function handleExchangeBuy(username, msg) {
  const ids = msg.items || [];
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

// Same "give yourself money" cheat as above, additive instead of a reset.
// Only ever affects the sender's own account - the amount is fixed
// server-side (not read from the client) so it can't be abused into an
// arbitrary-amount cheat via a hand-crafted message.
function handleAddTestCoins(username) {
  if (!ENABLE_DEV_BYPASS) return send(usernameToSocket.get(username), { type: 'error', message: 'That feature is disabled.' });
  const u = ensureUser(username);
  const before = u.coins;
  u.coins += 1000000000;
  persistUser(username, { type: 'debug_add_coins', amount: u.coins - before, balanceBefore: before, balanceAfter: u.coins, reason: 'Dev bypass: add test coins' });
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
function handleLeaderboardRequest(username) {
  const rows = [...users.entries()].map(([uname, u]) => ({
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

  send(usernameToSocket.get(username), {
    type: 'leaderboard:data',
    mostPlayed: topBy('wagered'),
    mostLost: topBy('lost'),
    mostWon: topBy('won'),
  });
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
  const before = u.coins;
  if (amount > 0) u.coins -= amount;
  for (const id of items) {
    u.inventory[id] = Math.max(0, (u.inventory[id] || 0) - 1); // clamped - never goes negative even if client/server briefly disagree
  }
  trackWager(username, amount > 0 ? amount : stakeValue(items));
  persistUser(username, { type: `${msg.game || 'game'}_wager`, amount: -amount, balanceBefore: before, balanceAfter: u.coins, itemsTouched: items });
  syncAccount(username);
}

const MINES_MAX_BET = 500000; // must match the client's MINES_MAX_BET

function handleGameResolve(username, msg) {
  const payout = Math.max(0, Math.floor(Number(msg.payout) || 0));
  const wager = Math.max(0, Math.floor(Number(msg.wager) || 0));
  const items = Array.isArray(msg.items) ? msg.items.filter(id => typeof id === 'string') : [];
  // Belt-and-suspenders: the client already stops you from starting a Mines
  // round above this, but don't trust that alone - reject anything that
  // claims a bigger wager was involved.
  if (msg.game === 'mines' && wager > MINES_MAX_BET) {
    return send(usernameToSocket.get(username), { type: 'error', message: `Mines is capped at ${MINES_MAX_BET} coins per round.` });
  }
  const u = ensureUser(username);
  const before = u.coins;
  if (payout > 0) {
    u.coins += payout;
    trackWin(username, payout);
  } else if (wager > 0) {
    trackLoss(username, wager);
  }
  for (const id of items) {
    u.inventory[id] = (u.inventory[id] || 0) + 1;
  }
  persistUser(username, { type: `${msg.game || 'game'}_resolve`, amount: payout, balanceBefore: before, balanceAfter: u.coins, itemsTouched: items });
  syncAccount(username);

  // Feed it to the site-wide live feed too, if it's a game the feed knows
  // how to describe (currently Mines and single Cases - Coinflip/Battles/
  // Jackpot log their own feed entries directly at the point they resolve).
  if (msg.game === 'mines') {
    if (payout > 0) {
      const multiplier = Number(msg.multiplier) || (wager > 0 ? payout / wager : 0);
      pushFeedEvent({ game: 'mines', username, amount: payout, multiplier, won: true });
    } else if (wager > 0) {
      pushFeedEvent({ game: 'mines', username, amount: wager, won: false });
    }
  } else if (msg.game === 'cases' && payout > 0) {
    const itemName = typeof msg.itemName === 'string' ? msg.itemName : null;
    pushFeedEvent({ game: 'cases', username, amount: payout, itemName });
  }
}

function handleAdminLookup(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const target = String(msg.username || '');
  const u = users.get(target);
  send(usernameToSocket.get(username), {
    type: 'admin:lookupResult',
    username: target,
    found: !!u,
    coins: u ? u.coins : null,
    inventory: u ? u.inventory : null,
  });
}

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

async function handleWithdrawRequest(username, msg) {
  const items = msg.items || [];
  const errTo = usernameToSocket.get(username);
  if (!items.length) return send(errTo, { type: 'error', message: 'Select at least one item to withdraw.' });
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

  // Remove exactly what's requested, capped by whatever they still actually
  // own right now (they may have sold/lost/traded something since asking) -
  // and report back precisely what was removed, so any mismatch is visible
  // rather than silently over- or under-removing.
  const target = ensureUser(req.username);
  const requested = {};
  for (const id of req.items) requested[id] = (requested[id] || 0) + 1;
  const actuallyRemoved = {};
  for (const [id, reqQty] of Object.entries(requested)) {
    const owned = target.inventory[id] || 0;
    const removeQty = Math.min(reqQty, owned);
    if (removeQty > 0) { target.inventory[id] = owned - removeQty; actuallyRemoved[id] = removeQty; }
  }

  // The DB row is the actual source of truth for "has this already been
  // actioned" - this only succeeds if it's still 'pending' there right now,
  // so two admin clicks (or a retry) can never both go through and remove
  // items twice, even if the in-memory array briefly disagreed.
  let ok;
  try {
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'fulfilled', actuallyRemoved);
  } catch (err) {
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return;
  }
  if (!ok) return; // someone/something else already resolved this request

  req.status = 'fulfilled';
  req.actuallyRemoved = actuallyRemoved;
  persistUser(req.username, { type: 'withdraw_fulfilled', amount: 0, itemsTouched: Object.keys(actuallyRemoved), reason: `Fulfilled by ${ADMIN_USERNAME}`, adminUsername: ADMIN_USERNAME });
  syncAccount(req.username);
  send(usernameToSocket.get(req.username), { type: 'chat:message', username: 'System', text: `Your withdrawal request was fulfilled by ${ADMIN_USERNAME}.`, timestamp: Date.now(), system: true });
  broadcastPendingWithdrawalsToAdmin();
}

async function handleAdminWithdrawReject(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req) return;
  let ok;
  try {
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'rejected', null);
  } catch (err) {
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return;
  }
  if (!ok) return; // already resolved
  req.status = 'rejected';
  send(usernameToSocket.get(req.username), { type: 'error', message: 'Your withdrawal request was declined.' });
  broadcastPendingWithdrawalsToAdmin();
}

async function handleWithdrawCancel(username, msg) {
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending' && r.username === username);
  if (!req) return;
  let ok;
  try {
    ok = await db.resolveWithdrawRequest(req.id, 'pending', 'cancelled', null);
  } catch (err) {
    console.error(`[db] Failed to resolve withdraw request ${req.id}:`, err.message);
    return;
  }
  if (!ok) return; // already resolved (e.g. admin acted on it at the same moment)
  req.status = 'cancelled';
  broadcastPendingWithdrawalsToAdmin();
}

// ---------------------------------------------------------------------------
// Chat - real, shared, no bots. Very light rate limiting (one message per
// 500ms per connection) just to stop an accidental flood from one client.
// ---------------------------------------------------------------------------
const lastChatAt = new Map(); // ws -> timestamp
function handleChatSend(username, msg, ws) {
  const now = Date.now();
  if (now - (lastChatAt.get(ws) || 0) < 500) return;
  lastChatAt.set(ws, now);
  const text = String(msg.text || '').slice(0, 300).trim();
  if (!text) return;
  const wagered = ensureUser(username).stats.wagered || 0;
  broadcast({ type: 'chat:message', username, text, timestamp: now, wagered });
}

// ---------------------------------------------------------------------------
// Tipping - coins and/or items, direct account-to-account transfer.
// ---------------------------------------------------------------------------
function handleTipSend(username, msg) {
  const toUsername = String(msg.toUsername || '').trim();
  const coins = Math.max(0, Math.floor(Number(msg.coins) || 0));
  const items = Array.isArray(msg.items) ? msg.items : [];
  const errTo = usernameToSocket.get(username);

  if (!toUsername || toUsername === username) return send(errTo, { type: 'error', message: 'Pick someone else to tip.' });
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
    if (username !== GIVEAWAY_USERNAME) { removeItems(username, items); senderItemsTouched = [...new Set(items)]; }
    addItems(toUsername, items);
  }

  persistUser(username, { type: 'tip_send', amount: -coins, balanceBefore: senderBefore, balanceAfter: u.coins, itemsTouched: senderItemsTouched, reason: `To ${toUsername}` });
  persistUser(toUsername, { type: 'tip_receive', amount: coins, balanceBefore: targetBefore, balanceAfter: target.coins, itemsTouched: [...new Set(items)], reason: `From ${username}` });
  syncAccount(username);
  syncAccount(toUsername);

  const parts = [];
  if (coins > 0) parts.push(`${coins.toLocaleString()} coins`);
  if (items.length) parts.push(`${items.length} item${items.length > 1 ? 's' : ''}`);
  broadcast({ type: 'chat:message', username: 'System', text: `${username} tipped ${toUsername} ${parts.join(' + ')}`, timestamp: Date.now(), system: true });
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
async function robloxResolveUsername(username){
  const r = await fetch(`https://users.roblox.com/v1/users/search?keyword=${encodeURIComponent(username)}&limit=10`);
  if(!r.ok) throw new Error('roblox search returned ' + r.status);
  const data = await r.json();
  return (data.data || []).find((u) => (u.name || '').toLowerCase() === username.toLowerCase()) || null;
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
    try{
      const match = await robloxResolveUsername(username);
      if(!match){ res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ imageUrl: null })); return; }
      const thumbR = await fetch(`https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds=${match.id}&size=150x150&format=Png&isCircular=false`);
      const thumbData = await thumbR.json();
      const entry = (thumbData.data || [])[0];
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ imageUrl: entry && entry.imageUrl ? entry.imageUrl : null, userId: match.id }));
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
      const profileR = await fetch(`https://users.roblox.com/v1/users/${match.id}`);
      const profileData = await profileR.json();
      const bio = profileData.description || '';
      if(!bio.includes(code)){ res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: "Didn't find that code in your bio yet. Make sure you saved it, then try again." })); return; }
      const token = issueSessionToken(username);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, userId: match.id, token }));
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

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('BloxyVault multiplayer server is running.\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
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
      socketToUsername.set(ws, username);
      usernameToSocket.set(username, ws);
      const u = ensureUser(username);
      send(ws, { type: 'login:ok', user: { username, coins: u.coins, inventory: u.inventory, stats: u.stats } });
      send(ws, { type: 'exchange:stock', stock: shopStock });
      db.loadCoinflipHistoryForUser(username, 10).then((history) => {
        send(ws, { type: 'coinflip:history', history });
      }).catch((err) => console.error(`[db] Failed to load coinflip history for ${username}:`, err.message));
      send(ws, { type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
      send(ws, { type: 'jackpot:state', ...jackpotPublicState() });
      send(ws, { type: 'rain:state', ...rainPublicState() });
      send(ws, { type: 'feed:recent', entries: liveFeed });
      broadcastBattlesTo(ws);
      if (username === ADMIN_USERNAME) {
        send(ws, { type: 'admin:withdrawList', requests: withdrawRequests.filter((r) => r.status === 'pending') });
      }
      return;
    }

    const username = socketToUsername.get(ws);
    if (!username) return; // must login first

    switch (msg.type) {
      case 'rain:deposit': return handleRainDeposit(username, msg);
      case 'rain:claim': return handleRainClaim(username);
      case 'coinflip:create': return handleCoinflipCreate(username, msg);
      case 'coinflip:join': return handleCoinflipJoin(username, msg);
      case 'coinflip:cancel': return handleCoinflipCancel(username, msg);
      case 'jackpot:enter': return handleJackpotEnter(username, msg);
      case 'casebattle:create': return handleCaseBattleCreate(username, msg);
      case 'casebattle:join': return handleCaseBattleJoin(username, msg);
      case 'casebattle:start': return handleCaseBattleStart(username, msg);
      case 'casebattle:callBot': return handleCaseBattleCallBot(username, msg);
      case 'casebattle:cancel': return handleCaseBattleCancel(username, msg);
      case 'exchange:sell': return handleExchangeSell(username, msg);
      case 'exchange:buy': return handleExchangeBuy(username, msg);
      case 'account:reset': return handleAccountReset(username);
      case 'debug:addTestCoins': return handleAddTestCoins(username);
      case 'admin:lookup': return handleAdminLookup(username, msg);
      case 'game:wager': return handleGameWager(username, msg);
      case 'game:resolve': return handleGameResolve(username, msg);
      case 'leaderboard:request': return handleLeaderboardRequest(username);
      case 'withdraw:request': return handleWithdrawRequest(username, msg);
      case 'withdraw:cancel': return handleWithdrawCancel(username, msg);
      case 'admin:withdrawList': return handleAdminWithdrawList(username);
      case 'admin:withdrawFulfill': return handleAdminWithdrawFulfill(username, msg);
      case 'admin:withdrawReject': return handleAdminWithdrawReject(username, msg);
      case 'chat:send': return handleChatSend(username, msg, ws);
      case 'tip:send': return handleTipSend(username, msg);
    }
  });

  ws.on('close', () => {
    const username = socketToUsername.get(ws);
    socketToUsername.delete(ws);
    if (username && usernameToSocket.get(username) === ws) usernameToSocket.delete(username);
  });
});

function broadcastBattlesTo(ws) {
  send(ws, {
    type: 'casebattle:list',
    battles: [...battles.values()].map((b) => ({
      id: b.id, creator: b.creator, status: b.status, mode: b.mode,
      caseQueue: b.caseQueue, caseNames: b.caseNames,
      players: b.players.map((p) => ({ username: p.username, isBot: p.isBot, team: p.team, total: p.total || 0 })),
      winner: b.winner, winnerValue: b.winnerValue,
    })),
  });
}

const PORT = process.env.PORT || 8080;

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

    const loadedUsers = await db.loadAllUsers();
    for (const [uname, u] of loadedUsers) users.set(uname, u);
    console.log(`[boot] Loaded ${loadedUsers.size} user account(s) from Postgres.`);

    const loadedRequests = await db.loadPendingWithdrawRequests();
    withdrawRequests = loadedRequests;
    console.log(`[boot] Loaded ${loadedRequests.length} pending withdrawal request(s) from Postgres.`);

    server.listen(PORT, () => console.log(`BloxyVault server listening on :${PORT}`));
  } catch (err) {
    console.error('[boot] Failed to start - could not initialize/load from Postgres:', err);
    process.exit(1);
  }
})();
