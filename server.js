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

const gameData = JSON.parse(fs.readFileSync(path.join(__dirname, 'gameData.json'), 'utf8'));
const { petCatalog, caseData, caseItems, modeConfigs, botNamesCf, botNamesBattles } = gameData;

const STARTING_BALANCE = 1000000; // bumped from 5000 - the cheapest case alone costs 375k, old value made the game unplayable for new accounts
const CASEBATTLE_ABANDON_MS = 5 * 60 * 1000; // if a creator walks away without calling bots or getting joined, clean it up after 5 minutes
const ROUND_PACE_MS = 4300;        // must match the client's MOCK_ROUND_PACE_MS
const CF_MATCH_TOLERANCE = 0.10;   // coinflip stakes must be within ±10% of each other, matching the client's own join-range check

// Site owner - gets the coinflip rake (see resolveCoinflip below) and is the
// only account allowed to use the admin:lookup inventory viewer. Whoever
// this is effectively runs the house, so keep this accurate.
const ADMIN_USERNAME = 'tim_tim1345';

// ---------------------------------------------------------------------------
// Connection + account state
// ---------------------------------------------------------------------------
const users = new Map();          // username -> { coins, inventory: {catalogId: qty} }
const socketToUsername = new Map(); // ws -> username
const usernameToSocket = new Map(); // username -> ws (most recent connection)

function ensureUser(username) {
  if (!users.has(username)) users.set(username, { coins: STARTING_BALANCE, inventory: {}, stats: { wagered: 0, won: 0, lost: 0 } });
  const u = users.get(username);
  if (!u.stats) u.stats = { wagered: 0, won: 0, lost: 0 }; // backfill for accounts created before stats existed
  return u;
}
function trackWager(username, amount) { if (amount > 0) ensureUser(username).stats.wagered += amount; }
// A cancelled coinflip lobby never actually played out, so it shouldn't
// keep counting toward "Played" / Most Played - this undoes the trackWager
// that ran when the lobby was first created.
function untrackWager(username, amount) { if (amount > 0) { const u = ensureUser(username); u.stats.wagered = Math.max(0, u.stats.wagered - amount); } }
function trackWin(username, amount) { if (amount > 0) ensureUser(username).stats.won += amount; }
function trackLoss(username, amount) { if (amount > 0) ensureUser(username).stats.lost += amount; }

function itemValue(id) {
  const p = petCatalog[id];
  return p ? p.value : 0;
}
function stakeValue(items) {
  return items.reduce((sum, id) => sum + itemValue(id), 0);
}

// Finds the subset of `items` whose combined value is closest to `target`.
// Used to pick out exactly which item(s) make up the "excess" on the
// larger side of an uneven-but-within-range coinflip stake, so that excess
// can be carved out as the house rake while the rest flips normally.
// Exhaustive search - fine since a single stake realistically has a small
// handful of items (2^n), but capped defensively just in case.
function pickClosestSubset(items, target){
  if(!items.length || target <= 0) return [];
  const n = Math.min(items.length, 20);
  const withValues = items.slice(0, n).map(id => ({ id, value: itemValue(id) }));
  let best = [], bestDiff = Infinity;
  for(let mask = 1; mask < (1 << n); mask++){
    let sum = 0; const subset = [];
    for(let i = 0; i < n; i++) if(mask & (1 << i)){ sum += withValues[i].value; subset.push(withValues[i].id); }
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
function syncAccount(username) {
  const ws = usernameToSocket.get(username);
  if (!ws) return; // not currently connected - they'll get fresh state on next login
  const u = ensureUser(username);
  send(ws, { type: 'account', user: { username, coins: u.coins, inventory: u.inventory, stats: u.stats } });
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
let cfLobbies = []; // { id, creator, side, items, isBotLobby? }
const botNameSet = new Set([...botNamesCf, ...botNamesBattles]); // never treated as real accounts

function broadcastCfLobbies() {
  broadcast({ type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
}

// Picks a stake of real catalog items landing near targetValue (or fully
// random if no target), same approach the old local practice-mode used.
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
    let sum = 0, combo = [];
    for (const [id, p] of shuffled) {
      if (sum + p.value <= hi) { combo.push(id); sum += p.value; }
      if (sum >= lo) break;
    }
    if (sum >= lo && sum <= hi) return combo;
  }
  const closestId = pool.reduce((best, [id, p]) => Math.abs(p.value - targetValue) < Math.abs(petCatalog[best].value - targetValue) ? id : best, pool[0][0]);
  return [closestId];
}

// A handful of open bot lobbies always available so there's something to do
// even if you're the only real player connected right now.
function seedBotLobbies() {
  const values = [40000, 120000, 400000];
  values.forEach((value, i) => {
    if (cfLobbies.some((l) => l.isBotLobby && l.seedIndex === i)) return;
    cfLobbies.push({
      id: 'cfbot_' + i + '_' + Date.now(),
      creator: botNamesCf[i % botNamesCf.length],
      side: Math.random() < 0.5 ? 'H' : 'T',
      items: pickBotStake(value),
      isBotLobby: true,
      seedIndex: i,
    });
  });
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
    const candidate = pickClosestSubset(fullHaul, targetRake);
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
  }
  if (!botNameSet.has(loser)) {
    trackLoss(loser, wonAmount);
  }
  if (rakeItems.length) {
    addItems(ADMIN_USERNAME, rakeItems);
    syncAccount(ADMIN_USERNAME);
  }

  syncAccount(creator);
  syncAccount(joiner);
  broadcast({ type: 'coinflip:result', creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems, winner, result });

  seedBotLobbies();
  broadcastCfLobbies();
}

function handleCoinflipCreate(username, msg) {
  const items = msg.items || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
  }
  removeItems(username, items); // escrow
  trackWager(username, stakeValue(items));
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
  if (coinsPortion > 0) u.coins += coinsPortion;
  if (allItems.length) addItems(winner.username, allItems);
  trackWin(winner.username, total);
  for (const e of round.entrants) {
    if (e.username !== winner.username) trackLoss(e.username, e.value);
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
  u.coins -= cost;
  trackWager(username, cost);
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
  u.coins -= cost;
  trackWager(username, cost);
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
    winners.forEach((p) => { ensureUser(p.username).coins += share; trackWin(p.username, share); syncAccount(p.username); });
    b.players.filter((p) => String(p.team) !== winningTeam && !p.isBot).forEach((p) => trackLoss(p.username, perPlayerCost));
  } else {
    const top = [...b.players].sort((a, c) => c.total - a.total)[0];
    winner = top.username;
    winnerValue = battleTotal; // the winner takes everyone's pulls, so the banner should reflect that, not just their own
    if (!top.isBot) { ensureUser(top.username).coins += battleTotal; trackWin(top.username, battleTotal); syncAccount(top.username); }
    b.players.filter((p) => p !== top && !p.isBot).forEach((p) => trackLoss(p.username, perPlayerCost));
  }

  b.status = 'finished';
  b.winner = winner;
  b.winnerValue = winnerValue;
  broadcastBattles();
  broadcast({
    type: 'casebattle:finished', battleId: b.id, winner, winnerValue,
    players: b.players.map((p) => ({ username: p.username, total: p.total })),
  });
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
  for (const [id, reqQty] of Object.entries(requested)) {
    const owned = u.inventory[id] || 0;
    const sellQty = Math.min(reqQty, owned);
    if (sellQty <= 0) continue;
    rawTotal += sellQty * itemValue(id);
    u.inventory[id] = owned - sellQty;
  }
  const payout = Math.floor(rawTotal * (1 - TAX_RATE));
  u.coins += payout;
  syncAccount(username);
}

function handleExchangeBuy(username, msg) {
  const ids = msg.items || [];
  const cost = ids.reduce((sum, id) => sum + itemValue(id), 0);
  const u = ensureUser(username);
  if (u.coins < cost) return send(usernameToSocket.get(username), { type: 'error', message: `Not enough coins - you need ${cost}.` });
  u.coins -= cost;
  addItems(username, ids);
  syncAccount(username);
}

// Practice-mode convenience: resets your OWN account back to the starting
// balance. This is intentionally a "give yourself money" cheat - fine for a
// solo hobby server, but if you ever have real friends playing against each
// other here for keeps, you may want to remove this handler so balances
// actually mean something in head-to-head games.
function handleAccountReset(username) {
  const u = ensureUser(username);
  u.coins = STARTING_BALANCE;
  u.inventory = {};
  syncAccount(username);
}

// Same "give yourself money" cheat as above, additive instead of a reset.
// Only ever affects the sender's own account - the amount is fixed
// server-side (not read from the client) so it can't be abused into an
// arbitrary-amount cheat via a hand-crafted message.
function handleAddTestCoins(username) {
  const u = ensureUser(username);
  u.coins += 1000000000;
  syncAccount(username);
}

// Admin-only, view-only lookup of another player's current coins/inventory
// - lets the site owner verify a withdrawal claim without being able to
// add, remove, or edit anything in that account. Silently ignored for
// anyone who isn't ADMIN_USERNAME.
// Ranks every account the server has data for by lifetime stats. Bots
// aren't real accounts (never touched via ensureUser), so they naturally
// never appear here - only real players.
const LEADERBOARD_SIZE = 10;
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
  const u = ensureUser(username);
  if (amount > u.coins) return; // can't wager more than they actually have
  if (amount > 0) u.coins -= amount;
  for (const id of items) {
    u.inventory[id] = Math.max(0, (u.inventory[id] || 0) - 1); // clamped - never goes negative even if client/server briefly disagree
  }
  trackWager(username, amount > 0 ? amount : stakeValue(items));
  syncAccount(username);
}

function handleGameResolve(username, msg) {
  const payout = Math.max(0, Math.floor(Number(msg.payout) || 0));
  const wager = Math.max(0, Math.floor(Number(msg.wager) || 0));
  const items = Array.isArray(msg.items) ? msg.items.filter(id => typeof id === 'string') : [];
  const u = ensureUser(username);
  if (payout > 0) {
    u.coins += payout;
    trackWin(username, payout);
  } else if (wager > 0) {
    trackLoss(username, wager);
  }
  for (const id of items) {
    u.inventory[id] = (u.inventory[id] || 0) + 1;
  }
  syncAccount(username);
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

function handleWithdrawRequest(username, msg) {
  const items = msg.items || [];
  const errTo = usernameToSocket.get(username);
  if (!items.length) return send(errTo, { type: 'error', message: 'Select at least one item to withdraw.' });
  if (!ownsAll(username, items)) return send(errTo, { type: 'error', message: "You don't own all of those items." });

  const id = 'wd_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  withdrawRequests.push({ id, username, items, requestedAt: Date.now(), status: 'pending' });
  send(errTo, { type: 'withdraw:requested', id });
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

function handleAdminWithdrawFulfill(username, msg) {
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
  req.status = 'fulfilled';
  req.actuallyRemoved = actuallyRemoved;
  syncAccount(req.username);
  send(usernameToSocket.get(req.username), { type: 'chat:message', username: 'System', text: `Your withdrawal request was fulfilled by ${ADMIN_USERNAME}.`, timestamp: Date.now(), system: true });
  broadcastPendingWithdrawalsToAdmin();
}

function handleAdminWithdrawReject(username, msg) {
  if (username !== ADMIN_USERNAME) return;
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending');
  if (!req) return;
  req.status = 'rejected';
  send(usernameToSocket.get(req.username), { type: 'error', message: 'Your withdrawal request was declined.' });
  broadcastPendingWithdrawalsToAdmin();
}

function handleWithdrawCancel(username, msg) {
  const req = withdrawRequests.find(r => r.id === msg.requestId && r.status === 'pending' && r.username === username);
  if (!req) return;
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

  if (coins > 0) { u.coins -= coins; target.coins += coins; }
  if (items.length) { removeItems(username, items); addItems(toUsername, items); }

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
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, userId: match.id }));
    } catch(e){
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, reason: "Couldn't reach Roblox right now — try again in a few seconds." }));
    }
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
      socketToUsername.set(ws, username);
      usernameToSocket.set(username, ws);
      const u = ensureUser(username);
      send(ws, { type: 'login:ok', user: { username, coins: u.coins, inventory: u.inventory, stats: u.stats } });
      send(ws, { type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
      send(ws, { type: 'jackpot:state', ...jackpotPublicState() });
      broadcastBattlesTo(ws);
      if (username === ADMIN_USERNAME) {
        send(ws, { type: 'admin:withdrawList', requests: withdrawRequests.filter((r) => r.status === 'pending') });
      }
      return;
    }

    const username = socketToUsername.get(ws);
    if (!username) return; // must login first

    switch (msg.type) {
      case 'coinflip:create': return handleCoinflipCreate(username, msg);
      case 'coinflip:join': return handleCoinflipJoin(username, msg);
      case 'coinflip:cancel': return handleCoinflipCancel(username, msg);
      case 'jackpot:enter': return handleJackpotEnter(username, msg);
      case 'casebattle:create': return handleCaseBattleCreate(username, msg);
      case 'casebattle:join': return handleCaseBattleJoin(username, msg);
      case 'casebattle:start': return handleCaseBattleStart(username, msg);
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
seedBotLobbies(); // so there's something to do even before any real player connects
server.listen(PORT, () => console.log(`BloxyVault server listening on :${PORT}`));
