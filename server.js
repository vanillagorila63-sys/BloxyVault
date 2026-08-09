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
  if (!users.has(username)) users.set(username, { coins: STARTING_BALANCE, inventory: {} });
  return users.get(username);
}
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
  send(ws, { type: 'account', user: { username, coins: u.coins, inventory: u.inventory } });
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

function resolveCoinflip(creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems) {
  const creatorValue = stakeValue(creatorItems);
  const joinerValue = stakeValue(joinerItems);

  // House rake: when the two sides aren't exactly equal (allowed as long as
  // they're within the ±10% match window enforced in handleCoinflipJoin),
  // the item(s) on the larger side that make up that difference go straight
  // to the house instead of into the flip. A perfectly matched flip (equal
  // values) has no excess, so no rake - matches an even 1-for-1 exactly.
  let rakeItems = [];
  let matchedCreatorItems = creatorItems;
  let matchedJoinerItems = joinerItems;
  if (creatorValue !== joinerValue) {
    const excess = Math.abs(creatorValue - joinerValue);
    const biggerIsCreator = creatorValue > joinerValue;
    const biggerItems = biggerIsCreator ? creatorItems : joinerItems;
    rakeItems = pickClosestSubset(biggerItems, excess);
    if (rakeItems.length) {
      if (biggerIsCreator) matchedCreatorItems = creatorItems.filter((id) => !rakeItems.includes(id));
      else matchedJoinerItems = joinerItems.filter((id) => !rakeItems.includes(id));
    }
  }

  const result = Math.random() < 0.5 ? 'H' : 'T';
  const winner = result === creatorSide ? creator : joiner;
  const loser = winner === creator ? joiner : creator;
  const winnerItems = winner === creator ? matchedCreatorItems : matchedJoinerItems;
  const loserItems = winner === creator ? matchedJoinerItems : matchedCreatorItems;

  // Both sides' stakes were escrowed (removed from inventory) back at
  // coinflip:create / coinflip:join time. The winner gets their own
  // (matched, post-rake) stake back PLUS the loser's (matched) stake; the
  // loser's stake just stays gone. Bots aren't real accounts, so skip
  // touching `users` for them entirely.
  if (!botNameSet.has(winner)) {
    addItems(winner, winnerItems);
    addItems(winner, loserItems);
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
  cfLobbies = cfLobbies.filter((l) => l.id !== msg.lobbyId);
  broadcastCfLobbies();
  const joinerSide = lobby.side === 'H' ? 'T' : 'H';
  resolveCoinflip(lobby.creator, lobby.side, lobby.items, username, joinerSide, items);
}

function handleCoinflipCancel(username, msg) {
  const lobby = cfLobbies.find((l) => l.id === msg.lobbyId);
  if (!lobby || lobby.creator !== username) return;
  addItems(username, lobby.items); // return escrow
  cfLobbies = cfLobbies.filter((l) => l.id !== msg.lobbyId);
  syncAccount(username);
  broadcastCfLobbies();
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
  if (b.cfg.isTeam) {
    const teamTotals = {};
    b.players.forEach((p) => { teamTotals[p.team] = (teamTotals[p.team] || 0) + p.total; });
    const winningTeam = Object.entries(teamTotals).sort((a, c) => c[1] - a[1])[0][0];
    winner = `Team ${String.fromCharCode(65 + Number(winningTeam))}`;
    winnerValue = battleTotal; // full combined pot, matching how "Team A won X" is displayed as one number
    const winners = b.players.filter((p) => String(p.team) === winningTeam && !p.isBot);
    const share = Math.floor(battleTotal / Math.max(1, b.players.filter((p) => String(p.team) === winningTeam).length));
    winners.forEach((p) => { ensureUser(p.username).coins += share; syncAccount(p.username); });
  } else {
    const top = [...b.players].sort((a, c) => c.total - a.total)[0];
    winner = top.username;
    winnerValue = battleTotal; // the winner takes everyone's pulls, so the banner should reflect that, not just their own
    if (!top.isBot) { ensureUser(top.username).coins += battleTotal; syncAccount(top.username); }
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
  const ids = [...new Set(msg.items || [])]; // sells ALL owned qty of each id, matching client semantics
  const u = ensureUser(username);
  let rawTotal = 0;
  for (const id of ids) {
    const qty = u.inventory[id] || 0;
    const val = itemValue(id);
    rawTotal += qty * val;
    u.inventory[id] = 0;
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
  u.coins += 10000000;
  syncAccount(username);
}

// Admin-only, view-only lookup of another player's current coins/inventory
// - lets the site owner verify a withdrawal claim without being able to
// add, remove, or edit anything in that account. Silently ignored for
// anyone who isn't ADMIN_USERNAME.
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
  broadcast({ type: 'chat:message', username, text, timestamp: now });
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
const server = http.createServer((req, res) => {
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
      send(ws, { type: 'login:ok', user: { username, coins: u.coins, inventory: u.inventory } });
      send(ws, { type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
      broadcastBattlesTo(ws);
      return;
    }

    const username = socketToUsername.get(ws);
    if (!username) return; // must login first

    switch (msg.type) {
      case 'coinflip:create': return handleCoinflipCreate(username, msg);
      case 'coinflip:join': return handleCoinflipJoin(username, msg);
      case 'coinflip:cancel': return handleCoinflipCancel(username, msg);
      case 'casebattle:create': return handleCaseBattleCreate(username, msg);
      case 'casebattle:join': return handleCaseBattleJoin(username, msg);
      case 'casebattle:start': return handleCaseBattleStart(username, msg);
      case 'exchange:sell': return handleExchangeSell(username, msg);
      case 'exchange:buy': return handleExchangeBuy(username, msg);
      case 'account:reset': return handleAccountReset(username);
      case 'debug:addTestCoins': return handleAddTestCoins(username);
      case 'admin:lookup': return handleAdminLookup(username, msg);
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
