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

const STARTING_BALANCE = 5000;
const CASEBATTLE_WAIT_MS = 8000;   // must match the client's enterWaitingRoom() countdown
const ROUND_PACE_MS = 4300;        // must match the client's MOCK_ROUND_PACE_MS

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
let cfLobbies = []; // { id, creator, side, items }

function broadcastCfLobbies() {
  broadcast({ type: 'coinflip:lobbies', lobbies: cfLobbies.map((l) => ({ id: l.id, creator: l.creator, side: l.side, items: l.items })) });
}

function resolveCoinflip(creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems) {
  const result = Math.random() < 0.5 ? 'H' : 'T';
  const winner = result === creatorSide ? creator : joiner;
  const loser = winner === creator ? joiner : creator;
  const winnerItems = winner === creator ? creatorItems : joinerItems;
  const loserItems = winner === creator ? joinerItems : creatorItems;

  // Both sides' stakes were escrowed (removed from inventory) back at
  // coinflip:create / coinflip:join time. The winner gets their own stake
  // back PLUS everything the loser staked; the loser's stake just stays gone.
  addItems(winner, winnerItems);
  addItems(winner, loserItems);

  syncAccount(creator);
  syncAccount(joiner);
  broadcast({ type: 'coinflip:result', creator, creatorSide, creatorItems, joiner, joinerSide, joinerItems, winner, result });
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
}

function handleCoinflipJoin(username, msg) {
  const lobby = cfLobbies.find((l) => l.id === msg.lobbyId);
  if (!lobby) return send(usernameToSocket.get(username), { type: 'error', message: 'That lobby is no longer available.' });
  const items = msg.items || [];
  if (!items.length || !ownsAll(username, items)) {
    return send(usernameToSocket.get(username), { type: 'error', message: "You don't own those items." });
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

  // Safety net in case this client never sends casebattle:start (e.g. they
  // closed the tab during the waiting room) - the battle still needs to
  // resolve for anyone else watching/waiting on it.
  setTimeout(() => {
    const b = battles.get(id);
    if (b && b.status === 'waiting') startBattle(b);
  }, CASEBATTLE_WAIT_MS + 1500);
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
server.listen(PORT, () => console.log(`BloxyVault server listening on :${PORT}`));
