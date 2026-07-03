require('dotenv').config();
console.log('--- DATABASE CONNECTION DIAGNOSTICS ---');
console.log('DATABASE_URL env var:', process.env.DATABASE_URL ? `Defined (length: ${process.env.DATABASE_URL.length})` : 'Undefined');
console.log('PostgreSQL env var:', process.env.PostgreSQL ? `Defined (length: ${process.env.PostgreSQL.length})` : 'Undefined');

const { Pool } = require('pg');
const path = require('path');

const databaseUrl = process.env.DATABASE_URL || process.env.PostgreSQL;
const usePostgres = !!databaseUrl;
console.log('usePostgres resolved to:', usePostgres);
console.log('---------------------------------------');

let pgPool;
let sqliteDb;
let useMemoryDb = false;
let memoryTranscripts = [];
let memorySummaries = new Map();
let memoryAttendance = [];

if (usePostgres) {
  pgPool = new Pool({
    connectionString: databaseUrl,
    ssl: {
      rejectUnauthorized: false
    }
  });
  console.log('Connected to PostgreSQL database.');
  
  // Init tables for PG
  pgPool.query(`
    CREATE TABLE IF NOT EXISTS transcripts (
      id SERIAL PRIMARY KEY,
      room_id VARCHAR(255),
      sender_name VARCHAR(255),
      text TEXT,
      timestamp VARCHAR(255)
    );
    
    CREATE TABLE IF NOT EXISTS summaries (
      room_id VARCHAR(255) PRIMARY KEY,
      summary_text TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS attendance (
      id SERIAL PRIMARY KEY,
      room_id VARCHAR(255),
      socket_id VARCHAR(255),
      user_name VARCHAR(255),
      joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      left_at TIMESTAMP
    );
  `).catch(err => console.error('Error creating PG tables', err));

} else {
  try {
    const sqlite3 = require('sqlite3').verbose();
    const dbPath = path.resolve(__dirname, 'meetings.db');
    sqliteDb = new sqlite3.Database(dbPath, (err) => {
      if (err) {
        console.error('Error opening SQLite database', err.message);
      } else {
        console.log('Connected to the SQLite database.');
        sqliteDb.run(`CREATE TABLE IF NOT EXISTS transcripts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          room_id TEXT,
          sender_name TEXT,
          text TEXT,
          timestamp TEXT
        )`);
        
        sqliteDb.run(`CREATE TABLE IF NOT EXISTS summaries (
          room_id TEXT PRIMARY KEY,
          summary_text TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`);
        
        sqliteDb.run(`CREATE TABLE IF NOT EXISTS attendance (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          room_id TEXT,
          socket_id TEXT,
          user_name TEXT,
          joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          left_at DATETIME
        )`);
      }
    });
  } catch (err) {
    console.warn('CRITICAL WARNING: SQLite3 native binding could not be loaded. Falling back to an in-memory database mock.', err.message);
    useMemoryDb = true;
  }
}

function insertTranscript(roomId, senderName, text, timestamp) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'INSERT INTO transcripts (room_id, sender_name, text, timestamp) VALUES ($1, $2, $3, $4) RETURNING id',
        [roomId, senderName, text, timestamp]
      ).then(res => resolve(res.rows[0].id)).catch(reject);
    } else if (useMemoryDb) {
      const newId = memoryTranscripts.length + 1;
      memoryTranscripts.push({ id: newId, room_id: roomId, sender_name: senderName, text: text, timestamp: timestamp });
      resolve(newId);
    } else {
      const stmt = sqliteDb.prepare('INSERT INTO transcripts (room_id, sender_name, text, timestamp) VALUES (?, ?, ?, ?)');
      stmt.run([roomId, senderName, text, timestamp], function(err) {
        if (err) reject(err);
        else resolve(this.lastID);
      });
      stmt.finalize();
    }
  });
}

function getTranscripts(roomId) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'SELECT sender_name, text, timestamp FROM transcripts WHERE room_id = $1 ORDER BY id ASC',
        [roomId]
      ).then(res => resolve(res.rows)).catch(reject);
    } else if (useMemoryDb) {
      const rows = memoryTranscripts.filter(t => t.room_id === roomId).map(t => ({
        sender_name: t.sender_name,
        text: t.text,
        timestamp: t.timestamp
      }));
      resolve(rows);
    } else {
      sqliteDb.all('SELECT sender_name, text, timestamp FROM transcripts WHERE room_id = ? ORDER BY id ASC', [roomId], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    }
  });
}

function saveSummary(roomId, summaryText) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'INSERT INTO summaries (room_id, summary_text) VALUES ($1, $2) ON CONFLICT (room_id) DO UPDATE SET summary_text = EXCLUDED.summary_text',
        [roomId, summaryText]
      ).then(() => resolve()).catch(reject);
    } else if (useMemoryDb) {
      memorySummaries.set(roomId, summaryText);
      resolve();
    } else {
      const stmt = sqliteDb.prepare('INSERT OR REPLACE INTO summaries (room_id, summary_text) VALUES (?, ?)');
      stmt.run([roomId, summaryText], function(err) {
        if (err) reject(err);
        else resolve();
      });
      stmt.finalize();
    }
  });
}

function getSummary(roomId) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'SELECT summary_text FROM summaries WHERE room_id = $1',
        [roomId]
      ).then(res => resolve(res.rows.length ? res.rows[0].summary_text : null)).catch(reject);
    } else if (useMemoryDb) {
      resolve(memorySummaries.get(roomId) || null);
    } else {
      sqliteDb.get('SELECT summary_text FROM summaries WHERE room_id = ?', [roomId], (err, row) => {
        if (err) reject(err);
        else resolve(row ? row.summary_text : null);
      });
    }
  });
}

function recordJoin(roomId, socketId, userName) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'INSERT INTO attendance (room_id, socket_id, user_name) VALUES ($1, $2, $3) RETURNING id',
        [roomId, socketId, userName]
      ).then(res => resolve(res.rows[0].id)).catch(reject);
    } else if (useMemoryDb) {
      const newId = memoryAttendance.length + 1;
      memoryAttendance.push({
        id: newId,
        room_id: roomId,
        socket_id: socketId,
        user_name: userName,
        joined_at: new Date().toISOString(),
        left_at: null
      });
      resolve(newId);
    } else {
      const stmt = sqliteDb.prepare('INSERT INTO attendance (room_id, socket_id, user_name) VALUES (?, ?, ?)');
      stmt.run([roomId, socketId, userName], function(err) {
        if (err) reject(err);
        else resolve(this.lastID);
      });
      stmt.finalize();
    }
  });
}

function recordLeave(roomId, socketId) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'UPDATE attendance SET left_at = CURRENT_TIMESTAMP WHERE room_id = $1 AND socket_id = $2 AND left_at IS NULL',
        [roomId, socketId]
      ).then(() => resolve()).catch(reject);
    } else if (useMemoryDb) {
      const record = memoryAttendance.find(a => a.room_id === roomId && a.socket_id === socketId && a.left_at === null);
      if (record) {
        record.left_at = new Date().toISOString();
      }
      resolve();
    } else {
      const stmt = sqliteDb.prepare('UPDATE attendance SET left_at = CURRENT_TIMESTAMP WHERE room_id = ? AND socket_id = ? AND left_at IS NULL');
      stmt.run([roomId, socketId], function(err) {
        if (err) reject(err);
        else resolve();
      });
      stmt.finalize();
    }
  });
}

function getAttendance(roomId) {
  return new Promise((resolve, reject) => {
    if (usePostgres) {
      pgPool.query(
        'SELECT user_name, joined_at, left_at FROM attendance WHERE room_id = $1 ORDER BY joined_at ASC',
        [roomId]
      ).then(res => resolve(res.rows)).catch(reject);
    } else if (useMemoryDb) {
      const rows = memoryAttendance.filter(a => a.room_id === roomId).map(a => ({
        user_name: a.user_name,
        joined_at: a.joined_at,
        left_at: a.left_at
      }));
      resolve(rows);
    } else {
      sqliteDb.all('SELECT user_name, joined_at, left_at FROM attendance WHERE room_id = ? ORDER BY joined_at ASC', [roomId], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    }
  });
}

function searchTranscripts(query) {
  return new Promise((resolve, reject) => {
    const searchTerm = `%${query}%`;
    if (usePostgres) {
      pgPool.query(
        'SELECT room_id, sender_name, text, timestamp FROM transcripts WHERE text ILIKE $1 ORDER BY id DESC LIMIT 50',
        [searchTerm]
      ).then(res => resolve(res.rows)).catch(reject);
    } else if (useMemoryDb) {
      const rows = memoryTranscripts.filter(t => t.text.toLowerCase().includes(query.toLowerCase())).slice(0, 50).map(t => ({
        room_id: t.room_id,
        sender_name: t.sender_name,
        text: t.text,
        timestamp: t.timestamp
      }));
      resolve(rows);
    } else {
      sqliteDb.all('SELECT room_id, sender_name, text, timestamp FROM transcripts WHERE text LIKE ? ORDER BY id DESC LIMIT 50', [searchTerm], (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    }
  });
}

module.exports = {
  db: usePostgres ? pgPool : sqliteDb,
  insertTranscript,
  getTranscripts,
  saveSummary,
  getSummary,
  recordJoin,
  recordLeave,
  getAttendance,
  searchTranscripts
};
