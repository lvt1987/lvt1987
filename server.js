require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
const db = new Database(path.join(dataDir, 'autopost.sqlite'));
db.pragma('journal_mode = WAL');
db.exec(`
CREATE TABLE IF NOT EXISTS posts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 title TEXT NOT NULL,
 content TEXT NOT NULL,
 platforms TEXT NOT NULL,
 scheduled_at TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'scheduled',
 error TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 published_at TEXT
);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 post_id INTEGER,
 event TEXT NOT NULL,
 details TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`);
const logEvent = db.prepare('INSERT INTO events (post_id,event,details) VALUES (?,?,?)');

function validPost(body) {
  return body && typeof body.title === 'string' && body.title.trim() &&
    typeof body.content === 'string' && body.content.trim() &&
    Array.isArray(body.platforms) && body.platforms.length > 0 &&
    typeof body.scheduledAt === 'string' && !Number.isNaN(Date.parse(body.scheduledAt));
}

async function publishFacebookPageText(content) {
  const enabled = process.env.ENABLE_FACEBOOK_PUBLISHING === 'true';
  const version = process.env.META_GRAPH_VERSION;
  const pageId = process.env.FACEBOOK_PAGE_ID;
  const token = process.env.FACEBOOK_PAGE_ACCESS_TOKEN;
  if (!enabled || !version || version.includes('X') || !pageId || !token) {
    throw new Error('Facebook publishing is not configured. Add valid Meta API settings and enable publishing.');
  }
  const endpoint = `https://graph.facebook.com/${version}/${encodeURIComponent(pageId)}/feed`;
  const form = new URLSearchParams({ message: content, access_token: token });
  const response = await fetch(endpoint, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: form });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || result.error) throw new Error(result.error?.message || `Meta API request failed (${response.status})`);
  return result;
}

async function publishPost(post) {
  const platforms = JSON.parse(post.platforms);
  const results = [];
  // Deliberately only publishes to Facebook Pages in this starter. Other platforms must be implemented with their official APIs.
  for (const platform of platforms) {
    if (platform === 'Facebook') {
      const result = await publishFacebookPageText(post.content);
      results.push({ platform, id: result.id });
    } else {
      throw new Error(`${platform} publishing adapter is not implemented yet. Remove it from this post or implement its official API adapter.`);
    }
  }
  return results;
}

async function processDuePosts() {
  const due = db.prepare("SELECT * FROM posts WHERE status='scheduled' AND scheduled_at <= ? ORDER BY scheduled_at LIMIT 10").all(new Date().toISOString());
  for (const post of due) {
    try {
      const results = await publishPost(post);
      db.prepare("UPDATE posts SET status='published', published_at=?, error=NULL WHERE id=?").run(new Date().toISOString(), post.id);
      logEvent.run(post.id, 'published', JSON.stringify(results));
    } catch (err) {
      db.prepare("UPDATE posts SET status='failed', error=? WHERE id=?").run(String(err.message || err).slice(0, 1000), post.id);
      logEvent.run(post.id, 'failed', String(err.message || err).slice(0, 1000));
    }
  }
}
// Check every minute. For production, run a durable queue/worker and monitor missed jobs.
cron.schedule('* * * * *', () => { processDuePosts().catch(err => console.error('Scheduler error:', err)); });

app.get('/api/health', (_req, res) => res.json({ ok: true, app: 'AutoPost MVP', facebookConfigured: process.env.ENABLE_FACEBOOK_PUBLISHING === 'true' && !!process.env.FACEBOOK_PAGE_ID && !!process.env.FACEBOOK_PAGE_ACCESS_TOKEN }));
app.get('/api/posts', (_req, res) => {
  const posts = db.prepare('SELECT * FROM posts ORDER BY scheduled_at DESC LIMIT 200').all().map(p => ({ ...p, platforms: JSON.parse(p.platforms) }));
  res.json(posts);
});
app.post('/api/posts', (req, res) => {
  if (!validPost(req.body)) return res.status(400).json({ error: 'Please provide title, content, platforms and a valid scheduledAt date.' });
  const platforms = [...new Set(req.body.platforms)];
  const allowed = new Set(['Facebook', 'Instagram', 'TikTok', 'YouTube', 'Zalo', 'Telegram']);
  if (platforms.some(p => !allowed.has(p))) return res.status(400).json({ error: 'Unsupported platform.' });
  const when = new Date(req.body.scheduledAt);
  if (when.getTime() < Date.now() - 60000) return res.status(400).json({ error: 'Schedule must be in the future.' });
  const info = db.prepare('INSERT INTO posts (title,content,platforms,scheduled_at,status) VALUES (?,?,?,?,?)').run(req.body.title.trim(), req.body.content.trim(), JSON.stringify(platforms), when.toISOString(), 'scheduled');
  logEvent.run(info.lastInsertRowid, 'scheduled', 'Post added to scheduler');
  res.status(201).json({ id: info.lastInsertRowid });
});
app.delete('/api/posts/:id', (req, res) => {
  const info = db.prepare("DELETE FROM posts WHERE id=? AND status='scheduled'").run(Number(req.params.id));
  if (!info.changes) return res.status(404).json({ error: 'Scheduled post not found or cannot be deleted.' });
  res.json({ ok: true });
});
app.get('/api/events', (_req, res) => res.json(db.prepare('SELECT * FROM events ORDER BY id DESC LIMIT 100').all()));
app.get('*', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
const port = Number(process.env.PORT || 3000);
app.listen(port, () => console.log(`AutoPost MVP running on http://localhost:${port}`));
