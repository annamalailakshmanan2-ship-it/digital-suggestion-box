/**
 * server.js
 * Simple Node/Express server that uses Firebase Admin SDK to persist suggestions
 * and broadcasts real-time updates to connected clients via Server-Sent Events (SSE).
 *
 * Setup:
 * 1) Install deps: npm install
 * 2) Provide Firebase admin credentials using either:
 *    - Set GOOGLE_APPLICATION_CREDENTIALS to the path of a service account JSON on the server, OR
 *    - Configure FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY env vars (private key must include literal \n for newlines or be restored on the host).
 * 3) Start: node server.js
 *
 * NOTE: Do NOT commit your service account JSON or private keys to this repository.
 */

const express = require('express');
const cors = require('cors');
const admin = require('firebase-admin');
const bodyParser = require('body-parser');
const path = require('path');
require('dotenv').config();

const PORT = process.env.PORT || 4000;

// Initialize Firebase Admin
try {
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    // When GOOGLE_APPLICATION_CREDENTIALS is set, admin.initializeApp() will use it.
    admin.initializeApp();
  } else if (process.env.FIREBASE_PROJECT_ID && process.env.FIREBASE_PRIVATE_KEY && process.env.FIREBASE_CLIENT_EMAIL) {
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
      })
    });
  } else {
    console.error('Firebase admin credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or FIREBASE_* env vars.');
    process.exit(1);
  }
} catch (err) {
  console.error('Failed to initialize Firebase Admin SDK:', err);
  process.exit(1);
}

const db = admin.firestore();

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '8mb' }));

// Serve static files from ./public
app.use(express.static(path.join(__dirname, 'public')));

// In-memory list of SSE clients
let sseClients = [];

// Firestore snapshot listener -> broadcast to SSE clients
try {
  db.collection('suggestions').orderBy('ts', 'desc').onSnapshot((snapshot) => {
    try {
      const suggestions = [];
      snapshot.forEach(doc => suggestions.push({ id: doc.id, ...doc.data() }));
      const payload = JSON.stringify({ suggestions });
      sseClients.forEach(res => res.write(`data: ${payload}\n\n`));
      console.info('Broadcasted update to', sseClients.length, 'clients');
    } catch (err) {
      console.error('Error processing snapshot:', err);
    }
  }, (err) => {
    console.error('Firestore onSnapshot error:', err);
  });
} catch (err) {
  console.error('Could not attach Firestore listener:', err);
}

// SSE stream endpoint
app.get('/api/stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });
  res.flushHeaders();
  res.write(': connected\n\n');

  sseClients.push(res);
  req.on('close', () => {
    sseClients = sseClients.filter(r => r !== res);
  });
});

// Create suggestion
app.post('/api/suggestions', async (req, res) => {
  try {
    const { text, category, photo } = req.body || {};
    if (!text || !category) return res.status(400).json({ error: 'Missing text or category' });

    const docRef = await db.collection('suggestions').add({
      text: String(text).trim(),
      category: String(category),
      photo: photo || null,
      status: 'New',
      ts: Date.now(),
      anonymous: true
    });

    const docSnap = await docRef.get();
    const created = { id: docRef.id, ...docSnap.data() };
    // snapshot listener will broadcast; reply to client as well
    res.status(201).json(created);
  } catch (err) {
    console.error('Error adding suggestion', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Get suggestions (fallback if SSE not used)
app.get('/api/suggestions', async (req, res) => {
  try {
    const snap = await db.collection('suggestions').orderBy('ts', 'desc').get();
    const suggestions = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json(suggestions);
  } catch (err) {
    console.error('Error fetching suggestions', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Mark resolved
app.post('/api/suggestions/:id/resolve', async (req, res) => {
  try {
    const id = req.params.id;
    const docRef = db.collection('suggestions').doc(id);
    const doc = await docRef.get();
    if (!doc.exists) return res.status(404).json({ error: 'Not found' });
    await docRef.update({ status: 'Resolved', resolvedAt: Date.now() });
    res.json({ ok: true });
  } catch (err) {
    console.error('Error marking resolved', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Dev helper: clear all suggestions (use only for demo/dev)
app.post('/api/suggestions/clear', async (req, res) => {
  try {
    const snap = await db.collection('suggestions').get();
    const batch = db.batch();
    snap.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    res.json({ ok: true });
  } catch (err) {
    console.error('Error clearing suggestions', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Fallback to index.html for SPA routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
