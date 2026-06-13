require('dotenv').config();
const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { insertTranscript, getTranscripts, saveSummary, getSummary, recordJoin, recordLeave, getAttendance, searchTranscripts } = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ['http://localhost:3000', 'http://localhost:5173', 'http://localhost:5174'],
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, '..', 'client', 'public')));

// Simple room map: roomId -> { hostId, members: Map<socketId, userInfo> }
// userInfo: { id, name, isMuted, isVideoOff, isSharingScreen }
const rooms = new Map();
// Pending guests: socketId -> { socket, roomId, name, userInfo }
const pendingGuests = new Map();

// Collaborative features state
const sharedCode = new Map(); // roomId -> string
const activePolls = new Map(); // roomId -> { question, options: [{text, votes}], hasVoted: Set<socketId> }

io.on('connection', (socket) => {
  // Helper to fully join a room after passing lobby
  const performJoin = (socket, roomId, name, isHost) => {
    socket.join(roomId);
    if (!rooms.has(roomId)) {
      rooms.set(roomId, { hostId: isHost ? socket.id : null, members: new Map() });
    }
    
    const roomObj = rooms.get(roomId);
    if (isHost && !roomObj.hostId) roomObj.hostId = socket.id;

    const userInfo = { id: socket.id, name: name || 'Guest', isMuted: false, isVideoOff: false, isSharingScreen: false };
    roomObj.members.set(socket.id, userInfo);

    const existingPeers = Array.from(roomObj.members.values()).filter((u) => u.id !== socket.id);
    
    // Tell the new user they are accepted
    socket.emit('join-accepted', { peers: existingPeers, isHost });

    // Send latest shared code if it exists
    if (sharedCode.has(roomId)) {
      socket.emit('code-update', { code: sharedCode.get(roomId) });
    }
    
    // Send active poll if it exists
    if (activePolls.has(roomId)) {
      const poll = activePolls.get(roomId);
      const totalVotes = poll.hasVoted.size;
      const results = poll.options.map(opt => ({
        text: opt.text,
        votes: opt.votes,
        percentage: totalVotes === 0 ? 0 : Math.round((opt.votes / totalVotes) * 100)
      }));
      socket.emit('poll-started', {
        question: poll.question,
        options: poll.options.map(opt => opt.text),
        existingResults: results,
        hasVoted: poll.hasVoted.has(socket.id),
        totalVotes
      });
    }

    // Notify existing participants about new peer
    socket.to(roomId).emit('peer-joined', { peer: userInfo });
    
    // Record Attendance
    recordJoin(roomId, socket.id, userInfo.name).catch(console.error);
  };

  socket.on('request-join', ({ roomId, name }) => {
    // If room doesn't exist, this person is the host and joins immediately
    if (!rooms.has(roomId) || rooms.get(roomId).members.size === 0) {
      performJoin(socket, roomId, name, true);
    } else {
      // Room exists, they are a guest. Send to lobby.
      const roomObj = rooms.get(roomId);
      const userInfo = { id: socket.id, name: name || 'Guest', isMuted: false, isVideoOff: false, isSharingScreen: false };
      
      // Store pending guest
      pendingGuests.set(socket.id, { socket, roomId, name, userInfo });
      
      socket.emit('lobby-waiting');
      
      // Notify host
      if (roomObj.hostId) {
        io.to(roomObj.hostId).emit('lobby-request', { guest: userInfo });
      } else {
        // Fallback: if no host found, auto-admit (or maybe deny?)
        // For now, let's auto-admit if host is missing
        pendingGuests.delete(socket.id);
        performJoin(socket, roomId, name, false);
      }
    }
  });

  socket.on('lobby-response', ({ guestId, admitted }) => {
    if (!pendingGuests.has(guestId)) return;
    const guestData = pendingGuests.get(guestId);
    pendingGuests.delete(guestId);

    if (admitted) {
      performJoin(guestData.socket, guestData.roomId, guestData.name, false);
    } else {
      guestData.socket.emit('join-denied');
    }
  });

  // Legacy join (if needed by old code, map it to request-join)
  socket.on('join', ({ roomId, name }) => {
    // Forward to new logic
    const dummyEmit = socket.emit;
    // We just override what happens next
  });

  socket.on('signal', (data) => {
    // data: { to, from, signal }
    const { to } = data;
    if (to) io.to(to).emit('signal', data);
  });
  
  socket.on('chat-message', (data) => {
    // data: { roomId, message, name, timestamp }
    socket.to(data.roomId).emit('chat-message', data);
  });

  socket.on('media-state', (data) => {
    // data: { roomId, isMuted, isVideoOff }
    const roomObj = rooms.get(data.roomId);
    if (roomObj && roomObj.members.has(socket.id)) {
      const user = roomObj.members.get(socket.id);
      user.isMuted = data.isMuted;
      user.isVideoOff = data.isVideoOff;
      socket.to(data.roomId).emit('media-state-changed', {
        peerId: socket.id,
        isMuted: data.isMuted,
        isVideoOff: data.isVideoOff
      });
    }
  });

  socket.on('draw-line', (data) => {
    socket.to(data.roomId).emit('draw-line', data);
  });

  socket.on('code-update', (data) => {
    sharedCode.set(data.roomId, data.code);
    socket.to(data.roomId).emit('code-update', data);
  });

  socket.on('poll-create', (data) => {
    // data: { roomId, question, options }
    const poll = {
      question: data.question,
      options: data.options.map(opt => ({ text: opt, votes: 0 })),
      hasVoted: new Set()
    };
    activePolls.set(data.roomId, poll);
    io.to(data.roomId).emit('poll-started', {
      question: poll.question,
      options: poll.options.map(opt => opt.text)
    });
  });

  socket.on('poll-vote', (data) => {
    // data: { roomId, optionIndex }
    const poll = activePolls.get(data.roomId);
    if (!poll || poll.hasVoted.has(socket.id)) return;
    
    poll.hasVoted.add(socket.id);
    if (poll.options[data.optionIndex]) {
      poll.options[data.optionIndex].votes += 1;
    }
    
    const totalVotes = poll.hasVoted.size;
    const results = poll.options.map(opt => ({
      text: opt.text,
      votes: opt.votes,
      percentage: totalVotes === 0 ? 0 : Math.round((opt.votes / totalVotes) * 100)
    }));
    
    io.to(data.roomId).emit('poll-updated', { results, totalVotes });
  });

  socket.on('screen-share', (data) => {
    const roomObj = rooms.get(data.roomId);
    if (roomObj && roomObj.members.has(socket.id)) {
      const user = roomObj.members.get(socket.id);
      user.isSharingScreen = data.isSharing;
    }
    socket.to(data.roomId).emit('screen-share-status', { peerId: socket.id, isSharing: data.isSharing });
  });

  socket.on('caption-text', async (data) => {
    // data: { roomId, name, text }
    socket.to(data.roomId).emit('caption-text', data);
    
    const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    try {
      await insertTranscript(data.roomId, data.name, data.text, time);
    } catch(err) {
      console.error('DB Insert Error:', err);
    }
  });

  socket.on('get-summary', async ({ roomId }, callback) => {
    try {
      // Check if we already have a summary saved
      const existingSummary = await getSummary(roomId);
      if (existingSummary) {
        return callback({ summary: existingSummary });
      }

      const transcripts = await getTranscripts(roomId);
      if (transcripts.length === 0) {
        return callback({ summary: "No conversation was recorded to summarize." });
      }
      
      const textBlocks = transcripts.map(t => `[${t.timestamp}] ${t.sender_name}: ${t.text}`).join('\n');
      
      const prompt = `You are an expert AI executive assistant. Please read the following meeting transcript and provide a structured output.
Provide exactly three sections, formatted using markdown headers (###):
### Meeting Overview
(A brief summary of the meeting)
### Action Items
(A bulleted list of tasks, who is assigned, and any deadlines mentioned)
### Meeting Analytics
(List each speaker and an approximate estimate of their speaking time percentage based on their total transcript lines)

Transcript:
${textBlocks}`;

      let aiSummary = "";
      
      if (process.env.GROQ_API_KEY) {
        // Use Groq Cloud
        const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${process.env.GROQ_API_KEY}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            model: 'llama3-8b-8192',
            messages: [{ role: 'user', content: prompt }]
          })
        });

        if (!response.ok) {
           const errText = await response.text();
           throw new Error(`Groq API Error: ${errText}`);
        }
        
        const result = await response.json();
        aiSummary = result.choices[0].message.content;
      } else {
        // Fallback to local Ollama
        const response = await fetch('http://127.0.0.1:11434/api/generate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'llama3',
            prompt: prompt,
            stream: false
          })
        });

        if (!response.ok) throw new Error('Ollama API returned an error status.');
        
        const result = await response.json();
        aiSummary = result.response;
      }

      await saveSummary(roomId, aiSummary);
      callback({ summary: aiSummary });

    } catch (err) {
      console.error('Summary error:', err);
      // Fallback response if Ollama fails
      callback({ summary: `### Error\nFailed to generate summary. Please ensure your local Ollama server is running (llama3 model). \n\n**Details:** ${err.message}` });
    }
  });

  socket.on('get-attendance', async ({ roomId }, callback) => {
    try {
      const attendance = await getAttendance(roomId);
      callback({ success: true, attendance });
    } catch (err) {
      console.error(err);
      callback({ success: false, error: err.message });
    }
  });

  socket.on('search-meetings', async ({ query }, callback) => {
    try {
      const results = await searchTranscripts(query);
      callback({ success: true, results });
    } catch (err) {
      console.error(err);
      callback({ success: false, error: err.message });
    }
  });

  socket.on('disconnecting', () => {
    const joined = Array.from(socket.rooms).filter((r) => r !== socket.id);
    for (const roomId of joined) {
      const roomObj = rooms.get(roomId);
      if (roomObj) {
        roomObj.members.delete(socket.id);
        
        // If the host leaves, for now we don't transfer host. 
        // We could pick the next member, but we'll leave it as is.
        
        socket.to(roomId).emit('peer-left', { peerId: socket.id });
        if (roomObj.members.size === 0) rooms.delete(roomId);
      }
      recordLeave(roomId, socket.id).catch(console.error);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
