/* eslint-disable import/no-unresolved */
import { runTransaction } from 'firebase/firestore';

(() => {

  class SecureChatApp {
    constructor() {
      this.db = null;
      this.storage = null;
      this.currentUser = null;
      this.activeConnection = null;
      this.connections = [];
      this.sharedKeyCache = {}; // connId -> CryptoKey
      this.me = null; // cached profile
      this.usernameCache = new Map(); // uid -> {username, avatarUrl}
      this.userUnsubs = new Map(); // uid -> unsubscribe
      this._activePCs = new Map(); // peerUid -> {pc, unsubs, stream, videoEl}
      this._roomUnsub = null;
      this._peersUnsub = null;
      this._roomState = null;
      this._peersPresence = {};
      this._lastSpeech = new Map(); // uid -> timestamp
      this._inactTimer = null;
      this._videoEnabled = false;
      this._micEnabled = true;
      this._monitoring = false;
      this._monitorStream = null;
      this._inRoom = false;
      this._startingCall = false;
      this._joiningCall = false;
      this._cleanupIdleTimer = null;
      this._lastJoinedCallId = null;
      this._connectingCid = null;
      this._activeCid = null;
      this._joinRetryTimer = null;
      this._forceRelay = true; // Debug: force TURN-only until stable
      this._pcWatchdogs = new Map(); // key: callId:peerUid -> {t1,t2}
      this.init();
    }

    computeConnKey(uids){
      try{ return (uids||[]).slice().sort().join('|'); }catch(_){ return ''; }
    }

    getConnParticipants(data){
      const parts = Array.isArray(data?.participants)
        ? data.participants
        : (Array.isArray(data?.users) ? data.users : (Array.isArray(data?.memberIds) ? data.memberIds : []));
      if (parts.length) return parts.filter(Boolean);
      if (typeof data?.key === 'string' && data.key.includes('|')) return data.key.split('|').filter(Boolean);
      return [];
    }

    async findConnectionByKey(key){
      try{
        const q = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('key','==', key));
        const s = await firebase.getDocs(q);
        const rows = [];
        s.forEach(d=> rows.push({ id:d.id, ...d.data() }));
        rows.sort((a,b)=>{
          const aa = !!a.archived; const bb = !!b.archived;
          if (aa !== bb) return aa ? 1 : -1;
          return new Date(b.updatedAt||0) - new Date(a.updatedAt||0);
        });
        return rows[0]?.id || null;
      }catch(_){ return null; }
    }

    async getIceServers(){
      try{
        // 1) Try static TURN from keys
        let regionPref = 'europe-west1';
        if (window.secureKeyManager && typeof window.secureKeyManager.getKeys === 'function'){
          const keys = await window.secureKeyManager.getKeys();
          regionPref = (keys && keys.firebase && (keys.firebase.functionsRegion || keys.firebase.region)) || regionPref;
          const turn = keys && keys.turn;
          if (turn && Array.isArray(turn.uris) && turn.username && turn.credential){
            return [
              { urls: ['stun:stun.l.google.com:19302','stun:global.stun.twilio.com:3478'] },
              { urls: turn.uris, username: turn.username, credential: turn.credential }
            ];
          }
        }
        // 2) Else fetch ephemeral TURN via Cloud Function
        if (window.firebaseService && window.firebaseService.auth && window.firebaseService.auth.currentUser){
          const idToken = await window.firebaseService.auth.currentUser.getIdToken(true);
          // Prefer explicit run.app URL if provided in keys or known
          let runAppUrl = null;
          try{ const keys = await window.secureKeyManager.getKeys(); runAppUrl = keys && (keys.turnFunctionUrl || keys.turn?.functionUrl) || null; }catch(_){ runAppUrl = null; }
          const knownRunHost = 'https://getturnconfig-hkhtxasofa-ew.a.run.app';
          const regions = [regionPref, 'europe-west1', 'us-central1'];
          const candidates = [];
          if (runAppUrl) candidates.push(runAppUrl);
          candidates.push(knownRunHost);
          regions.forEach(r=> candidates.push(`https://${r}-liber-apps-cca20.cloudfunctions.net/getTurnConfig`));
          for (const url of candidates){
            try{
              const resp = await fetch(url, { headers: { 'Authorization': `Bearer ${idToken}` }});
              if (resp.ok){
                const json = await resp.json();
                if (Array.isArray(json.iceServers) && json.iceServers.length){
                  // Prefer TCP/TLS relays first for restrictive networks
                  const expanded = [];
                  const normalizeTcp = (u)=>{
                    if (typeof u !== 'string') return u;
                    if (!u.startsWith('turn')) return u;
                    // Replace any existing transport parameter with tcp
                    const hasQ = u.includes('?');
                    const base = hasQ ? u.replace(/([?&])transport=(udp|tcp)/i, '$1transport=tcp') : u + '?transport=tcp';
                    // Ensure only one transport param exists
                    const parts = base.split('?');
                    if (parts.length>1){
                      const q = parts[1]
                        .split('&')
                        .filter(kv => !/^transport=(udp|tcp)$/i.test(kv))
                        .concat(['transport=tcp'])
                        .join('&');
                      return parts[0] + '?' + q;
                    }
                    return base;
                  };
                  json.iceServers.forEach(s => {
                    const urls = Array.isArray(s.urls) ? s.urls : (s.urls ? [s.urls] : []);
                    const tcpUrls = urls.map(normalizeTcp);
                    const dedup = Array.from(new Set([ ...urls, ...tcpUrls ]));
                    expanded.push({ ...s, urls: dedup });
                  });
                  console.log('TURN from', url);
                  return expanded;
                }
              }
            }catch(_){ /* try next region */ }
          }
        }
      }catch(_){ /* ignore */ }
      return [ { urls: ['stun:stun.l.google.com:19302','stun:global.stun.twilio.com:3478'] } ];
    }

    async init() {
      // Wait for firebase
      let attempts = 0; while((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < 150){ await new Promise(r=>setTimeout(r,100)); attempts++; }
      if (!window.firebaseService || !window.firebaseService.isInitialized) return;
      this.db = window.firebaseService.db;
      this.storage = window.firebaseService.app ? firebase.getStorage(window.firebaseService.app) : null;
      
      // Enhanced auth readiness with token refresh
      attempts = 0;
      while (attempts < 50) {
        try {
          await window.firebaseService.auth.currentUser?.getIdToken(true); // Force refresh token
          this.currentUser = await window.firebaseService.getCurrentUser();
          if (this.currentUser) break;
        } catch (err) {
          console.warn('Auth retry attempt', attempts, err.message);
        }
        await new Promise(r => setTimeout(r, 100));
        attempts++;
      }
      if (!this.currentUser) {
        console.error('Firebase auth not ready after retries');
        return;
      }

      try { this.me = await window.firebaseService.getUserData(this.currentUser.uid); } catch { this.me = null; }
      this.bindUI();
      // If a connId is provided via query param, set active after connections load
      try{
        const params = new URLSearchParams(location.search);
        this._deepLinkConnId = params.get('connId') || null;
      }catch(_){ this._deepLinkConnId = null; }
      await this.loadConnections();
      // If deep link provided, activate it after list is ready (supports key or doc id)
      if (this._deepLinkConnId){
        try{
          let id = this._deepLinkConnId;
          // If this appears to be a participant key (contains a pipe), resolve to existing doc id
          if (id.includes('|')){
            const existing = await this.findConnectionByKey(id);
            if (existing) id = existing; else {
              // If not found, fall back to using the key directly if a doc with same id exists
              try{ const test = await firebase.getDoc(firebase.doc(this.db,'chatConnections', id)); if (!test.exists()) id = null; }catch(_){ id = null; }
            }
          }
          if (id) this.setActive(id);
        }catch(_){ /* ignore deep link issues */ }
      }

      // Add debug method to check Firebase config
      await this.debugFirebaseConfig();

      // Ensure self is cached
      this.usernameCache.set(this.currentUser.uid, { username: this.me?.username || 'You', avatarUrl: this.me?.avatarUrl || '../../images/default-bird.png' });
    }

    bindUI(){
      document.getElementById('new-connection-btn').addEventListener('click', ()=> { this.groupBaseParticipants = null; this.promptNewConnection(); });
      const actionBtn = document.getElementById('action-btn');
      if (actionBtn){
        actionBtn.addEventListener('click', ()=> this.handleActionButton());
        actionBtn.addEventListener('mousedown', (e)=> this.handleActionPressStart(e));
      }
      actionBtn.addEventListener('touchstart', (e)=> this.handleActionPressStart(e));
      ['mouseup','mouseleave','touchend','touchcancel'].forEach(evt=> actionBtn.addEventListener(evt, ()=> this.handleActionPressEnd()));
      document.getElementById('attach-btn').addEventListener('click', ()=> document.getElementById('file-input').click());
      document.getElementById('file-input').addEventListener('change', (e)=> this.sendFiles(e.target.files));
      const stickerBtn = document.getElementById('sticker-btn');
      if (stickerBtn){ stickerBtn.addEventListener('click', ()=> this.toggleStickers()); }
      document.getElementById('user-search').addEventListener('input', (e)=> this.searchUsers(e.target.value.trim()));
      // Predictive suggestions: update datalist as user types
      const suggest = document.getElementById('user-suggestions');
      const searchEl = document.getElementById('user-search');
      if (suggest && searchEl){
        let lastTerm = '';
        searchEl.addEventListener('input', async ()=>{
          const term = (searchEl.value||'').trim();
          if (term === lastTerm || term.length === 0){ if (term.length===0) suggest.innerHTML=''; lastTerm = term; return; }
          lastTerm = term;
          try{
            const users = await window.firebaseService.searchUsers(term.toLowerCase());
            // simple dedupe and top-10 ranking by prefix/contains
            const rank = (u)=>{
              const n=(u.username||'').toLowerCase(); const em=(u.email||'').toLowerCase(); const t=term.toLowerCase();
              let s=0; if (n.startsWith(t)||em.startsWith(t)) s+=3; if (n.includes(t)||em.includes(t)) s+=2; return s;
            };
            const opts = (users||[])
              .sort((a,b)=> rank(b)-rank(a))
              .slice(0,10)
              .map(u=> `<option value="${u.username||u.email}"></option>`)
              .join('');
            suggest.innerHTML = opts;
          }catch(_){ /* ignore */ }
        });
      }
      const userSearch = document.getElementById('user-search');
      if (userSearch){
        const openSidebar = ()=>{ const sidebar = document.querySelector('.sidebar'); if (sidebar && !sidebar.classList.contains('open')) sidebar.classList.add('open'); };
        userSearch.addEventListener('focus', openSidebar);
        userSearch.addEventListener('click', openSidebar);
        // iOS Safari sometimes needs a short delay to compute layout; force open on input with rAF
        userSearch.addEventListener('input', ()=>{ requestAnimationFrame(()=> openSidebar()); });
        userSearch.addEventListener('keydown', (e)=>{ if(e.key==='Escape'){ const s=document.querySelector('.sidebar'); if(s) s.classList.remove('open'); }});
        // Prevent header container from swallowing the tap
        const header = document.getElementById('sidebar-header');
        if (header){
          header.style.pointerEvents = 'auto';
          userSearch.style.pointerEvents = 'auto';
        }
      }

      // Register service worker once (best-effort)
      if ('serviceWorker' in navigator){
        navigator.serviceWorker.register('/sw.js').catch(()=>{});
      }
      // Call and recording buttons (placeholders)
      const voiceBtn = document.getElementById('voice-call-btn'); if (voiceBtn) voiceBtn.addEventListener('click', ()=> this.enterRoom(false));
      const videoBtn = document.getElementById('video-call-btn'); if (videoBtn) videoBtn.addEventListener('click', ()=> this.enterRoom(true));
      const groupBtn = document.getElementById('group-menu-btn'); if (groupBtn) groupBtn.addEventListener('click', ()=> this.toggleGroupPanel());
      const fixDupBtn = document.getElementById('fix-duplicates-btn'); if (fixDupBtn) fixDupBtn.addEventListener('click', ()=> this.fixDuplicateConnections());
      const recAudioBtn = document.getElementById('record-audio-btn'); if (recAudioBtn) recAudioBtn.addEventListener('click', ()=> this.recordVoiceMessage());
      const recVideoBtn = document.getElementById('record-video-btn'); if (recVideoBtn) recVideoBtn.addEventListener('click', ()=> this.recordVideoMessage());
      // Drag & Drop upload within chat app area
      const appEl = document.getElementById('chat-app');
      if (appEl){
        ['dragenter','dragover'].forEach(evt=> appEl.addEventListener(evt, (e)=>{
          e.preventDefault();
          if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
        }));
        appEl.addEventListener('drop', (e)=>{
          e.preventDefault();
          const dt = e.dataTransfer;
          if (!dt) return;
          const files = dt.files && dt.files.length ? dt.files : null;
          if (files && files.length) this.sendFiles(files);
        });
      }
      // Paste-to-upload on message input (supports images/files from clipboard)
      const msgInput = document.getElementById('message-input');
      if (msgInput){
        msgInput.addEventListener('paste', async (e)=>{
          const cd = e.clipboardData;
          if (!cd) return;
          const files = [];
          if (cd.files && cd.files.length){
            for (let i=0;i<cd.files.length;i++) files.push(cd.files[i]);
          } else if (cd.items && cd.items.length){
            for (let i=0;i<cd.items.length;i++){
              const it = cd.items[i];
              if (it && it.kind === 'file'){
                const f = it.getAsFile();
                if (f) files.push(f);
              }
            }
          }
          if (files.length){
            e.preventDefault();
            await this.sendFiles(files);
          }
        });
      }
      const sidebarHeader = document.getElementById('sidebar-header');
      if (sidebarHeader){
        sidebarHeader.addEventListener('click', (ev)=>{
          // Avoid toggling when interacting with controls inside header
          const tgt = ev.target;
          if (tgt && (tgt.tagName === 'A' || tgt.tagName === 'BUTTON' || tgt.closest('button') || tgt.closest('a'))) return;
          const sidebar = document.querySelector('.sidebar');
          if (!sidebar) return;
          if (sidebar.classList.contains('open')) sidebar.classList.remove('open'); else sidebar.classList.add('open');
        });
      }
      // Enter to send, Shift+Enter for newline (desktop & mobile)
      const msgInput2 = document.getElementById('message-input');
      if (msgInput2){
        msgInput2.addEventListener('input', ()=> this.refreshActionButton());
        msgInput2.addEventListener('keydown', (e)=>{
          if (e.key === 'Enter'){
            if (e.shiftKey){
              // allow newline
              return;
            }
            e.preventDefault();
            this.sendCurrent();
          }
        });
      }
      document.addEventListener('keydown', (e)=>{
        if (e.key === 'Escape'){
          const sidebar = document.querySelector('.sidebar');
          if (sidebar && sidebar.classList.contains('open')){
            sidebar.classList.remove('open');
          }
        }
      });
    }

    async fixDuplicateConnections(){
      try{
        const byId = new Map();
        const fields = ['participants', 'users', 'memberIds'];
        for (const field of fields){
          try{
            const q = firebase.query(
              firebase.collection(this.db,'chatConnections'),
              firebase.where(field,'array-contains', this.currentUser.uid)
            );
            const s = await firebase.getDocs(q);
            s.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
          }catch(_){ }
        }
        // Include key-only docs that may not have participant arrays.
        try{
          const qAll = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.orderBy('updatedAt','desc'), firebase.limit(400));
          const sAll = await firebase.getDocs(qAll);
          sAll.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
        }catch(_){
          try{
            const sAll2 = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
            sAll2.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
          }catch(__){ }
        }
        const all = Array.from(byId.values())
          .map(c=> ({ ...c, participants: this.getConnParticipants(c) }))
          .filter(c=> Array.isArray(c.participants) && c.participants.includes(this.currentUser.uid));
        // Group by stable key of participants
        const groups = new Map();
        for (const c of all){
          const key = c.key || this.computeConnKey(c.participants||[]);
          if (!groups.has(key)) groups.set(key, []);
          groups.get(key).push(c);
        }
        let archived = 0;
        for (const [key, conns] of groups.entries()){
          if (conns.length <= 1) continue;
          // Keep the newest; mark others archived → no message copying to respect rules
          conns.sort((a,b)=> new Date(b.updatedAt||0) - new Date(a.updatedAt||0));
          const keep = conns[0];
          const rest = conns.slice(1);
          for (const r of rest){
            try{
              await firebase.updateDoc(firebase.doc(this.db,'chatConnections', r.id),{
                archived: true,
                mergedInto: keep.id,
                key: key,
                updatedAt: new Date().toISOString()
              });
              archived++;
            }catch(err){ console.warn('Archive duplicate failed', r.id, err); }
          }
          // Normalize keep doc fields and ensure key present
          try{
            await firebase.updateDoc(firebase.doc(this.db,'chatConnections', keep.id),{
              key,
              participants: keep.participants || [],
              updatedAt: new Date().toISOString()
            });
          }catch(_){ }
        }
        alert(archived>0 ? `Archived ${archived} duplicate chats.` : 'No duplicates found.');
        await this.loadConnections();
      }catch(e){ console.error('Fix duplicates failed:', e); /* no alert */ }
    }

    refreshActionButton(){
      const input = document.getElementById('message-input');
      const review = document.getElementById('recording-review');
      const actionBtn = document.getElementById('action-btn');
      const inReview = review && !review.classList.contains('hidden');
      const hasContent = !!(input && input.value.trim().length);
      if (hasContent){
        actionBtn.title = 'Send';
        actionBtn.innerHTML = '<i class="fas fa-arrow-up"></i>';
        actionBtn.style.background = '#2563eb';
        actionBtn.style.borderRadius = '12px';
        actionBtn.style.color = '#fff';
      } else if (inReview){
        actionBtn.title = 'Send';
        actionBtn.innerHTML = '<i class="fas fa-paper-plane"></i>';
        actionBtn.style.background = '#2563eb';
        actionBtn.style.borderRadius = '12px';
        actionBtn.style.color = '#fff';
      } else {
        actionBtn.title = 'Voice message';
        actionBtn.innerHTML = '<i class="fas fa-microphone"></i>';
        actionBtn.style.background = 'transparent';
        actionBtn.style.color = '';
      }
    }

    handleActionButton(){
      const input = document.getElementById('message-input');
      const review = document.getElementById('recording-review');
      if (review && !review.classList.contains('hidden')){
        const sendBtn = document.getElementById('send-recording-btn');
        if (sendBtn){ sendBtn.click(); return; }
      }
      if (input && input.value.trim().length){
        this.sendCurrent();
      } else {
        // short click toggles to video icon then back to mic
        const btn = document.getElementById('action-btn');
        if (!this._lastActionToggle || (Date.now() - this._lastActionToggle) > 1200){
          btn.innerHTML = '<i class="fas fa-video"></i>';
          btn.title = 'Video message';
          this._lastActionToggle = Date.now();
          setTimeout(()=> this.refreshActionButton(), 1200);
        } else {
          this.refreshActionButton();
        }
      }
    }

    handleActionPressStart(e){
      const input = document.getElementById('message-input');
      if (input && input.value.trim().length) return; // only record when empty
      const indicator = document.getElementById('recording-indicator');
      this._pressTimer = setTimeout(async()=>{
        try{
          if (indicator) { indicator.classList.remove('hidden'); indicator.querySelector('i').className = 'fas fa-microphone'; }
          await this.recordVoiceMessage();
        }catch(err){ alert('Recording failed'); }
        finally{ if (indicator) indicator.classList.add('hidden'); }
      }, 400);
    }

    handleActionPressEnd(){
      if (this._pressTimer){ clearTimeout(this._pressTimer); this._pressTimer = null; }
      try{ if (this._recStop) this._recStop(); }catch(_){ }
      const indicator = document.getElementById('recording-indicator'); if (indicator) indicator.classList.add('hidden');
      this.refreshActionButton();
    }

    async promptNewConnection(){
      // Enter group selection mode
      this.isGroupMode = true;
      this.groupSelection = new Map(); // uid -> {uid, username, email}
      const panel = document.getElementById('group-builder');
      const chips = document.getElementById('group-selected');
      const createBtn = document.getElementById('create-group-btn');
      if (panel) panel.style.display = 'block';
      if (chips) chips.innerHTML = '';
      if (createBtn){
        createBtn.onclick = async ()=>{
          const members = Array.from(this.groupSelection.values());
          if (members.length === 0){ this.isGroupMode = false; if (panel) panel.style.display='none'; return; }
          let baseUids = Array.isArray(this.groupBaseParticipants)? this.groupBaseParticipants.slice() : [this.currentUser.uid];
          const addUids = members.map(m=> m.uid||m.id);
          const participantUids = Array.from(new Set([...baseUids, ...addUids]));
          const myName = (this.me&&this.me.username) || (this.currentUser.email||'me');
          const nameMap = new Map();
          nameMap.set(this.currentUser.uid, myName);
          members.forEach(m=> nameMap.set(m.uid||m.id, m.username||m.email));
          const participantNames = participantUids.map(uid=> nameMap.get(uid) || uid);
          const key = this.computeConnKey(participantUids);
          let connId = await this.findConnectionByKey(key);
          if (!connId){
            try{
              const stableRef = firebase.doc(this.db,'chatConnections', key);
              await firebase.setDoc(stableRef,{
                id: key,
                key,
                participants: participantUids,
                participantUsernames: participantNames,
                admins: [this.currentUser.uid],
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                lastMessage:''
              }, { merge:false });
              connId = key;
            }catch(errStable){
              const newRef = firebase.doc(firebase.collection(this.db,'chatConnections'));
              connId = newRef.id;
              await firebase.setDoc(newRef,{
                id: connId,
                key,
                participants: participantUids,
                participantUsernames: participantNames,
                admins: [this.currentUser.uid],
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                lastMessage:''
              });
            }
          }
          this.isGroupMode = false;
          if (panel) panel.style.display='none';
          await this.loadConnections();
          this.setActive(connId, participantNames.filter(n=> n !== ((this.me&&this.me.username)||this.currentUser.email)).join(', '));
        };
      }
      // Ensure panel is open on mobile
      const sidebar = document.querySelector('.sidebar');
      if (sidebar && !sidebar.classList.contains('open')) sidebar.classList.add('open');
      const search = document.getElementById('user-search'); if (search) search.focus();
    }

    async loadConnections(){
      const listEl = document.getElementById('connections-list');
      listEl.innerHTML = '';
      let permissionDenied = false;
      try{
        const fields = ['participants', 'users', 'memberIds'];
        const byId = new Map();
        for (const field of fields){
          try{
            const q = firebase.query(
              firebase.collection(this.db,'chatConnections'),
              firebase.where(field,'array-contains', this.currentUser.uid),
              firebase.orderBy('updatedAt','desc')
            );
            const s = await firebase.getDocs(q);
            s.forEach(d=> byId.set(d.id, { id: d.id, ...d.data() }));
          }catch(_){
            try{
              const q2 = firebase.query(
                firebase.collection(this.db,'chatConnections'),
                firebase.where(field,'array-contains', this.currentUser.uid)
              );
              const s2 = await firebase.getDocs(q2);
              s2.forEach(d=> byId.set(d.id, { id: d.id, ...d.data() }));
            }catch(e2){
              if (e2 && e2.code === 'permission-denied') permissionDenied = true;
            }
          }
        }
        const temp = Array.from(byId.values());
        temp.forEach((c)=>{
          const fallbackParts = this.getConnParticipants(c);
          if (!Array.isArray(c.participants) && fallbackParts.length) c.participants = fallbackParts;
        });
        temp.sort((a,b)=> new Date(b.updatedAt||0) - new Date(a.updatedAt||0));
        this.connections = temp;
      } catch (e) {
        if (e && e.code === 'permission-denied') permissionDenied = true;
        this.connections = [];
      }
      if (permissionDenied && this.connections.length === 0){
        listEl.innerHTML = '<li style="opacity:.8">No access to chat connections. Please redeploy Firestore rules and reload.</li>';
      }
      const seen = new Set();
      // Backfill participantUsernames if missing
      for (const c of this.connections){
        try{
          const parts = Array.isArray(c.participants)? c.participants:[];
          const names = Array.isArray(c.participantUsernames)? c.participantUsernames:[];
          if (parts.length && names.length !== parts.length){
            const enriched = [];
            for (const uid of parts){
              if (uid === this.currentUser.uid){ enriched.push((this.me&&this.me.username)||this.currentUser.email||'me'); continue; }
              let cached = this.usernameCache.get(uid);
              if (!cached) {
                try {
                  const u = await window.firebaseService.getUserData(uid);
                  cached = { username: (u && u.username) || u?.email || 'User ' + uid.slice(0,6), avatarUrl: u?.avatarUrl || '../../images/default-bird.png' };
                  this.usernameCache.set(uid, cached);
                } catch (err) {
                  console.error('Failed to resolve user for', uid, err);
                  cached = { username: 'Unknown', avatarUrl: '../../images/default-bird.png' };
                }
              }
              enriched.push(cached.username);
            }
            await firebase.updateDoc(firebase.doc(this.db,'chatConnections', c.id),{ participantUsernames: enriched, updatedAt: new Date().toISOString() });
            c.participantUsernames = enriched;
          }
        }catch(_){ }
      }
      // Clear list before re-rendering
      listEl.innerHTML = '';
      this.connections.forEach(c=>{
        const key = c.key || this.computeConnKey(c.participants||[]);
        if (seen.has(key)) return; seen.add(key);
        const li = document.createElement('li');
        li.setAttribute('data-id', c.id);
        let label = 'Chat';
        const myNameLower = ((this.me && this.me.username) || '').toLowerCase();
        if (Array.isArray(c.participantUsernames) && c.participantUsernames.length){
          const others = c.participantUsernames.filter(n=> String(n ?? '').toLowerCase() !== myNameLower);
          if (others.length===1) label = others[0];
          else if (others.length>1){
            label = others.slice(0,2).join(', ');
            if (others.length>2) label += `, +${others.length-2}`;
          } else {
            label = 'Chat';
          }
        } else if (Array.isArray(c.participants) && c.participants.length) {
          const others = c.participants.filter(u => u !== this.currentUser.uid);
          label = others.length === 1 ? `Chat with ${others[0].slice(0,8)}` : `Group Chat (${others.length})`;
        } else {
          label = 'Chat';
        }
        li.textContent = label; // Initial set, async will update
        // Admin badge in header when active
        li.addEventListener('mouseenter', ()=> li.classList.add('active-hover'));
        li.addEventListener('click',()=>{
          this.setActive(c.id);
          const sidebar = document.querySelector('.sidebar');
          if (sidebar) sidebar.classList.remove('open');
          setTimeout(async()=>{
            try{
              const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', c.id));
              const data = snap.exists()? snap.data():null;
              const admins = Array.isArray(data?.admins)? data.admins: [];
              const hdr = document.querySelector('.chat-header h3');
              const titleBar = document.getElementById('active-connection-name');
              if (titleBar){
                const badgeId = 'admin-badge';
                let badge = document.getElementById(badgeId);
                if (!badge){
                  badge = document.createElement('span'); badge.id = badgeId; badge.style.marginLeft='8px'; badge.style.fontSize='12px'; badge.style.opacity='.8';
                  titleBar.parentElement && titleBar.parentElement.appendChild(badge);
                }
                badge.textContent = admins.includes(this.currentUser.uid) ? '(Admin)' : '';
              }
            }catch(_){ }
          }, 50);
        });
        listEl.appendChild(li);
      });
      // Subscribe to participant username changes for live label updates
      try{
        const uidSet = new Set();
        this.connections.forEach(c => (Array.isArray(c.participants)?c.participants:[]).forEach(u=>uidSet.add(u)));
        const uids = Array.from(uidSet);
        // Create listeners
        uids.forEach(uid => {
          if (this.userUnsubs && this.userUnsubs.has(uid)) return;
          try{
            const ref = firebase.doc(this.db, 'users', uid);
            const unsub = firebase.onSnapshot(ref, (snap)=>{
              if (!snap.exists()) return;
              const d = snap.data() || {};
              const name = d.username || d.email || uid;
              const prev = this.usernameCache.get(uid);
              if (prev !== name){
                this.usernameCache.set(uid, name);
                // Refresh connection list labels and active header
                try{
                  const listEl = document.getElementById('connections-list');
                  if (listEl){
                    const myNameLower = (this.me?.username || '').toLowerCase();
                    listEl.querySelectorAll('li').forEach(li => {
                      const id = li.getAttribute('data-id');
                      const c = this.connections.find(x => x.id === id);
                      if (!c) return;
                      const parts = Array.isArray(c.participants)?c.participants:[];
                      const stored = Array.isArray(c.participantUsernames)?c.participantUsernames:[];
                      const names = parts.map((p,i)=> this.usernameCache.get(p) || stored[i] || p);
                      const others = names.filter(n => String(n ?? '').toLowerCase() !== myNameLower);
                      const label = others.length===1? others[0] : (others.slice(0,2).join(', ')+(others.length>2?`, +${others.length-2}`:''));
                      li.textContent = label || 'Chat';
                      li.setAttribute('data-id', c.id);
                    });
                  }
                  if (this.activeConnection) this.setActive(this.activeConnection);
                }catch(_){ }
              }
            });
            if (this.userUnsubs) this.userUnsubs.set(uid, unsub);
          }catch(_){ }
        });
      }catch(_){ }
      // No recursive call
    }

    async setActive(connId, displayName){
      this.activeConnection = connId;
      // Resolve displayName if not provided
      if (!displayName) {
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        if (snap.exists()) {
          const data = snap.data();
          const parts = Array.isArray(data.participants)
            ? data.participants
            : (Array.isArray(data.users) ? data.users : (Array.isArray(data.memberIds) ? data.memberIds : []));
          // Prefer live cache, fallback to stored usernames
          const stored = Array.isArray(data.participantUsernames) ? data.participantUsernames : [];
          const names = parts.map((uid, i)=> this.usernameCache.get(uid) || stored[i] || uid);
          const myNameLower = (this.me?.username || '').toLowerCase();
          const others = names.filter(n => String(n ?? '').toLowerCase() !== myNameLower);
          displayName = others.length === 1 ? others[0] : (others.slice(0,2).join(', ') + (others.length > 2 ? `, +${others.length-2}` : ''));
        }
        displayName = displayName || 'Chat';
      }
      document.getElementById('active-connection-name').textContent = displayName;
      await this.loadMessages();
      // If current user is not a participant of this connection, show banner to recreate with same users
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        if (snap.exists()){
          const data = snap.data();
          const parts = Array.isArray(data.participants)
            ? data.participants
            : (Array.isArray(data.users) ? data.users : (Array.isArray(data.memberIds) ? data.memberIds : []));
          const header = document.querySelector('.chat-header');
          const existing = document.getElementById('chat-access-banner');
          if (!parts.includes(this.currentUser.uid)){
            if (!existing && header){
              const bar = document.createElement('div');
              bar.id='chat-access-banner';
              bar.style.cssText='background:#2a2f36;color:#fff;padding:8px 12px;border-bottom:1px solid #3a404a;display:flex;gap:10px;align-items:center';
              const msg = document.createElement('div'); msg.textContent='You are not a participant of this chat. Recreate a new chat with the same users to start messaging.'; bar.appendChild(msg);
              const btn = document.createElement('button'); btn.className='btn btn-secondary'; btn.textContent='Recreate chat';
              btn.onclick = async ()=>{
                try{
                  const participants = parts.slice(); const names = (data.participantUsernames||[]).slice();
                  if (!participants.includes(this.currentUser.uid)){ participants.push(this.currentUser.uid); names.push((this.me&&this.me.username)||this.currentUser.email||'me'); }
                  const newKey = this.computeConnKey(participants);
                  let newId = await this.findConnectionByKey(newKey);
                  if (!newId){
                    const ref = firebase.doc(this.db,'chatConnections', newKey);
                    await firebase.setDoc(ref,{ id:newKey, key:newKey, participants, participantUsernames:names, admins:[this.currentUser.uid], createdAt:new Date().toISOString(), updatedAt:new Date().toISOString(), lastMessage:'' });
                    newId = newKey;
                  }
                  await this.loadConnections(); this.setActive(newId);
                }catch(_){ }
              };
              bar.appendChild(btn);
              header.parentNode.insertBefore(bar, header.nextSibling);
            }
          } else if (existing){ existing.remove(); }
        }
      }catch(_){ }
      // refresh group panel if open
      const gp = document.getElementById('group-panel'); if (gp){ await this.renderGroupPanel(); }
    }

    async loadMessages(){
      const box = document.getElementById('messages');
      box.innerHTML='';
      if (!this.activeConnection) return;
      try{
        if (this._unsubMessages) { this._unsubMessages(); this._unsubMessages = null; }
        let q;
        try{
          q = firebase.query(
            firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'),
            firebase.orderBy('createdAtTS','asc'),
            firebase.limit(500)
          );
        }catch(_){
          q = firebase.query(
            firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'),
            firebase.orderBy('createdAt','asc'),
            firebase.limit(500)
          );
        }
        // Also include archived/merged message histories
        const archivedConnIds = await this.getArchivedConnIds(this.activeConnection);
        const handleSnap = async (snap)=>{
          const prevTop = box.scrollTop;
          const wasNearBottom = (box.scrollHeight - box.scrollTop - box.clientHeight) < 100;
          box.innerHTML='';
          let aesKey = await this.getFallbackKey();
          const renderOne = async (d)=>{
            const m=d.data();
            let text='';
            try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey);}catch{
              try {
                const peerUid = await this.getPeerUid();
                const legacy = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, this.activeConnection);
                text = await chatCrypto.decryptWithKey(m.cipher, legacy);
              } catch {
                try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch { text='[unable to decrypt]'; }
              }
            }
            const el = document.createElement('div');
            el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
            // Resolve sender name async
            let senderName = m.sender === this.currentUser.uid ? 'You' : this.usernameCache.get(m.sender) || m.sender.slice(0,8);
            if (!this.usernameCache.has(m.sender)) {
              try {
                const user = await window.firebaseService.getUserData(m.sender);
                senderName = (user?.username || user?.email || m.sender.slice(0,8));
                this.usernameCache.set(m.sender, senderName);
              } catch (err) {
                console.error('Sender name resolution failed:', err);
                senderName = 'Unknown';
              }
            }
            const hasFile = !!m.fileUrl && !!m.fileName;
            const fileText = hasFile ? `Attachment from ${senderName}` : '';
            // Render call invites as buttons
            let bodyHtml = this.renderText(text);
            const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
            if (callMatch){
              const kind = callMatch[1]; const callId = callMatch[2];
              const btnLabel = kind==='voice' ? 'Join voice call' : 'Join video call';
              bodyHtml = `<button class=\"btn secondary\" data-call-id=\"${callId}\" data-kind=\"${kind}\">${btnLabel}</button>`;
            }
            const canModify = m.sender === this.currentUser.uid;
            el.innerHTML = `<div class=\"msg-text\">${bodyHtml}</div>${hasFile?`<div class=\"file-link\"><a href=\"${m.fileUrl}\" target=\"_blank\" rel=\"noopener noreferrer\">${fileText}</a></div><div class=\"file-preview\"></div>`:''}<div class=\"meta\">${senderName} · ${new Date(m.createdAt).toLocaleString()}${canModify?` · <span class=\"msg-actions\" data-mid=\"${m.id}\" style=\"cursor:pointer\"><i class=\"fas fa-edit\" title=\"Edit\"></i> <i class=\"fas fa-trash\" title=\"Delete\"></i> <i class=\"fas fa-paperclip\" title=\"Replace file\"></i></span>`:''}</div>`;
            box.appendChild(el);
            const joinBtn = el.querySelector('button[data-call-id]');
            if (joinBtn){ joinBtn.addEventListener('click', ()=> this.joinOrStartCall({ video: joinBtn.dataset.kind === 'video' })); }
            if (hasFile){
              const preview = el.querySelector('.file-preview');
              if (preview) this.renderEncryptedAttachment(preview, m.fileUrl, m.fileName, aesKey);
            }
            // Bind edit/delete/replace for own messages
            if (canModify){
              const actions = el.querySelector('.msg-actions');
              if (actions){
                const mid = actions.getAttribute('data-mid');
                const icons = actions.querySelectorAll('i');
                const editIcon = icons[0];
                const delIcon = icons[1];
                const repIcon = icons[2];
                editIcon.onclick = async ()=>{
                  const current = el.querySelector('.msg-text')?.textContent || '';
                  const next = prompt('Edit message:', current);
                  if (next===null) return;
                  const key = await this.getFallbackKey();
                  const cipher2 = await chatCrypto.encryptWithKey(next, key);
                  await firebase.updateDoc(firebase.doc(this.db,'chatMessages',this.activeConnection,'messages', mid),{ cipher: cipher2, updatedAt: new Date().toISOString() });
                };
                delIcon.onclick = async ()=>{
                  if (!confirm('Delete this message?')) return;
                  await firebase.deleteDoc(firebase.doc(this.db,'chatMessages',this.activeConnection,'messages', mid));
                };
                repIcon.onclick = async ()=>{
                  const picker = document.createElement('input'); picker.type='file'; picker.accept='*/*'; picker.style.display='none'; document.body.appendChild(picker);
                  picker.onchange = async ()=>{
                    try{
                      const f = picker.files[0]; if (!f) return;
                      const aesKey2 = await this.getFallbackKey();
                      const base64 = await new Promise((resolve, reject)=>{ const r = new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(f); });
                      const cipherF = await chatCrypto.encryptWithKey(base64, aesKey2);
                      const blob = new Blob([JSON.stringify(cipherF)], {type:'application/json'});
                      const sref = firebase.ref(this.storage, `chat/${this.activeConnection}/${Date.now()}_${f.name.replace(/[^a-zA-Z0-9._-]/g,'_')}.enc.json`);
                      await firebase.uploadBytes(sref, blob, { contentType: 'application/json' });
                      const url = await firebase.getDownloadURL(sref);
                      await firebase.updateDoc(firebase.doc(this.db,'chatMessages',this.activeConnection,'messages', mid),{ fileUrl:url, fileName:f.name, updatedAt: new Date().toISOString() });
                    }catch(_){ alert('Failed to replace file'); }
                    finally{ document.body.removeChild(picker); }
                  };
                  picker.click();
                };
              }
            }
          };
          // Render primary messages in deterministic order.
          for (const d of (snap.docs || [])) {
            await renderOne(d);
          }
          // Fetch and render archived chains (best-effort, no onSnapshot for archives to reduce listeners)
          for (const aid of archivedConnIds){
            try{
              let qa;
              try{
                qa = firebase.query(
                  firebase.collection(this.db,'chatMessages',aid,'messages'),
                  firebase.orderBy('createdAtTS','asc'),
                  firebase.limit(200)
                );
              }catch(_){
                qa = firebase.query(
                  firebase.collection(this.db,'chatMessages',aid,'messages'),
                  firebase.orderBy('createdAt','asc'),
                  firebase.limit(200)
                );
              }
              const s2 = await firebase.getDocs(qa);
              for (const d of (s2.docs || [])) {
                await renderOne(d);
              }
            }catch(_){ /* ignore per-archive failure */ }
          }
          // Keep chat pinned to newest message for smoother live conversation flow.
          box.scrollTop = box.scrollHeight;
        };
        if (firebase.onSnapshot){
          this._unsubMessages = firebase.onSnapshot(q, handleSnap);
        } else {
          this._msgPoll && clearInterval(this._msgPoll);
          this._msgPoll = setInterval(async()=>{ const s = await firebase.getDocs(q); handleSnap(s); }, 2500);
          const snap = await firebase.getDocs(q); handleSnap(snap);
        }
      }catch{
        try{
          const q = firebase.query(
            firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'),
            firebase.orderBy('createdAt','asc'),
            firebase.limit(200)
          );
          const snap = await firebase.getDocs(q);
          let aesKey = await this.getFallbackKey();
          snap.forEach(async d=>{
            const m=d.data();
            let text='';
            try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey);}catch{
              try {
                const peerUid = await this.getPeerUid();
                const legacy = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, this.activeConnection);
                text = await chatCrypto.decryptWithKey(m.cipher, legacy);
              } catch {
                try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch { text='[unable to decrypt]'; }
              }
            }
            const el = document.createElement('div');
            el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
            // Resolve sender name async
            let senderName = m.sender === this.currentUser.uid ? 'You' : this.usernameCache.get(m.sender) || m.sender.slice(0,8);
            if (!this.usernameCache.has(m.sender)) {
              try {
                const user = await window.firebaseService.getUserData(m.sender);
                senderName = (user?.username || user?.email || m.sender.slice(0,8));
                this.usernameCache.set(m.sender, senderName);
              } catch (err) {
                console.error('Sender name resolution failed:', err);
                senderName = 'Unknown';
              }
            }
            const hasFile = !!m.fileUrl && !!m.fileName;
            const fileText = hasFile ? `Attachment from ${senderName}` : '';
            // Render call invites as buttons
            let bodyHtml = this.renderText(text);
            const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
            if (callMatch){
              const kind = callMatch[1]; const callId = callMatch[2];
              const btnLabel = kind==='voice' ? 'Join voice call' : 'Join video call';
              bodyHtml = `<button class=\"btn secondary\" data-call-id=\"${callId}\" data-kind=\"${kind}\">${btnLabel}</button>`;
            }
            el.innerHTML = `<div class=\"msg-text\">${bodyHtml}</div>${hasFile?`<div class=\"file-link\"><a href=\"${m.fileUrl}\" target=\"_blank\" rel=\"noopener noreferrer\">${fileText}</a></div><div class=\"file-preview\"></div>`:''}<div class=\"meta\">${senderName} · ${new Date(m.createdAt).toLocaleString()}</div>`;
            box.appendChild(el);
            const joinBtn = el.querySelector('button[data-call-id]');
            if (joinBtn){ joinBtn.addEventListener('click', ()=> this.answerCall(joinBtn.dataset.callId, { video: joinBtn.dataset.kind === 'video' })); }
            if (hasFile){
              const preview = el.querySelector('.file-preview');
              if (preview) this.renderEncryptedAttachment(preview, m.fileUrl, m.fileName, aesKey);
            }
          });
          box.scrollTop = box.scrollHeight;
        }catch(e){
          console.error('Failed to load messages:', e);
          box.innerHTML = '<div class="error">Failed to load messages. Check console.</div>';
        }
      }
    }
    // New helper
    async getArchivedConnIds(connId){
      const q = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('mergedInto','==', connId));
      const s = await firebase.getDocs(q);
      return s.docs.map(d=> d.id);
    }

    renderText(t){ return t.replace(/</g,'&lt;'); }

    async sendCurrent(){
      const input = document.getElementById('message-input');
      const text = input.value.trim();
      if (!text || !this.activeConnection) return;
      await this.saveMessage({text});
      input.value='';
    }

    async sendFiles(files){
      if (!files || !files.length || !this.activeConnection) { console.warn('No files or no active connection'); return; }
      if (!this.storage) {
        alert('File upload is not available because Firebase Storage is not configured.');
        return;
      }
      try{
        const cRef = firebase.doc(this.db, 'chatConnections', this.activeConnection);
        const cSnap = await firebase.getDoc(cRef);
        const participants = cSnap.exists()
          ? (Array.isArray(cSnap.data().participants)
              ? cSnap.data().participants
              : (Array.isArray(cSnap.data().users)
                  ? cSnap.data().users
                  : (Array.isArray(cSnap.data().memberIds) ? cSnap.data().memberIds : [])))
          : [];
        if (!participants.includes(this.currentUser.uid)) {
          alert('You are not a participant of this chat. Please reopen the chat and try again.');
          return;
        }
      }catch(_){ /* best-effort pre-check */ }
      console.log('Auth state before sendFiles:', !!this.currentUser, firebase.auth().currentUser?.uid);
      try {
        await firebase.auth().currentUser?.getIdToken(true); // Force refresh
        if (!firebase.auth().currentUser) throw new Error('Auth lost - please re-login');
      } catch (err) {
        console.error('Auth refresh failed before sendFiles:', err);
        alert('Auth error - please reload and re-login');
        return;
      }
      for (const f of files){
        try {
          console.log('Sending file:', f.name);
          const aesKey = await this.getFallbackKey();
          // Read file as base64 via FileReader to avoid large argument spreads
          const base64 = await new Promise((resolve, reject)=>{
            try{
              const reader = new FileReader();
              reader.onload = ()=>{
                const result = String(reader.result || '');
                const b64 = result.includes(',') ? result.split(',')[1] : '';
                resolve(b64);
              };
              reader.onerror = reject;
              reader.readAsDataURL(f);
            }catch(e){ reject(e); }
          });
          // Encrypt base64 string
          const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
          // Store encrypted JSON payload with .json extension to aid CORS/content-type and preview
          const blob = new Blob([JSON.stringify(cipher)], {type:'application/json'});
          const safeName = f.name.replace(/[^a-zA-Z0-9._-]/g,'_');
          const r = firebase.ref(this.storage, `chat/${this.activeConnection}/${Date.now()}_${safeName}.enc.json`);
          console.log('File upload started');
          await firebase.uploadBytes(r, blob, { contentType: 'application/json' });
          const url = await firebase.getDownloadURL(r);
          console.log('File upload completed');
          await this.saveMessage({text:`[file] ${f.name}`, fileUrl:url, fileName:f.name});
        } catch (err) {
          console.error('Send file error details:', err.code, err.message, err);
          alert('Failed to send file: ' + err.message);
        }
      }
    }

    /* Stickerpacks */
    async toggleStickers(){
      const existing = document.getElementById('sticker-panel');
      if (existing){ existing.remove(); return; }
      const panel = document.createElement('div'); panel.id='sticker-panel'; panel.className='sticker-panel';
      panel.innerHTML = `
        <div class="panel-header">
          <strong>Stickers</strong>
          <div class="sticker-pack-actions">
            <button class="btn secondary" id="add-pack-btn">Add pack</button>
            <button class="btn secondary" id="manage-packs-btn">Manage</button>
            <button class="btn secondary" id="close-stickers-btn">Close</button>
          </div>
        </div>
        <div id="sticker-grid" class="sticker-grid"></div>
        <input id="sticker-pack-input" type="file" accept="image/png" multiple style="display:none" />
      `;
      document.querySelector('.main').appendChild(panel);
      document.getElementById('close-stickers-btn').onclick = ()=> panel.remove();
      document.getElementById('add-pack-btn').onclick = ()=> document.getElementById('sticker-pack-input').click();
      document.getElementById('sticker-pack-input').onchange = (e)=> this.addStickerFiles(e.target.files);
      document.getElementById('manage-packs-btn').onclick = ()=> this.manageStickerpacks();
      await this.renderStickerGrid();
    }

    async getStickerIndex(){
      try{
        const raw = localStorage.getItem('liber_stickerpacks');
        const obj = raw ? JSON.parse(raw) : { packs: [] };
        if (!Array.isArray(obj.packs)) obj.packs = [];
        return obj;
      }catch(_){ return { packs: [] }; }
    }
    async setStickerIndex(idx){ localStorage.setItem('liber_stickerpacks', JSON.stringify(idx)); }

    async addStickerFiles(fileList){
      if (!fileList || !fileList.length) return;
      const idx = await this.getStickerIndex();
      // Current pack is timestamp-based
      const packId = 'pack_'+Date.now();
      const pack = { id: packId, name: 'My pack '+new Date().toLocaleDateString(), items: [] };
      // Store PNGs to Firebase Storage encrypted, keep manifest locally with storage URLs
      for (const f of fileList){
        if (!/\.png$/i.test(f.name)) continue;
        try{
          const aesKey = await this.getFallbackKey();
          const base64 = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(f); });
          const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
          const safeName = f.name.replace(/[^a-zA-Z0-9._-]/g,'_');
          const path = `stickers/${this.currentUser.uid}/${Date.now()}_${safeName}.enc.json`;
          const sref = firebase.ref(this.storage, path);
          await firebase.uploadBytes(sref, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
          const url = await firebase.getDownloadURL(sref);
          pack.items.push({ name: safeName, url });
        }catch(_){ /* skip failed file */ }
      }
      if (pack.items.length){ idx.packs.unshift(pack); await this.setStickerIndex(idx); await this.renderStickerGrid(); }
    }

    async renderStickerGrid(){
      const grid = document.getElementById('sticker-grid'); if (!grid) return;
      grid.innerHTML='';
      const idx = await this.getStickerIndex();
      // Most used quick row
      const rec = JSON.parse(localStorage.getItem('liber_sticker_recents')||'[]');
      if (Array.isArray(rec) && rec.length){
        const frag = document.createElement('div'); frag.className='sticker-grid';
        rec.slice(0,8).forEach(item=>{
          const cell = document.createElement('div');
          const img = document.createElement('img'); img.alt = item.name;
          (async()=>{
            try{ const res=await fetch(item.url); const payload=await res.json(); const b64=await chatCrypto.decryptWithKey(payload, await this.getFallbackKey()); const blob=this.base64ToBlob(b64,'image/png'); img.src=URL.createObjectURL(blob);}catch(_){ }
          })();
          cell.appendChild(img); cell.addEventListener('click', ()=> this.sendSticker(item)); frag.appendChild(cell);
        });
        grid.appendChild(frag);
      }
      const aesKey = await this.getFallbackKey();
      for (const p of idx.packs){
        for (const it of p.items){
          const cell = document.createElement('div');
          const img = document.createElement('img');
          img.alt = it.name;
          // Lazy load with decrypted thumb (best-effort)
          (async()=>{
            try{
              const res = await fetch(it.url); const payload = await res.json();
              const b64 = await chatCrypto.decryptWithKey(payload, aesKey);
              const blob = this.base64ToBlob(b64, 'image/png');
              img.src = URL.createObjectURL(blob);
            }catch(_){ img.src='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGMAAQAABQABdQh3VwAAAABJRU5ErkJggg=='; }
          })();
          cell.appendChild(img);
          cell.addEventListener('click', ()=> this.sendSticker(it));
          grid.appendChild(cell);
        }
      }
    }

    async manageStickerpacks(){
      try{
        const idx = await this.getStickerIndex();
        const names = idx.packs.map((p,i)=> `${i+1}. ${p.name} (${p.items.length})`).join('\n');
        const pick = prompt(`Your stickerpacks:\n${names}\n\nType number to delete or rename, or leave blank to close:`);
        if (!pick) return;
        const n = parseInt(pick,10); if (!Number.isFinite(n) || n<1 || n>idx.packs.length) return;
        const act = prompt('Type "d" to delete, or enter new name to rename:');
        if (!act) return;
        if (act.toLowerCase()==='d'){ idx.packs.splice(n-1,1); await this.setStickerIndex(idx); await this.renderStickerGrid(); return; }
        idx.packs[n-1].name = act; await this.setStickerIndex(idx); await this.renderStickerGrid();
      }catch(_){ }
    }

    async sendSticker(item){
      if (!this.activeConnection) return;
      try{
        // Simply send as a file message with original encrypted JSON URL and filename
        // The renderer already handles decryption and preview
        await this.saveMessage({ text: '[sticker]', fileUrl: item.url, fileName: item.name });
        // update recents
        try{
          const curr = JSON.parse(localStorage.getItem('liber_sticker_recents')||'[]').filter(x=> x.url!==item.url);
          curr.unshift(item); localStorage.setItem('liber_sticker_recents', JSON.stringify(curr.slice(0,24)));
        }catch(_){ }
        const pnl = document.getElementById('sticker-panel'); if (pnl) pnl.remove();
      }catch(_){ alert('Failed to send sticker'); }
    }

    async saveMessage({text,fileUrl,fileName}){
      const aesKey = await this.getFallbackKey();
      const cipher = await chatCrypto.encryptWithKey(text, aesKey);
      const msgRef = firebase.doc(firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'));
      await firebase.setDoc(msgRef,{
        id: msgRef.id,
        connId: this.activeConnection,
        sender: this.currentUser.uid,
        cipher,
        fileUrl: fileUrl||null,
        fileName: fileName||null,
        createdAt: new Date().toISOString(),
        createdAtTS: firebase.serverTimestamp()
      });
      await firebase.updateDoc(firebase.doc(this.db,'chatConnections',this.activeConnection),{
        lastMessage: text.slice(0,200),
        updatedAt: new Date().toISOString()
      });
      // Notify recipients (best-effort)
      this.notifyParticipants(text);
      // Optional push notification via Firebase Functions; fallback to email if no tokens
      try{
        if (window.firebaseService && typeof window.firebaseService.callFunction === 'function'){
          const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
          const data = connSnap.exists()? connSnap.data():null;
          const participantUids = (data && Array.isArray(data.participants)) ? data.participants : [];
          const recipients = participantUids.filter(uid=> uid !== this.currentUser.uid);
          const pushResp = await window.firebaseService.callFunction('sendPush', { connId: this.activeConnection, recipients, preview: text.slice(0,120) }) || {};
          const sent = Number(pushResp.sent||0);
          if (!sent && recipients && recipients.length){
            // Fallback: sendMail per recipient (best-effort)
            for (const uid of recipients){
              try{
                const u = await window.firebaseService.getUserData(uid);
                const email = u && u.email;
                if (email){
                  await window.firebaseService.callFunction('sendMail', { to: email, subject: 'New message', html: `<p>You have a new message:</p><p>${(text||'').slice(0,200)}</p>` });
                }
              }catch(_){ }
            }
          }
        }
      }catch(_){ /* ignore optional push errors */ }
      await this.loadMessages();
    }

    /* Group admin management */
    async toggleGroupPanel(){
      const existing = document.getElementById('group-panel');
      if (existing){ existing.remove(); return; }
      const panel = document.createElement('div'); panel.id='group-panel'; panel.className='group-panel';
      panel.innerHTML = `<h4>Group</h4><div id="group-summary"></div><ul id="group-list" class="group-list"></ul><div class="group-actions"><button class="btn secondary" id="add-member-btn">Add member</button><button class="btn secondary" id="close-group-btn">Close</button></div>`;
      document.querySelector('.main').appendChild(panel);
      document.getElementById('close-group-btn').onclick = ()=> panel.remove();
      document.getElementById('add-member-btn').onclick = async ()=>{
        const s=document.getElementById('user-search');
        try{
          const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
          if (snap.exists()) this.groupBaseParticipants = (snap.data().participants||[]);
        }catch(_){ this.groupBaseParticipants = null; }
        if(s){ this.isGroupMode=true; this.groupSelection=this.groupSelection||new Map(); s.focus(); }
      };
      await this.renderGroupPanel();
    }

    async renderGroupPanel(){
      const list = document.getElementById('group-list'); const summary = document.getElementById('group-summary');
      if (!list || !this.activeConnection) return;
      list.innerHTML=''; if (summary) summary.textContent='';
      try{
        const doc = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
        if (!doc.exists()) return;
        const conn = doc.data();
        const participants = Array.isArray(conn.participants)? conn.participants:[];
        const usernames = Array.isArray(conn.participantUsernames)? conn.participantUsernames:[];
        const admins = Array.isArray(conn.admins)? conn.admins : [participants[0]].filter(Boolean);
        if (!Array.isArray(conn.admins)){
          await firebase.updateDoc(firebase.doc(this.db,'chatConnections', this.activeConnection),{ admins });
        }
        const amAdmin = admins.includes(this.currentUser.uid);
        if (summary){ summary.innerHTML = `Members: ${participants.length} · Admins: ${admins.length}${amAdmin? ' · You are admin':''}`; }
        for (let i=0;i<participants.length;i++){
          const uid = participants[i]; const name = usernames[i] || uid;
          const li = document.createElement('li');
          const left = document.createElement('span'); left.textContent = name + (admins.includes(uid)? ' (admin)':'');
          const right = document.createElement('span');
          if (amAdmin && uid !== this.currentUser.uid){
            const rm = document.createElement('button'); rm.className='btn secondary'; rm.textContent='Remove'; rm.onclick = ()=> this.removeMember(uid);
            right.appendChild(rm);
            const isAdmin = admins.includes(uid);
            const toggle = document.createElement('button'); toggle.className='btn secondary'; toggle.textContent = isAdmin? 'Revoke admin':'Make admin'; toggle.onclick = ()=> this.toggleAdmin(uid, !isAdmin);
            right.appendChild(toggle);
          }
          li.appendChild(left); li.appendChild(right); list.appendChild(li);
        }
      }catch(_){ }
    }

    async removeMember(uid){
      try{
        const ref = firebase.doc(this.db,'chatConnections', this.activeConnection);
        const doc = await firebase.getDoc(ref); if (!doc.exists()) return;
        const conn = doc.data();
        const parts = (conn.participants||[]).filter(x=> x!==uid);
        const names = (conn.participantUsernames||[]).filter((_,i)=> (conn.participants||[])[i]!==uid);
        let admins = Array.isArray(conn.admins)? conn.admins: [];
        admins = admins.filter(x=> x!==uid);
        await firebase.updateDoc(ref, { participants: parts, participantUsernames: names, admins, updatedAt: new Date().toISOString() });
        // Recompute key and possibly merge with existing connection with same set
        const key = this.computeConnKey(parts);
        await firebase.updateDoc(ref, { key });
        await this.renderGroupPanel(); await this.loadConnections();
      }catch(_){ alert('Failed to remove member'); }
    }

    async toggleAdmin(uid, make){
      try{
        const ref = firebase.doc(this.db,'chatConnections', this.activeConnection);
        const doc = await firebase.getDoc(ref); if (!doc.exists()) return;
        let admins = Array.isArray(doc.data().admins)? doc.data().admins: [];
        if (make){ if (!admins.includes(uid)) admins.push(uid); }
        else { admins = admins.filter(x=> x!==uid); }
        await firebase.updateDoc(ref, { admins, updatedAt: new Date().toISOString() });
        await this.renderGroupPanel();
      }catch(_){ alert('Failed to update admin'); }
    }

    async notifyParticipants(plaintext){
      try{
        // Local device notification
        if ('Notification' in window){
          if (Notification.permission === 'granted'){
            new Notification('New message', { body: plaintext.slice(0,80) });
          } else if (Notification.permission !== 'denied'){
            // Request once
            Notification.requestPermission().then(p=>{ if(p==='granted'){ new Notification('New message', { body: plaintext.slice(0,80) }); } });
          }
        }
      }catch(_){}

      // Email notifications now handled server-side by Cloud Function on chat message create
    }

    async getOrCreateSharedAesKey(){
      if (!this.activeConnection) throw new Error('No active connection');
      if (this.sharedKeyCache[this.activeConnection]) return this.sharedKeyCache[this.activeConnection];

      // Load my identity keys (ECDH)
      const myUid = this.currentUser.uid;
      const myId = await chatCrypto.loadOrCreateIdentity(myUid);

      // Store my public key in Firestore under users collection for discovery
      const pubRef = firebase.doc(this.db, 'userPublicKeys', myUid);
      await firebase.setDoc(pubRef, { uid: myUid, publicJwk: myId.publicJwk, updatedAt: new Date().toISOString() }, { merge: true });

      // Determine peer uid
      const peerUid = await this.getPeerUid();

      // Fetch peer public key
      const peerSnap = await firebase.getDoc(firebase.doc(this.db, 'userPublicKeys', peerUid));
      if (!peerSnap.exists()) throw new Error('Peer public key not found, ask peer to open chat');
      const peerJwk = peerSnap.data().publicJwk;

      // Import my private key
      const myPriv = await chatCrypto.getPrivateKey(myUid);

      // Derive shared AES key
      const aesKey = await chatCrypto.deriveSharedAesKey(myPriv, peerJwk);
      this.sharedKeyCache[this.activeConnection] = aesKey;
      return aesKey;
    }

    async getPeerUid(){
      const conn = (this.connections||[]).find(c=> c.id === this.activeConnection);
      if (conn && Array.isArray(conn.participants)){
        return conn.participants.find(u=> u !== this.currentUser.uid);
      }
      try {
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
        if (snap.exists()){
          const data = snap.data();
          if (Array.isArray(data.participants)){
            return data.participants.find(u=> u !== this.currentUser.uid);
          }
        }
      } catch {}
      const parts = (this.activeConnection||'').split('_');
      if (parts.length === 2){
        return parts[0] === this.currentUser.uid ? parts[1] : parts[0];
      }
      return '';
    }

    async getFallbackKey(){
      const connId = this.activeConnection;
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        const parts = snap.exists() ? this.getConnParticipants(snap.data()) : [];
        if (parts.length > 2){
          const groupSecret = `${parts.slice().sort().join('|')}|${connId}|liber_group_fallback_v2`;
          return window.chatCrypto.deriveChatKey(groupSecret);
        }
      }catch(_){ }
      const peerUid = await this.getPeerUid();
      if (window.chatCrypto && typeof window.chatCrypto.deriveFallbackSharedAesKey === 'function'){
        return window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, connId);
      }
      const secret = `${[this.currentUser.uid, peerUid].sort().join('|')}|${connId}|liber_secure_chat_fallback_v1`;
      return window.chatCrypto.deriveChatKey(secret);
    }

    isImageFilename(name){
      const n = (name||'').toLowerCase();
      return n.endsWith('.png') || n.endsWith('.jpg') || n.endsWith('.jpeg') || n.endsWith('.gif') || n.endsWith('.webp');
    }
    isVideoFilename(name){
      const n = (name||'').toLowerCase();
      return n.endsWith('.mp4') || n.endsWith('.webm') || n.endsWith('.mov') || n.endsWith('.mkv');
    }
    isAudioFilename(name){
      const n = (name||'').toLowerCase();
      return n.endsWith('.mp3') || n.endsWith('.wav') || n.endsWith('.m4a') || n.endsWith('.aac') || n.endsWith('.ogg') || n.endsWith('.oga') || n.endsWith('.weba');
    }

    async renderEncryptedAttachment(containerEl, fileUrl, fileName, aesKey){
      try {
        const res = await fetch(fileUrl, { mode: 'cors' });
        const ct = res.headers.get('content-type')||'';
        const payload = ct.includes('application/json') ? await res.json() : JSON.parse(await res.text());
        let b64;
        try { b64 = await chatCrypto.decryptWithKey(payload, aesKey); }
        catch {
          try{
            const peerUid = await this.getPeerUid();
            const legacy = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, this.activeConnection);
            b64 = await chatCrypto.decryptWithKey(payload, legacy);
          }catch{
            const alt = await this.getOrCreateSharedAesKey();
            b64 = await chatCrypto.decryptWithKey(payload, alt);
          }
        }
        if (this.isImageFilename(fileName)){
          const mime = fileName.toLowerCase().endsWith('.png') ? 'image/png'
                      : fileName.toLowerCase().endsWith('.webp') ? 'image/webp'
                      : fileName.toLowerCase().endsWith('.gif') ? 'image/gif'
                      : 'image/jpeg';
          const blob = this.base64ToBlob(b64, mime);
          const url = URL.createObjectURL(blob);
          const img = document.createElement('img');
          img.src = url; img.style.maxWidth = '100%'; img.style.height='auto'; img.style.borderRadius='8px'; img.alt = fileName;
          containerEl.appendChild(img);
        } else if (this.isVideoFilename(fileName)){
          const mime = 'video/webm';
          const blob = this.base64ToBlob(b64, mime);
          const url = URL.createObjectURL(blob);
          const video = document.createElement('video');
          video.src = url; video.controls = true; video.playsInline = true; video.style.maxWidth = '100%'; video.style.borderRadius='8px';
          containerEl.appendChild(video);
        } else if (this.isAudioFilename(fileName)){
          const mime = 'audio/webm';
          const blob = this.base64ToBlob(b64, mime);
          const url = URL.createObjectURL(blob);
          const audio = document.createElement('audio');
          audio.src = url; audio.controls = true; audio.style.width = '100%';
          containerEl.appendChild(audio);
        } else if ((fileName||'').toLowerCase().endsWith('.pdf')){
          const blob = this.base64ToBlob(b64, 'application/pdf');
          const url = URL.createObjectURL(blob);
          const row = document.createElement('div');
          row.style.display = 'flex';
          row.style.gap = '8px';
          row.style.alignItems = 'center';
          row.style.flexWrap = 'wrap';
          const info = document.createElement('span');
          info.textContent = fileName || 'document.pdf';
          info.style.opacity = '0.9';
          const btn = document.createElement('a');
          btn.href = url;
          btn.download = fileName || 'document.pdf';
          btn.className = 'btn btn-secondary';
          btn.textContent = 'Download PDF';
          row.appendChild(info);
          row.appendChild(btn);
          containerEl.appendChild(row);
        } else {
          const blob = this.base64ToBlob(b64, 'application/octet-stream');
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a'); a.href = url; a.download = fileName; a.textContent = 'Download decrypted file';
          containerEl.appendChild(a);
        }
      } catch (e) { /* noop */ }
    }

    base64ToBlob(b64, mime){
      const byteString = atob(b64);
      const len = byteString.length;
      const bytes = new Uint8Array(len);
      for (let i=0;i<len;i++) bytes[i] = byteString.charCodeAt(i);
      return new Blob([bytes], {type: mime});
    }

    async searchUsers(term){
      const wrapper = document.getElementById('search-results');
      const userGroup = document.getElementById('user-results-group');
      const resultsEl = document.getElementById('user-results');
      const msgGroup = document.getElementById('message-results-group');
      const msgResults = document.getElementById('message-results');
      if (!wrapper || !resultsEl || !msgResults) return;
      resultsEl.innerHTML = '';
      msgResults.innerHTML = '';
      if (!term){ wrapper.style.display='none'; if (userGroup) userGroup.style.display='none'; if (msgGroup) msgGroup.style.display='none'; return; }
      try{
        if (window.firebaseService && window.firebaseService.isFirebaseAvailable()){
          const users = await window.firebaseService.searchUsers(term.toLowerCase());
          // Rank by fuzzy score + recency (last messaged) + frequency placeholder
          const filtered = (users||[]).filter(u=> u.uid !== (this.currentUser&&this.currentUser.uid)).map(u=>{
            const name = (u.username||'').toLowerCase(); const mail=(u.email||'').toLowerCase(); const t=term.toLowerCase();
            const contains = (s)=> s.includes(t);
            const prefix = (s)=> s.startsWith(t);
            const subseq = (s)=>{ let i=0; for (const ch of s){ if (ch===t[i]) i++; if (i===t.length) return true; } return t.length===0; };
            let score = 0; if (prefix(name)||prefix(mail)) score+=3; if (contains(name)||contains(mail)) score+=2; if (subseq(name)||subseq(mail)) score+=1;
            // recent conversation boost
            let recentBoost = 0;
            try{
              const existingKey = this.computeConnKey([this.currentUser.uid, u.uid||u.id]);
              const conn = this.connections.find(c=> (c.key||this.computeConnKey(c.participants||[]))===existingKey);
              if (conn && conn.updatedAt){
                const age = Date.now() - new Date(conn.updatedAt).getTime();
                recentBoost = Math.max(0, 5 - Math.log10(1+age/86400000));
              }
            }catch(_){ }
            return {u, score: score + recentBoost};
          })
          .sort((a,b)=> b.score - a.score)
          .map(x=> x.u);
          if (filtered.length > 0){
            if (userGroup) userGroup.style.display='block';
            filtered.slice(0,20).forEach(u=>{
              const li=document.createElement('li');
              li.textContent = `${u.username||u.email}`;
              li.addEventListener('click', async ()=>{
                const search = document.getElementById('user-search');
                if (this.isGroupMode){
                  // Add chip
                  const chips = document.getElementById('group-selected');
                  if (chips && !this.groupSelection.has(u.uid||u.id)){
                    this.groupSelection.set(u.uid||u.id, u);
                    const chip = document.createElement('span');
                    chip.className = 'chip';
                    chip.textContent = u.username||u.email;
                    chip.addEventListener('click', ()=>{
                      this.groupSelection.delete(u.uid||u.id);
                      chip.remove();
                    });
                    chips.appendChild(chip);
                  }
                  // stay in selection mode, keep results open and continue typing
                  if (search) { search.focus(); search.select(); }
                  return;
                }
                wrapper.style.display='none';
                if (search) search.value = '';
                const myName = (this.me && this.me.username) || (this.currentUser.email||'me');
                const uids = [this.currentUser.uid, u.uid||u.id];
                const key = this.computeConnKey(uids);
                try{
                  let connId = await this.findConnectionByKey(key);
                  if (!connId){
                    try{
                      const stableRef = firebase.doc(this.db,'chatConnections', key);
                      await firebase.setDoc(stableRef,{
                        id: key,
                        key,
                        participants: uids,
                        participantUsernames:[myName, u.username||u.email],
                        admins: [this.currentUser.uid],
                        createdAt: new Date().toISOString(),
                        updatedAt: new Date().toISOString(),
                        lastMessage:''
                      }, { merge:false });
                      connId = key;
                    }catch(errSet){
                      const newRef = firebase.doc(firebase.collection(this.db,'chatConnections'));
                      connId = newRef.id;
                      await firebase.setDoc(newRef,{
                        id: connId,
                        key,
                        participants: uids,
                        participantUsernames:[myName, u.username||u.email],
                        admins: [this.currentUser.uid],
                        createdAt: new Date().toISOString(),
                        updatedAt: new Date().toISOString(),
                        lastMessage:''
                      });
                    }
                  }
                  await this.loadConnections();
                  this.setActive(connId, u.username||u.email);
                }catch(err){
                  console.error('Chat creation failed:', err);
                  const wrapper = document.getElementById('search-results');
                  if (wrapper){
                    const warn = document.createElement('div');
                    warn.style.cssText='padding:8px;color:#ff6a6a';
                    warn.textContent = 'Failed to start chat (permissions or network). Check Firestore rules: user must be a participant.';
                    wrapper.appendChild(warn);
                  }
                }
              });
              resultsEl.appendChild(li);
            });
          } else if (userGroup) { userGroup.style.display='none'; }
          // Messages search within current view
          const termLower = term.toLowerCase();
          const allMessages = Array.from(document.querySelectorAll('.messages .message'));
          const matches = allMessages.filter(m => m.textContent.toLowerCase().includes(termLower));
          if (matches.length>0){
            if (msgGroup) msgGroup.style.display='block';
            matches.slice(0,20).forEach(m=>{
              const li=document.createElement('li');
              li.textContent = m.textContent.slice(0,120);
              li.addEventListener('click', ()=>{
                wrapper.style.display='none';
                const box=document.getElementById('messages');
                if (box){ box.scrollTop = m.offsetTop - 40; }
              });
              msgResults.appendChild(li);
            });
          } else if (msgGroup) { msgGroup.style.display='none'; }
          wrapper.style.display = (filtered.length>0 || matches.length>0) ? 'block':'none';
        }
      }catch(e){ console.warn('Search failed', e); if (wrapper) wrapper.style.display='none'; }
    }

    // Placeholders / basic signaling for calls
  async startVoiceCall(){ await this.startCall({ callId: `${this.activeConnection}_latest`, video:false }); }
  async startVideoCall(){ await this.startCall({ callId: `${this.activeConnection}_latest`, video:true }); }

  async ensureRoom(){
    if (!this.activeConnection) return null;
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    try{
      const snap = await firebase.getDoc(roomRef);
      if (!snap.exists()){
        await firebase.setDoc(roomRef, {
          id: this.activeConnection,
          status: 'idle',
          activeCallId: null,
          createdAt: new Date().toISOString(),
          lastActiveAt: new Date().toISOString()
        });
      } else {
        const data = snap.data() || {};
        const updates = { lastActiveAt: new Date().toISOString() };
        if (typeof data.status === 'undefined') updates.status = 'idle';
        if (!('activeCallId' in data)) updates.activeCallId = null;
        await firebase.updateDoc(roomRef, updates);
      }
      return roomRef;
    }catch(_){ return roomRef; }
  }

  async enterRoom(video = false){
    try {
      await navigator.mediaDevices.getUserMedia({ audio: true });
      console.log('Mic permission granted');
    } catch (err) {
      console.error('Mic permission denied', err);
      alert('Microphone access is required for voice detection in calls.');
      return;
    }
    this._videoEnabled = !!video;
    await this.ensureRoom();
    const ov = document.getElementById('call-overlay');
    if (ov) ov.classList.remove('hidden');
    const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'Room open. Waiting for speech to start call...';
    const status = document.getElementById('call-status');
    if (status) status.textContent = 'Room open. Waiting for speech to start call...';
    this.initCallControls(video);
    if (this._roomUnsub) this._roomUnsub();
    if (this._peersUnsub) this._peersUnsub();
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    this._roomUnsub = firebase.onSnapshot(roomRef, async snap => {
      this._roomState = snap.data() || { status: 'idle', activeCallId: null };
      // Only auto-join when a call is active and I'm NOT the initiator of this active call
      const activeCid = this._roomState.activeCallId;
      const startedBy = this._roomState.startedBy;
      const iAmInitiator = !!activeCid && (startedBy === (this.currentUser && this.currentUser.uid)) || (activeCid === this._connectingCid);
      const alreadyActiveHere = !!this._activeCid && this._activeCid === activeCid;
      if (activeCid && !iAmInitiator && !alreadyActiveHere && this._activePCs.size === 0 && !this._joiningCall && !this._startingCall && this._lastJoinedCallId !== activeCid) {
        await this.joinMultiCall(activeCid, video);
      }
      // Do not auto-cleanup here; end is controlled by silence timer or End button
      await this.updateRoomUI();
      const status = document.getElementById('call-status');
      if (status) status.textContent = activeCid ? 'In call' : 'Room open. Waiting for speech to start call...';
    });
    const peersRef = firebase.collection(this.db,'callRooms', this.activeConnection, 'peers');
    this._peersUnsub = firebase.onSnapshot(peersRef, snap => {
      this._peersPresence = {};
      snap.forEach(d => this._peersPresence[d.id] = d.data());
      this.updateRoomUI();
    });
    await this.updatePresence('idle', false);
    // Start silence monitor if no call active
    if (this._roomState.status === 'idle') {
      this._startAutoResumeMonitor(video);
    }
    this._inRoom = true;
  }

  initCallControls(video){
    const ov = document.getElementById('call-overlay');
    const endBtn = document.getElementById('end-call-btn');
    const micBtn = document.getElementById('toggle-mic-btn');
    const camBtn = document.getElementById('toggle-camera-btn');
    const hideBtn = document.getElementById('hide-call-btn');
    const showBtn = document.getElementById('show-call-btn');
    const exitBtn = document.getElementById('exit-room-btn');
    if (endBtn) endBtn.onclick = async () => { 
      console.log('End clicked'); 
      // End the call for everyone but keep overlay open for the room
      await this.cleanupActiveCall(true, 'end_button');
      const status = document.getElementById('call-status');
      if (status) status.textContent = 'Room open. Waiting for speech to start call...';
    };
    if (exitBtn) exitBtn.onclick = async () => { 
      console.log('Exit clicked'); 
      await this.cleanupActiveCall(false, 'exit_button'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.add('hidden'); 
    };
    if (micBtn) micBtn.onclick = async () => {
      console.log('Mic toggle');
      this._micEnabled = !this._micEnabled;
      this._activePCs.forEach(p => p.stream.getAudioTracks().forEach(t => t.enabled = this._micEnabled));
      if (this._roomState.activeCallId) await this.renegotiateCall(this._roomState.activeCallId, this._videoEnabled);
    };
    if (camBtn) camBtn.onclick = async () => {
      console.log('Camera toggle');
      this._videoEnabled = !this._videoEnabled;
      this._activePCs.forEach(p => p.stream.getVideoTracks().forEach(t => t.enabled = this._videoEnabled));
      await this.updatePresence(this._roomState ? this._roomState.status : 'idle', this._videoEnabled);
      const lv = document.getElementById('localVideo');
      if (lv) lv.style.display = this._videoEnabled ? 'block' : 'none';
      if (this._roomState.activeCallId) await this.renegotiateCall(this._roomState.activeCallId, this._videoEnabled);
    };
    if (hideBtn) hideBtn.onclick = () => { 
      console.log('Hide clicked'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.add('hidden'); 
      const showBtn = document.getElementById('show-call-btn');
      if (showBtn) showBtn.style.display = 'block'; 
    };
    if (showBtn) showBtn.onclick = () => { 
      console.log('Show clicked'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.remove('hidden'); 
      const showBtn = document.getElementById('show-call-btn');
      if (showBtn) showBtn.style.display = 'none'; 
    };
  }

  async attemptStartRoomCall(video){
    console.log('Attempting to start room call');
    if (this._startingCall || this._joiningCall) { console.warn('Call start suppressed (busy)'); return; }
    if (!this._roomState) {
      console.warn('Room state not loaded yet, retrying...');
      await new Promise(r => setTimeout(r, 500)); // Short delay
      if (!this._roomState) return; // Bail if still null
    }
    // If room already active, join instead of trying to set active
    if (this._roomState.activeCallId) {
      this._joiningCall = true;
      try { await this.joinMultiCall(this._roomState.activeCallId, video); }
      finally { this._joiningCall = false; }
      return;
    }
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    const cid = `${this.activeConnection}_latest`;
    this._startingCall = true;
    this._connectingCid = cid;
    const success = await this.runStartTransaction(roomRef, cid).catch(err => {
      console.error('Transaction failed:', err);
      if (err.code === 'permission-denied') alert('Permission denied starting call. Check Firestore rules for /callRooms.');
      return false;
    });
    if (success) {
      console.log('Transaction success, starting multi call');
      try { await this.startMultiCall(cid, video); }
      finally { this._startingCall = false; }
      await this.saveMessage({ text: `[call:voice:${cid}]` });
      this._monitoring = false;
    } else {
      console.log('Room already active, joining');
      if (this._roomState && this._roomState.activeCallId && this._roomState.activeCallId !== 'undefined') {
        console.log('Joining call ID:', this._roomState.activeCallId);
        this._joiningCall = true;
        try { await this.joinMultiCall(this._roomState.activeCallId, video); }
        finally { this._joiningCall = false; }
      } else {
        console.warn('No valid active call to join');
      }
      this._startingCall = false;
    }
  }

  async runStartTransaction(roomRef, cid) {
    return runTransaction(this.db, async (tx) => {
      const snap = await tx.get(roomRef);
      const data = snap.exists() ? (snap.data() || {}) : { status: 'idle', activeCallId: null };
      if (data.status !== 'idle' || data.activeCallId) return false;
      tx.update(roomRef, {
        status: 'connecting',
        activeCallId: cid,
        startedBy: this.currentUser.uid,
        startedAt: new Date().toISOString()
      });
      return true;
    }).catch(err => {
      console.error('Transaction failed:', err);
      return false;
    });
  }

  async startMultiCall(callId, video = false){
    console.log('Starting multi call', callId, video);
    if (this._activePCs && this._activePCs.size > 0) {
      await this.cleanupActiveCall(false, 'start_multi_preclean');
    }
    const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
    const conn = connSnap.data() || {};
    const participants = (conn.participants||[]).filter(Boolean).filter(uid => uid !== this.currentUser.uid);
    // Temporary: limit to first remote only for stability
    const limited = participants.slice(0,1);
    if (participants.length + 1 > 8) {
      alert('Max 8 users per room');
      return;
    }
    await this.updatePresence('connecting', video);
    this._activePCs = new Map();
    const config = { audio: true, video: !!video };
    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia(config);
    } catch (err) {
      console.error('getUserMedia failed:', err);
      return;
    }
    stream.getVideoTracks().forEach(t => t.enabled = !!video);
    let videosCont = document.getElementById('call-videos');
    if (!videosCont) {
      videosCont = document.createElement('div');
      videosCont.id = 'call-videos';
      videosCont.style.cssText = 'display: flex; flex-wrap: wrap; gap: 10px; justify-content: center;';
      const overlay = document.getElementById('call-overlay');
      if (overlay) overlay.appendChild(videosCont);
    }
    let lv = document.getElementById('localVideo');
    if (!lv) {
      lv = document.createElement('video');
      lv.id = 'localVideo';
      lv.autoplay = true;
      lv.playsInline = true;
      lv.muted = true;
      lv.style.display = 'none';
      videosCont.appendChild(lv);
    }
    lv.srcObject = stream;
    lv.style.display = (video && stream.getVideoTracks().some(t=>t.enabled)) ? 'block' : 'none';
    for (const peerUid of limited){
      const pc = new RTCPeerConnection({
        iceServers: await this.getIceServers(),
        iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
      });
      pc.oniceconnectionstatechange = () => {
        console.log('ICE state for ' + peerUid + ':', pc.iceConnectionState);
        const st = pc.iceConnectionState;
        if (st === 'disconnected' || st === 'failed'){
          try { pc.restartIce && pc.restartIce(); } catch(_){ }
        }
      };
      pc.onconnectionstatechange = () => {
        console.log('PC state for ' + peerUid + ':', pc.connectionState);
        if (pc.connectionState === 'connected'){
          const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'In call';
          this._activeCid = callId;
          const key = callId+':'+peerUid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); }
        }
      };
      // ICE watchdogs
      const wdKey = callId+':'+peerUid;
      try { const old = this._pcWatchdogs.get(wdKey); if (old){ clearTimeout(old.t1); clearTimeout(old.t2); } } catch(_){ }
      const t1 = setTimeout(()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog: restartIce for '+peerUid); pc.restartIce && pc.restartIce(); } }catch(e){ console.warn('watchdog restartIce error', e?.message||e); } }, 5000);
      const t2 = setTimeout(async()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog: renegotiate for '+peerUid); const offer = await pc.createOffer({ iceRestart:true }); await pc.setLocalDescription(offer); const offersRef = firebase.collection(this.db,'calls',callId,'offers'); await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid }); } }catch(e){ console.warn('watchdog renegotiate error', e?.message||e); } }, 12000);
      this._pcWatchdogs.set(wdKey, { t1, t2 });
      // Prepare transceivers first to lock m-line order
      const txAudio = pc.addTransceiver('audio', { direction: 'sendrecv' });
      const txVideo = pc.addTransceiver('video', { direction: video ? 'sendrecv' : 'recvonly' });
      // Attach local tracks via replaceTrack when possible
      try {
        const a = stream.getAudioTracks()[0];
        if (a && txAudio && txAudio.sender) { try { await txAudio.sender.replaceTrack(a); } catch(_) {} }
      } catch(_) {}
      try {
        const v = stream.getVideoTracks()[0];
        if (v && txVideo && txVideo.sender && video) { try { await txVideo.sender.replaceTrack(v); } catch(_) {} }
      } catch(_) {}
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      const offersRef = firebase.collection(this.db,'calls',callId,'offers');
      await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      // Mark room active once first offer is published
      try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'connecting', activeCallId: callId, lastActiveAt: new Date().toISOString() }); } catch(_){ }
      let rv = document.getElementById(`remoteVideo-${peerUid}`);
      if (!rv) {
        rv = document.createElement('video');
        rv.id = `remoteVideo-${peerUid}`;
        rv.autoplay = true;
        rv.playsInline = true;
        rv.style.display = 'none';
        videosCont.appendChild(rv);
      }
      pc.ontrack = e => {
        console.log('Received remote track for ' + peerUid, e.track.kind);
        const stream = e.streams[0];
        if (!stream) return;
        const hasVid = stream.getVideoTracks && stream.getVideoTracks().some(t => t.enabled);
        // Always ensure an audio sink exists (hidden) regardless of video
        let audEl = document.getElementById(`remoteAudio-${peerUid}`);
        if (!audEl){ audEl = document.createElement('audio'); audEl.id = `remoteAudio-${peerUid}`; audEl.autoplay = true; audEl.playsInline = true; audEl.style.display = 'none'; document.body.appendChild(audEl); }
        audEl.srcObject = stream; audEl.muted = false; audEl.volume = 1;
        // Video element visible only if video tracks exist
        let mediaEl = rv;
        if (!hasVid){ mediaEl = audEl; }
        mediaEl.srcObject = stream;
        mediaEl.muted = false;
        mediaEl.volume = 1;
        mediaEl.addEventListener('loadedmetadata', () => {
          try { mediaEl.play().catch(err => console.error('Play failed:', err)); }
          catch (err) { console.error('Play error:', err); }
        });
        mediaEl.style.display = hasVid ? 'block' : 'none';
        this._attachSpeakingDetector(stream, `[data-uid="${peerUid}"]`, peerUid);
      };
      const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
      const localCandidateQueue = [];
      const remoteCandidateQueue = [];
      pc.onicecandidate = e => {
        if (e.candidate) {
          if (pc.remoteDescription) {
            firebase.setDoc(firebase.doc(candsRef), { type: 'offer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: e.candidate.toJSON() });
          } else {
            localCandidateQueue.push(e.candidate);
          }
        }
      };
      const unsubs = [];
      unsubs.push(firebase.onSnapshot(firebase.doc(this.db,'calls',callId,'answers', peerUid), async doc => {
        if (!doc.exists()) return;
        const data = doc.data();
        // Normalize to RTCSessionDescriptionInit
        const desc = { type: data.type || 'answer', sdp: data.sdp };
        // Only set when we have a local offer
        if (pc.signalingState !== 'have-local-offer') { return; }
        console.log('Setting remote description for ' + peerUid);
        try {
          await pc.setRemoteDescription(desc);
          // Flush queued LOCAL ICE now that remoteDescription is set (send to peer)
          while (localCandidateQueue.length) {
            const cand = localCandidateQueue.shift();
            try { await firebase.setDoc(firebase.doc(candsRef), { type: 'offer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: cand.toJSON() }); } catch (_) {}
          }
          try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'active', lastActiveAt: new Date().toISOString() }); } catch(_){ }
        } catch (err) {
          console.error('setRemote failed for ' + peerUid, err);
        }
      }));
      unsubs.push(firebase.onSnapshot(candsRef, snap => {
        snap.forEach(d => {
          const v = d.data();
          if (v.type === 'answer' && v.fromUid === peerUid && v.toUid === this.currentUser.uid && v.candidate) {
            if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
            if (pc.signalingState === 'closed') return;
            if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
            pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
          }
        });
      }));
      this._activePCs.set(peerUid, {pc, unsubs, stream, videoEl: rv});
    }
    await this.updatePresence('connected', video);
    this._setupRoomInactivityMonitor();
    this._attachSpeakingDetector(stream, '[data-uid="' + this.currentUser.uid + '"]', this.currentUser.uid);
  }

  async joinMultiCall(callId, video = false){
    console.log('Joining multi call', callId, video);
    if (this._joiningCall) return;
    if (this._lastJoinedCallId === callId && this._activePCs.size > 0) { return; }
    this._lastJoinedCallId = callId;
    this._joiningCall = true;
    try{
      if (this._activePCs && this._activePCs.size > 0) {
        await this.cleanupActiveCall(false, 'join_multi_preclean');
      }
      await this.updatePresence('connecting', video);
    this._activePCs = new Map();
    // Prepare local media
    const localCfg = { audio: true, video: !!video };
    let localStream;
    try {
      localStream = await navigator.mediaDevices.getUserMedia(localCfg);
    } catch (err) {
      console.error('getUserMedia failed:', err);
      return;
    }
    localStream.getVideoTracks().forEach(t => t.enabled = !!video);
    // UI containers
    let videosCont = document.getElementById('call-videos');
    if (!videosCont) {
      videosCont = document.createElement('div');
      videosCont.id = 'call-videos';
      videosCont.style.cssText = 'display: flex; flex-wrap: wrap; gap: 10px; justify-content: center;';
      const overlay = document.getElementById('call-overlay');
      if (overlay) overlay.appendChild(videosCont);
    }
    let lv = document.getElementById('localVideo');
    if (!lv) {
      lv = document.createElement('video');
      lv.id = 'localVideo';
      lv.autoplay = true;
      lv.playsInline = true;
      lv.muted = true;
      lv.style.display = 'none';
      videosCont.appendChild(lv);
    }
    lv.srcObject = localStream;
    lv.style.display = (video && localStream.getVideoTracks().some(t=>t.enabled)) ? 'block' : 'none';

    // Find the offer addressed to me to identify the initiator
    // Find the newest offer addressed to me; if none, poll briefly waiting for initiator
    let offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid));
    if (!offerDoc.exists()){
      for (let i=0;i<6;i++){
        await new Promise(r=>setTimeout(r,250));
        offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid));
        if (offerDoc.exists()) break;
      }
      if (!offerDoc.exists()){
        console.warn('No offer for current user found; room may not be active yet');
        return;
      }
    }
    const offer = offerDoc.data();
    const peerUid = offer.fromUid;

    const pc = new RTCPeerConnection({
      iceServers: await this.getIceServers(),
      iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
    });
    pc.oniceconnectionstatechange = () => {
      console.log('ICE state for ' + peerUid + ':', pc.iceConnectionState);
      const st = pc.iceConnectionState;
      if (st === 'disconnected' || st === 'failed'){
        try { pc.restartIce && pc.restartIce(); } catch(_){ }
      }
    };
    pc.onconnectionstatechange = () => {
      console.log('PC state for ' + peerUid + ':', pc.connectionState);
      if (pc.connectionState === 'connected' || pc.connectionState === 'completed'){
        const key = callId+':'+peerUid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); }
        const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'In call';
      }
    };

    // Remote media element
    let rv = document.getElementById(`remoteVideo-${peerUid}`);
    if (!rv) {
      rv = document.createElement('video');
      rv.id = `remoteVideo-${peerUid}`;
      rv.autoplay = true;
      rv.playsInline = true;
      rv.style.display = 'none';
      videosCont.appendChild(rv);
    }
    pc.ontrack = e => {
      console.log('Received remote track for ' + peerUid, e.track.kind);
      const rstream = e.streams[0];
      if (!rstream) return;
      const hasVid = rstream.getVideoTracks && rstream.getVideoTracks().some(t => t.enabled);
      // Always ensure an audio sink exists
      let audEl = document.getElementById(`remoteAudio-${peerUid}`);
      if (!audEl){ audEl = document.createElement('audio'); audEl.id = `remoteAudio-${peerUid}`; audEl.autoplay = true; audEl.playsInline = true; audEl.style.display = 'none'; document.body.appendChild(audEl); }
      audEl.srcObject = rstream; audEl.muted = false; audEl.volume = 1;
      let mediaEl = rv; if (!hasVid) mediaEl = audEl;
      mediaEl.srcObject = rstream;
      mediaEl.muted = false;
      mediaEl.volume = 1;
      mediaEl.addEventListener('loadedmetadata', () => {
        try { mediaEl.play().catch(err => console.error('Play failed:', err)); }
        catch (err) { console.error('Play error:', err); }
      });
      mediaEl.style.display = hasVid ? 'block' : 'none';
      this._attachSpeakingDetector(rstream, `[data-uid="${peerUid}"]`, peerUid);
    };

    const answersRef = firebase.collection(this.db,'calls',callId,'answers');
    const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
    // Ensure we only process candidates for this peer
    const myUid = this.currentUser.uid;
    const remoteCandidateQueue = [];
    const candidateQueue = [];
    pc.onicecandidate = e => {
      if (e.candidate) {
        if (pc.remoteDescription) {
          firebase.setDoc(firebase.doc(candsRef), { type: 'answer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: e.candidate.toJSON() });
        } else {
          candidateQueue.push(e.candidate);
        }
      }
    };

    await pc.setRemoteDescription({ type:'offer', sdp: offer.sdp });
    // Align transceivers to the remote offer and attach local tracks without changing m-line order
    const trxs = pc.getTransceivers ? pc.getTransceivers() : [];
    trxs.forEach(tx => {
      if (!tx || !tx.receiver || !tx.receiver.track) return;
      const kind = tx.receiver.track.kind;
      try {
        if (kind === 'audio') tx.direction = 'sendrecv';
        if (kind === 'video') tx.direction = (video ? 'sendrecv' : 'recvonly');
      } catch(_) {}
      const local = localStream.getTracks().find(t => t.kind === kind);
      if (local && tx.sender && typeof tx.sender.replaceTrack === 'function') {
        try { tx.sender.replaceTrack(local); } catch(_) {}
      }
    });
    // Fallback: attach any local track that has no sender yet
    localStream.getTracks().forEach(tr => {
      const hasSender = trxs.some(tx => tx && tx.sender && tx.sender.track && tx.sender.track.kind === tr.kind);
      if (!hasSender) {
        if (tr.kind === 'video' && !video) return;
        try { pc.addTrack(tr, localStream); } catch(_) {}
      }
    });
      const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
      await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      // Flush any queued ICE now that descriptions are set
      while (candidateQueue.length) { const c = candidateQueue.shift(); try { await pc.addIceCandidate(c); } catch(_){} }
      while (remoteCandidateQueue.length) { const rc = remoteCandidateQueue.shift(); try { await pc.addIceCandidate(new RTCIceCandidate(rc)); } catch(_){} }
      try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'active', activeCallId: callId, lastActiveAt: new Date().toISOString() }); this._activeCid = callId; } catch(_){ }
      candidateQueue.forEach(async cand => await pc.addIceCandidate(cand));
    candidateQueue.length = 0;
    const unsubs = [];
    unsubs.push(firebase.onSnapshot(candsRef, snap => {
      snap.forEach(d => {
        const v = d.data();
        if (v.type === 'offer' && v.fromUid === peerUid && v.toUid === myUid && v.candidate) {
          // Guard against calling before remoteDescription set
          if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
          if (pc.signalingState === 'closed') return;
          if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
          pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
        }
      });
    }));
    // Listen for future offers (ICE restarts/renegotiations) from initiator
    let lastOfferSdp = offer.sdp;
    const myOfferRef = firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid);
    const uOffers = firebase.onSnapshot(myOfferRef, async d => {
      try{
        if (!d.exists()) return;
        const data = d.data() || {};
        const sdp = data.sdp || '';
        if (!sdp || sdp === lastOfferSdp) return;
        lastOfferSdp = sdp;
        if (pc.signalingState === 'closed') return;
        // Apply new remote offer and answer back
        await pc.setRemoteDescription({ type:'offer', sdp: sdp });
        const ans2 = await pc.createAnswer();
        await pc.setLocalDescription(ans2);
        await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: ans2.sdp, type: ans2.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      }catch(e){ console.warn('offer update handling failed', e?.message||e); }
    });
    unsubs.push(uOffers);

    this._activePCs.set(peerUid, {pc, unsubs, stream: localStream, videoEl: rv});

    // Add watchdogs on joiner too
    const wdKey = callId+':'+peerUid;
    try { const old = this._pcWatchdogs.get(wdKey); if (old){ clearTimeout(old.t1); clearTimeout(old.t2); } } catch(_){ }
    const t1 = setTimeout(()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog (join): restartIce for '+peerUid); pc.restartIce && pc.restartIce(); } }catch(e){ console.warn('watchdog restartIce error', e?.message||e); } }, 12000);
    const t2 = setTimeout(async()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog (join): resend answer for '+peerUid); if (pc.signalingState === 'have-remote-offer'){ const ans = await pc.createAnswer({}); await pc.setLocalDescription(ans); await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: ans.sdp, type: ans.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid }); } else { console.log('Skip resend answer, signalingState=', pc.signalingState); } } }catch(e){ console.warn('watchdog resend answer error', e?.message||e); } }, 12000);
    this._pcWatchdogs.set(wdKey, { t1, t2 });

      await this.updatePresence('connected', video);
      this._setupRoomInactivityMonitor();
      this._attachSpeakingDetector(localStream, '[data-uid="' + this.currentUser.uid + '"]', this.currentUser.uid);
    } finally {
      this._joiningCall = false;
    }
  }

  async _setupRoomInactivityMonitor(){
    this._lastSpeech.clear();
    if (this._inactTimer) clearInterval(this._inactTimer);
    // Temporarily disable auto-silence end during debug to avoid premature cleanup
    /* this._inactTimer = setInterval(()=>{
      const now = Date.now();
      let maxLast = 0;
      this._lastSpeech.forEach(ts => maxLast = Math.max(maxLast, ts));
      if (now - maxLast > 5 * 60 * 1000){
        clearInterval(this._inactTimer);
        this.cleanupActiveCall(false, 'silence_timer');
        const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
        firebase.updateDoc(roomRef, { status: 'idle', activeCallId: null });
        this.saveMessage({ text: '[system] Call ended due to silence' });
        this._startAutoResumeMonitor(false);
      }
    }, 15000); */
  }

  // Add to _attachSpeakingDetector to take uid and update this._lastSpeech.set(uid, now) when avg > 30

  // In camBtn onclick for local: toggle enabled and updatePresence with hasVideo = enabled

  // For remote video toggle: on ontrack check hasVid and toggle display

  // In endBtn onclick: this.cleanupActiveCall(true) to end room for all

  // Limit: in enterRoom check conn.participants.length > 8 and alert/return

  async joinOrStartCall({ video }){
    // If latest call offer exists in this room, join; otherwise create a new one
    const roomId = this.activeConnection; if (!roomId) return;
    try{
      const cid = `${roomId}_latest`;
      const offersRef = firebase.collection(this.db,'calls',cid,'offers');
      const docSnap = await firebase.getDoc(firebase.doc(offersRef,'offer'));
      if (docSnap.exists()){
        await this.answerCall(cid, { video });
      } else {
        await this.startCall({ callId: cid, video });
      }
    }catch{ const cid = `${roomId}_latest`; await this.startCall({ callId: cid, video }); }
  }

  async startCall({ callId, video }){
      try{
        // ensure previous listeners/pcs are cleaned up when reconnecting
        try{ this._activeCall && this._activeCall.unsubs && this._activeCall.unsubs.forEach(u=>{ try{u&&u();}catch(_){}}); }catch(_){ }
        try{ this._activeCall && this._activeCall.pc && this._activeCall.pc.close(); }catch(_){ }
        callId = callId || `${this.activeConnection}_${Date.now()}`;
        const config = { audio: true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({
          iceServers: await this.getIceServers(),
          iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
        });
        window._pc = pc; // debug handle
        pc.oniceconnectionstatechange = ()=> console.log('ICE state:', pc.iceConnectionState);
        pc.onconnectionstatechange = ()=> console.log('PC state:', pc.connectionState);
        // Add local tracks safely (avoid duplicate senders)
        try{
          const existing = pc.getSenders ? pc.getSenders() : [];
          stream.getTracks().forEach(tr => {
            const sender = existing && existing.find(s => s.track && s.track.kind === tr.kind);
            if (sender){ try{ sender.replaceTrack(tr); }catch(_){} }
            else { pc.addTrack(tr, stream); }
          });
        }catch(_){ }
        // tracks already added or replaced above
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; try{ lv.muted = true; lv.playsInline = true; lv.play().catch(()=>{}); }catch(_){} lv.style.display = (video && stream.getVideoTracks().some(t=>t.enabled))? 'block':'none'; }
        pc.ontrack = (e)=>{
          if (rv){
            rv.srcObject = e.streams[0];
            try{ rv.playsInline = true; rv.muted = false; rv.play().catch(()=>{}); }catch(_){ }
            try{ const hasVid = e.streams[0].getVideoTracks().some(t=> t.enabled); rv.style.display = hasVid? 'block':'none'; }catch(_){ }
          }
          // For audio-only calls ensure an audio sink exists
          if (!video){
            let aud = document.getElementById('remoteAudio');
            if (!aud){ aud = document.createElement('audio'); aud.id='remoteAudio'; aud.autoplay = true; aud.style.display='none'; document.body.appendChild(aud); }
            aud.srcObject = e.streams[0];
          }
          this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote');
        };
        if (ov){ ov.classList.remove('hidden'); const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'Connecting...'; }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'offer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        // Avoid duplicate starts: if an offer already exists, switch to join flow
        const existingOffer = await firebase.getDoc(firebase.doc(offersRef,'offer'));
        if (existingOffer.exists()){
          try{ stream.getTracks().forEach(t=> t.stop()); }catch(_){ }
          return await this.answerCall(callId, { video });
        }
        const offer = await pc.createOffer(); await pc.setLocalDescription(offer);
        await firebase.setDoc(firebase.doc(offersRef,'offer'), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
        // publish one join message for room-wide latest call id
        await this.saveMessage({ text:`[call:${video?'video':'voice'}:${callId}]` });
        // Listen for answer
        if (firebase.onSnapshot){
          const unsubs=[];
          const u1 = firebase.onSnapshot(firebase.doc(this.db,'calls',callId,'answers','answer'), async (doc)=>{
            if (doc.exists()){
              const data = doc.data();
              await pc.setRemoteDescription(new RTCSessionDescription({ type:'answer', sdp:data.sdp }));
            }
          });
          unsubs.push(u1);
          const u2 = firebase.onSnapshot(candsRef, (snap)=>{
            snap.forEach(d=>{
              const v=d.data();
              if(v.type==='answer' && v.candidate){
                try{
                  if (pc.signalingState==='closed') return;
                  if (!pc.remoteDescription) return;
                  pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
                }catch(_){ }
              }
            });
          });
          unsubs.push(u2);
          this._activeCall = { pc, unsubs };
        }
        const endBtn = document.getElementById('end-call-btn'); if (endBtn) endBtn.textContent = 'Exit';
        const micBtn = document.getElementById('toggle-mic-btn');
        const camBtn = document.getElementById('toggle-camera-btn');
        const hideBtn = document.getElementById('hide-call-btn');
        const showBtn = document.getElementById('show-call-btn');
        if (endBtn){ endBtn.onclick = ()=>{ try{ this._activeCall && this._activeCall.unsubs && this._activeCall.unsubs.forEach(u=>{ try{u&&u();}catch(_){}}); }catch(_){ } try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='none'; }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ const enabled = stream.getVideoTracks().some(t=> t.enabled); stream.getVideoTracks().forEach(t=> t.enabled = !enabled); if (lv) lv.style.display = stream.getVideoTracks().some(t=>t.enabled)? 'block':'none'; }; }
        if (hideBtn){ hideBtn.onclick = ()=>{ if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='block'; }; }
        if (showBtn){ showBtn.onclick = ()=>{ if (ov) ov.classList.remove('hidden'); showBtn.style.display='none'; }; }
      }catch(e){ console.warn('Call start failed', e); }
    }

    async answerCall(callId, { video }){
      try{
        const config = { audio:true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({
          iceServers: await this.getIceServers(),
          iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
        });
        window._pc = pc; // debug handle
        pc.oniceconnectionstatechange = ()=> console.log('ICE state:', pc.iceConnectionState);
        pc.onconnectionstatechange = ()=> console.log('PC state:', pc.connectionState);
        try{
          const existing = pc.getSenders ? pc.getSenders() : [];
          stream.getTracks().forEach(tr => {
            const sender = existing && existing.find(s => s.track && s.track.kind === tr.kind);
            if (sender){ try{ sender.replaceTrack(tr); }catch(_){} }
            else { pc.addTrack(tr, stream); }
          });
        }catch(_){ }
        // tracks already added or replaced above
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; try{ lv.muted = true; lv.playsInline = true; lv.play().catch(()=>{}); }catch(_){} }
        pc.ontrack = (e)=>{
          if (rv){
            rv.srcObject = e.streams[0];
            try{ rv.playsInline = true; rv.muted = false; rv.play().catch(()=>{}); }catch(_){ }
          }
          if (!video){
            let aud = document.getElementById('remoteAudio');
            if (!aud){ aud = document.createElement('audio'); aud.id='remoteAudio'; aud.autoplay = true; aud.style.display='none'; document.body.appendChild(aud); }
            aud.srcObject = e.streams[0];
          }
          this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote');
        };
        if (ov){ ov.classList.remove('hidden'); const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'Connecting...'; }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const answersRef = firebase.collection(this.db,'calls',callId,'answers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'answer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        const offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers','offer'));
        if (!offerDoc.exists()) return;
        const offer = offerDoc.data();
        await pc.setRemoteDescription(new RTCSessionDescription({ type:'offer', sdp: offer.sdp }));
        pc.addTransceiver('audio', { direction: 'sendrecv' });
        pc.addTransceiver('video', { direction: video ? 'sendrecv' : 'recvonly' });
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        await firebase.setDoc(firebase.doc(answersRef,'answer'), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
        // Listen for caller ICE candidates (type 'offer') and add to this peer connection
        if (firebase.onSnapshot){
          const added = new Set();
          firebase.onSnapshot(candsRef, (snap)=>{
            snap.forEach(d=>{
              const v=d.data();
              const key=v && v.candidate && (v.candidate.sdpMid+':'+v.candidate.sdpMLineIndex+':'+v.candidate.candidate);
              if (v.type==='offer' && v.candidate && !added.has(key)){
                added.add(key);
                if (pc.signalingState==='closed' || !pc.remoteDescription) return;
                pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
              }
            });
          });
        }
        const endBtn = document.getElementById('end-call-btn');
        const micBtn = document.getElementById('toggle-mic-btn');
        const camBtn = document.getElementById('toggle-camera-btn');
        const hideBtn = document.getElementById('hide-call-btn');
        const showBtn = document.getElementById('show-call-btn');
        if (endBtn){ endBtn.onclick = ()=>{ try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='none'; }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ stream.getVideoTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (hideBtn){ hideBtn.onclick = ()=>{ if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='block'; }; }
        if (showBtn){ showBtn.onclick = ()=>{ if (ov) ov.classList.remove('hidden'); showBtn.style.display='none'; }; }
      }catch(e){ console.warn('Answer call failed', e); }
    }

    async _renderParticipants(){
      try{
        const cont = document.getElementById('call-participants'); if (!cont) return;
        cont.innerHTML='';
        const selfAvatar = (this.me && this.me.avatarUrl) || '../../images/default-bird.png';
        const local = document.createElement('div'); local.className = 'avatar local connected'; local.innerHTML = `<img src="${selfAvatar}" alt="me"/>`;
        cont.appendChild(local);
        // Peer
        const peerUid = await this.getPeerUid();
        let peerAvatar = '../../images/default-bird.png';
        try{ const d = await window.firebaseService.getUserData(peerUid); if (d && d.avatarUrl) peerAvatar = d.avatarUrl; }catch(_){ }
        const remote = document.createElement('div'); remote.className = 'avatar remote connected'; remote.innerHTML = `<img src="${peerAvatar}" alt="peer"/>`;
        cont.appendChild(remote);
      }catch(_){ }
    }

    _attachSpeakingDetector(stream, selector, uid){
      try{
        const el = document.querySelector(selector); if (!el) return;
        const ac = new (window.AudioContext || window.webkitAudioContext)();
        const src = ac.createMediaStreamSource(stream);
        const analyser = ac.createAnalyser(); analyser.fftSize = 512;
        src.connect(analyser);
        const data = new Uint8Array(analyser.frequencyBinCount);
        let rafId = 0;
        const tick = ()=>{
          analyser.getByteFrequencyData(data);
          let sum = 0; for (let i=0;i<data.length;i++) sum += data[i];
          const avg = sum / data.length;
          if (avg > 30){ el.classList.add('speaking'); if (uid) this._lastSpeech.set(uid, Date.now()); } else { el.classList.remove('speaking'); }
          // Update last speech timestamps for inactivity monitor
          if (avg > 30){
            const now = Date.now();
            if (selector && selector.includes('local')){ this._lastLocalSpeechTs = now; }
            if (selector && selector.includes('remote')){ this._lastRemoteSpeechTs = now; }
          }
          rafId = requestAnimationFrame(tick);
        };
        tick();
        // Stop when stream ends
        const stop = ()=>{ try{ cancelAnimationFrame(rafId); }catch(_){} try{ ac.close(); }catch(_){} };
        stream.getTracks().forEach(t=> t.addEventListener('ended', stop));
      }catch(_){ }
    }

    _setupInactivityMonitor(pc, localStream, opts){
      try{
        this._lastLocalSpeechTs = Date.now();
        this._lastRemoteSpeechTs = Date.now();
        // initial detector values are set by _attachSpeakingDetector callbacks
        if (this._inactTimer) clearInterval(this._inactTimer);
        const limitMs = 5 * 60 * 1000; // 5 minutes
        this._inactTimer = setInterval(()=>{
          const last = Math.max(this._lastLocalSpeechTs||0, this._lastRemoteSpeechTs||0);
          if (Date.now() - last > limitMs){
            clearInterval(this._inactTimer); this._inactTimer=null;
            try{ pc.close(); }catch(_){ }
            try{ localStream && localStream.getTracks().forEach(t=> t.stop()); }catch(_){ }
            const ov = document.getElementById('call-overlay'); if (ov) ov.classList.add('hidden');
            this.saveMessage({ text: '[system] Call ended due to 5 minutes of silence' }).catch(()=>{});
            this._startAutoResumeMonitor(opts && opts.video);
          }
        }, 15000);
      }catch(_){ }
    }

    async _startAutoResumeMonitor(video){
      if (this._monitoring) return;
      this._monitoring = true;
      console.log('Starting speech monitor');
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      this._monitorStream = stream;
      const ac = new AudioContext();
      const src = ac.createMediaStreamSource(stream);
      const analyser = ac.createAnalyser();
      analyser.fftSize = 256; // Smaller for faster response
      src.connect(analyser);
      const data = new Uint8Array(analyser.frequencyBinCount);
      let streak = 0;
      const tick = () => {
        if (!this._inRoom || (this._roomState && this._roomState.activeCallId)) return;
        analyser.getByteFrequencyData(data);
        let sum = 0; data.forEach(v => sum += v * v); // Energy
        const rms = Math.sqrt(sum / data.length);
        console.log('RMS:', rms);
        if (rms > 10) streak++; else streak = 0; // Lower threshold
        if (streak > 3) { // Shorter streak
          console.log('Speech detected! Starting call');
          stream.getTracks().forEach(t => t.stop());
          this._monitorStream = null;
          ac.close();
          this.attemptStartRoomCall(video);
          this._monitoring = false;
          return;
        }
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }
    async recordVoiceMessage(){
      try{
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
        await this.captureMedia(stream, { mimeType: 'audio/webm' }, 60_000, 'voice.webm');
      }catch(e){ console.warn('Voice record unavailable', e); }
    }
    async recordVideoMessage(){
      try{
        const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
        const indicator = document.getElementById('recording-indicator');
        if (indicator){ indicator.classList.remove('hidden'); indicator.querySelector('i').className = 'fas fa-video'; }
        await this.captureMedia(stream, { mimeType: 'video/webm;codecs=vp9' }, 30_000, 'video.webm');
        if (indicator){ indicator.classList.add('hidden'); }
      }catch(e){ console.warn('Video record unavailable', e); }
    }

    async captureMedia(stream, options, maxMs, filename){
      return new Promise((resolve)=>{
        const rec = new MediaRecorder(stream, options);
        const chunks = [];
        let stopped = false;
        const stopAll = ()=>{ if (stopped) return; stopped = true; try{ rec.stop(); }catch(_){} try{ stream.getTracks().forEach(t=> t.stop()); }catch(_){} };
        this._activeRecorder = rec; this._activeStream = stream; this._recStop = stopAll;
        rec.ondataavailable = (e)=>{ if (e.data && e.data.size) chunks.push(e.data); };
        rec.onstop = async ()=>{
          try{
            const blob = new Blob(chunks, { type: options.mimeType || 'application/octet-stream' });
            // stage for review
            this._pendingRecording = { blob, type: (options.mimeType||'application/octet-stream'), filename };
            this.showRecordingReview(blob, filename);
          }catch(_){ }
          this._activeRecorder = null; this._activeStream = null; this._recStop = null;
          resolve();
        };
        rec.start();
        setTimeout(stopAll, maxMs);
      });
    }

    // Add debug method to check Firebase config
    async debugFirebaseConfig() {
      console.log('=== Firebase Configuration Debug ===');
      console.log('Firebase Service initialized:', !!window.firebaseService);
      console.log('Firebase Service isInitialized:', window.firebaseService?.isInitialized);
      
      if (window.firebaseService) {
        console.log('Firebase App:', window.firebaseService.app?.name);
        console.log('Firebase Auth:', !!window.firebaseService.auth);
        console.log('Firebase Firestore:', !!window.firebaseService.db);
        console.log('Firebase Storage:', !!window.firebaseService.storage);
        
        if (window.firebaseService.storage) {
          console.log('Storage Bucket:', window.firebaseService.storage._bucket);
          console.log('Storage App:', window.firebaseService.storage.app?.name);
        }
        
        console.log('Current User:', !!window.firebaseService.auth?.currentUser);
        if (window.firebaseService.auth?.currentUser) {
          console.log('User authenticated');
                      console.log('User email verified');
        }
      }
      
      // Check secure keys
      try {
        const keys = await window.secureKeyManager?.getKeys();
        console.log('Firebase Config in Keys:', !!keys?.firebase);
        if (keys?.firebase) {
          console.log('Firebase config loaded');
          console.log('Has storageBucket:', !!keys.firebase.storageBucket);
          if (keys.firebase.storageBucket) {
            console.log('Storage bucket configured');
          }
        }
      } catch (err) {
        console.error('Failed to check secure keys:', err);
      }
      
      console.log('=== End Debug ===');
    }

    async cleanupActiveCall(endRoom = false, reason = 'unknown'){
      console.log('Cleaning up active call', reason);
    this._startingCall = false;
    this._joiningCall = false;
    this._lastJoinedCallId = null;
      this._activePCs.forEach((p, uid) => {
        console.log('Stopping stream for ' + uid);
        try{ const key = (this._activeCid||'')+':'+uid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); } }catch(_){ }
        try{ p.unsubs.forEach(u => u()); }catch(_){ }
        try{ p.pc.close(); }catch(_){ }
        try{ p.stream.getTracks().forEach(t => { t.stop(); console.log('Stopped track ' + t.kind); }); }catch(_){ }
        if (p.videoEl) p.videoEl.remove();
      });
      this._activePCs.clear();
      if (this._inactTimer) clearInterval(this._inactTimer);
      if (endRoom){
        const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
        await firebase.updateDoc(roomRef, { status: 'idle', activeCallId: null });
      }
      await this.updatePresence('idle', false);
      if (this._monitorStream) {
        console.log('Stopping monitor stream');
        this._monitorStream.getTracks().forEach(t => { t.stop(); console.log('Stopped monitor track ' + t.kind); });
        this._monitorStream = null;
        this._monitoring = false;
      }
      document.querySelectorAll('#call-videos video').forEach(v => v.remove());
      if (!endRoom) this._inRoom = false;
      try {
        await navigator.mediaDevices.getUserMedia({audio: false});
      } catch (_) {}
      const audioEls = document.querySelectorAll('[id^="remoteAudio-"]');
      audioEls.forEach(el => el.remove());
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        devices.forEach(d => {
          if (d.kind === 'audioinput' && typeof d.stop === 'function') d.stop();
        });
      } catch (_) {}
    }

    async updatePresence(state, hasVideo = false){
      const ref = firebase.doc(this.db,'callRooms', this.activeConnection, 'peers', this.currentUser.uid);
      await firebase.setDoc(ref, { uid: this.currentUser.uid, state, hasVideo, updatedAt: new Date().toISOString() }, { merge: true });
    }

    async updateRoomUI(){
      const cont = document.getElementById('call-participants');
      if (!cont) return;
      cont.innerHTML = '';
      try {
        const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
        if (!connSnap.exists()) return;
        const conn = connSnap.data();
        // Deduplicate once
        const uniq = Array.from(new Set((conn.participants || []).filter(Boolean)));
        const fetches = uniq.map(async uid => {
          if (uid === this.currentUser.uid) return null;
          const p = this._peersPresence[uid] || { state: 'idle', hasVideo: false };
          let cached = this.usernameCache.get(uid);
          if (!cached || !cached.avatarUrl) {
            try {
              const u = await window.firebaseService.getUserData(uid);
              cached = { username: u?.username || uid.slice(0,8), avatarUrl: u?.avatarUrl || '../../images/default-bird.png' };
              this.usernameCache.set(uid, cached);
            } catch (_) { cached = { username: uid.slice(0,8), avatarUrl: '../../images/default-bird.png' }; }
          }
          return { uid, p, cached };
        });
        const remotes = (await Promise.all(fetches)).filter(item => item);
        remotes.forEach(item => {
          try {
            const {uid, p, cached} = item;
            const av = document.createElement('div');
            av.className = `avatar ${p.state}`;
            av.setAttribute('data-uid', uid);
            av.innerHTML = `<img src="${cached.avatarUrl || '../../images/default-bird.png'}" alt="${cached.username}"/>`;
            cont.appendChild(av);
            // Video tile logic...
          } catch (err) {
            console.error('Error rendering remote participant:', err);
          }
        });
        // Self avatar (single)
        let selfCached = this.usernameCache.get(this.currentUser.uid);
        if (!selfCached || !selfCached.avatarUrl) {
          try {
            const u = await window.firebaseService.getUserData(this.currentUser.uid);
            selfCached = { username: u?.username || 'You', avatarUrl: u?.avatarUrl || '../../images/default-bird.png' };
            this.usernameCache.set(this.currentUser.uid, selfCached);
          } catch (_) { selfCached = { username: 'You', avatarUrl: '../../images/default-bird.png' }; }
        }
        const meState = (this._peersPresence[this.currentUser.uid] && this._peersPresence[this.currentUser.uid].state) || 'idle';
        const selfAv = document.createElement('div');
        selfAv.className = `avatar local ${meState}`;
        selfAv.setAttribute('data-uid', this.currentUser.uid);
        selfAv.innerHTML = `<img src="${selfCached.avatarUrl || '../../images/default-bird.png'}" alt="${selfCached.username}"/>`;
        cont.appendChild(selfAv);
      } catch (err) {
        console.error('UI update error:', err);
      }
      // statusEl
      const statusEl = document.getElementById('call-status');
      if (statusEl) {
        statusEl.textContent = this._roomState.activeCallId ? 'In call' : 'Waiting for speech...';
      }
    }

    async renegotiateCall(callId, video){
      this._activePCs.forEach(async (p, peerUid) => {
        const pc = p.pc;
        pc.addTransceiver('audio', { direction: 'sendrecv' });
        pc.addTransceiver('video', { direction: video ? 'sendrecv' : 'recvonly' });
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      });
    }
  }

  window.secureChatApp = new SecureChatApp();
})();

// Review helpers (attach safely to running instance to avoid global class reference errors)
if (window && window.secureChatApp && typeof window.secureChatApp.showRecordingReview !== 'function'){
window.secureChatApp.showRecordingReview = function(blob, filename){
  try{
    const review = document.getElementById('recording-review');
    const player = document.getElementById('recording-player');
    const sendBtn = document.getElementById('send-recording-btn');
    const discardBtn = document.getElementById('discard-recording-btn');
    const switchBtn = document.getElementById('switch-camera-btn');
    if (!review || !player || !sendBtn || !discardBtn) return;
    player.innerHTML = '';
    const url = URL.createObjectURL(blob);
    const isVideo = (blob.type||'').startsWith('video');
    const mediaEl = document.createElement(isVideo ? 'video' : 'audio');
    mediaEl.controls = true; mediaEl.src = url; mediaEl.style.maxWidth = '100%'; mediaEl.playsInline = true; if (isVideo){ mediaEl.classList.add('circular'); } player.appendChild(mediaEl);
    review.classList.remove('hidden');
    const input = document.getElementById('message-input'); if (input) input.style.display='none';
    const actionBtn = document.getElementById('action-btn'); if (actionBtn){ actionBtn.innerHTML = '<i class="fas fa-paper-plane"></i>'; }
    const self = window.secureChatApp;
    if (switchBtn){ switchBtn.style.display = isVideo ? 'inline-block':'none'; }
    if (switchBtn && isVideo){
      switchBtn.onclick = async ()=>{
        try{
          // Toggle between user/environment
          const currFacing = self._recFacing || 'user';
          const next = currFacing === 'user' ? 'environment' : 'user';
          self._recFacing = next;
          const newStream = await navigator.mediaDevices.getUserMedia({ audio:true, video:{ facingMode: next } });
          // restart capture
          await self.captureMedia(newStream, { mimeType: 'video/webm;codecs=vp9' }, 30_000, 'video.webm');
        }catch(_){ }
      };
    }
    sendBtn.onclick = async ()=>{
      console.log('Auth state before sending recording:', !!self.currentUser, firebase.auth().currentUser?.uid);
      
      // Validate storage availability
      if (!self.storage) {
        console.error('Storage not available for recording');
        alert('Recording upload not available - storage not configured. Please check Firebase configuration.');
        return;
      }
      
      console.log('Storage bucket configured');
      
      try {
        await firebase.auth().currentUser?.getIdToken(true);
        if (!firebase.auth().currentUser) throw new Error('Auth lost - please re-login');
      } catch (err) {
        console.error('Auth refresh failed before recording send:', err);
        alert('Auth error - please reload and re-login');
        return;
      }
      try {
        console.log('Sending recording:', filename);
        const aesKey = await self.getFallbackKey();
        const base64 = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(blob); });
        const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
        const safe = `chat/${self.activeConnection}/${Date.now()}_${filename}`;
        const sref = firebase.ref(self.storage, `${safe}.enc.json`);
        console.log('Recording upload started');
        await firebase.uploadBytes(sref, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
        const url2 = await firebase.getDownloadURL(sref);
        console.log('Recording upload completed');
        await self.saveMessage({ text: isVideo? '[video message]': '[voice message]', fileUrl: url2, fileName: filename });
      } catch (err) {
        console.error('Recording upload error:', err.code, err.message, err);
        if (err.code === 'storage/unauthorized') {
          alert('Recording upload failed: Storage access denied. Please check Firebase Storage rules.');
        } else if (err.code === 'storage/bucket-not-found') {
          alert('Recording upload failed: Storage bucket not found. Please check Firebase configuration.');
        } else {
          alert('Failed to send recording: ' + err.message);
        }
      }
      finally{
        review.classList.add('hidden'); player.innerHTML=''; if (input) input.style.display=''; self.refreshActionButton();
      }
    };
    discardBtn.onclick = ()=>{ review.classList.add('hidden'); player.innerHTML=''; if (input) input.style.display=''; self.refreshActionButton(); };
  }catch(_){ }
};
}

window.addEventListener('beforeunload', () => {
  if (secureChatApp._inRoom) secureChatApp.cleanupActiveCall(false);
  if (secureChatApp._monitorStream) {
    secureChatApp._monitorStream.getTracks().forEach(t => t.stop());
    secureChatApp._monitorStream = null;
  }
});
