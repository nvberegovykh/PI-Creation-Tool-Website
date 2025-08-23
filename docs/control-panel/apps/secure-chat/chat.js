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
      this.usernameCache = new Map(); // uid -> username
      this.userUnsubs = new Map(); // uid -> unsubscribe
      this.init();
    }

    computeConnKey(uids){
      try{ return (uids||[]).slice().sort().join('|'); }catch(_){ return ''; }
    }

    async findConnectionByKey(key){
      try{
        const q = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('key','==', key), firebase.limit(1));
        const s = await firebase.getDocs(q); let id=null; s.forEach(d=> id=d.id); return id;
      }catch(_){ return null; }
    }

    async getIceServers(){
      try{
        if (window.secureKeyManager && typeof window.secureKeyManager.getKeys === 'function'){
          const keys = await window.secureKeyManager.getKeys();
          const turn = keys && keys.turn;
          if (turn && Array.isArray(turn.uris) && turn.username && turn.credential){
            return [
              { urls: ['stun:stun.l.google.com:19302','stun:global.stun.twilio.com:3478'] },
              { urls: turn.uris, username: turn.username, credential: turn.credential }
            ];
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
    }

    bindUI(){
      document.getElementById('new-connection-btn').addEventListener('click', ()=> { this.groupBaseParticipants = null; this.promptNewConnection(); });
      const actionBtn = document.getElementById('action-btn');
      actionBtn.addEventListener('click', ()=> this.handleActionButton());
      actionBtn.addEventListener('mousedown', (e)=> this.handleActionPressStart(e));
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
      const voiceBtn = document.getElementById('voice-call-btn'); if (voiceBtn) voiceBtn.addEventListener('click', ()=> this.startVoiceCall());
      const videoBtn = document.getElementById('video-call-btn'); if (videoBtn) videoBtn.addEventListener('click', ()=> this.startVideoCall());
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
        const all = [];
        const q = firebase.query(
          firebase.collection(this.db,'chatConnections'),
          firebase.where('participants','array-contains', this.currentUser.uid)
        );
        const snap = await firebase.getDocs(q); snap.forEach(d=> all.push({ id:d.id, ...d.data() }));
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
          try{ await firebase.updateDoc(firebase.doc(this.db,'chatConnections', keep.id),{ key, updatedAt: new Date().toISOString() }); }catch(_){ }
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
      let snap;
      try{
        const q = firebase.query(
          firebase.collection(this.db,'chatConnections'),
          firebase.where('participants','array-contains', this.currentUser.uid),
          firebase.orderBy('updatedAt','desc')
        );
        snap = await firebase.getDocs(q);
        this.connections = [];
        snap.forEach(d=> this.connections.push(d.data()));
      } catch (e){
        // Fallback without orderBy if index missing; sort client-side
        const q2 = firebase.query(
          firebase.collection(this.db,'chatConnections'),
          firebase.where('participants','array-contains', this.currentUser.uid)
        );
        const snap2 = await firebase.getDocs(q2);
        const temp = [];
        snap2.forEach(d=> temp.push(d.data()));
        temp.sort((a,b)=> new Date(b.updatedAt||0) - new Date(a.updatedAt||0));
        this.connections = temp;
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
              let name = this.usernameCache.get(uid);
              if (!name) {
                try {
                  const u = await window.firebaseService.getUserData(uid);
                  name = (u && u.username) || u?.email || 'User ' + uid.slice(0,6);
                  this.usernameCache.set(uid, name);
                } catch (err) {
                  console.error('Failed to resolve username for', uid, err);
                  name = 'Unknown';
                }
              }
              enriched.push(name);
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
          const others = c.participantUsernames.filter(n=> (n||'').toLowerCase() !== myNameLower);
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
                      const others = names.filter(n => (n||'').toLowerCase() !== myNameLower);
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
          const parts = Array.isArray(data.participants) ? data.participants : [];
          // Prefer live cache, fallback to stored usernames
          const stored = Array.isArray(data.participantUsernames) ? data.participantUsernames : [];
          const names = parts.map((uid, i)=> this.usernameCache.get(uid) || stored[i] || uid);
          const myNameLower = (this.me?.username || '').toLowerCase();
          const others = names.filter(n => (n||'').toLowerCase() !== myNameLower);
          displayName = others.length === 1 ? others[0] : (others.slice(0,2).join(', ') + (others.length > 2 ? `, +${others.length-2}` : ''));
        }
        displayName = displayName || 'Chat';
      }
      document.getElementById('active-connection-name').textContent = displayName;
      await this.loadMessages();
      // Force scroll to bottom after messages render
      try{ const box=document.getElementById('messages'); if(box){ setTimeout(()=>{ box.scrollTop = box.scrollHeight; }, 50); } }catch(_){ }
      // If current user is not a participant of this connection, show banner to recreate with same users
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        if (snap.exists()){
          const data = snap.data();
          const parts = Array.isArray(data.participants)? data.participants:[];
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
          box.innerHTML='';
          let aesKey = await this.getFallbackKey();
          const renderOne = async (d)=>{
            const m=d.data();
            let text='';
            try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey);}catch{
              try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch { text='[unable to decrypt]'; }
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
            if (joinBtn){ joinBtn.addEventListener('click', ()=> this.answerCall(joinBtn.dataset.callId, { video: joinBtn.dataset.kind === 'video' })); }
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
          // Render primary messages
          snap.forEach(async d=>{ await renderOne(d); });
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
              s2.forEach(async d=>{ await renderOne(d); });
            }catch(_){ /* ignore per-archive failure */ }
          }
          // Scroll to last message
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
              try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch { text='[unable to decrypt]'; }
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
            el.innerHTML = `<div>${this.renderText(text)}</div>${hasFile?`<div class="file-link"><a href="${m.fileUrl}" target="_blank" rel="noopener noreferrer">${fileText}</a></div><div class="file-preview"></div>`:''}<div class="meta">${senderName} · ${new Date(m.createdAt).toLocaleString()}</div>`;
            box.appendChild(el);
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
      // Optional push notification via Firebase Functions (no-op if not configured)
      try{
        if (window.firebaseService && typeof window.firebaseService.callFunction === 'function'){
          const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
          const data = connSnap.exists()? connSnap.data():null;
          const participantUids = (data && Array.isArray(data.participants)) ? data.participants : [];
          await window.firebaseService.callFunction('sendPush', {
            connId: this.activeConnection,
            recipients: participantUids.filter(uid=> uid !== this.currentUser.uid),
            preview: text.slice(0, 120)
          });
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
      const peerUid = await this.getPeerUid();
      const connId = this.activeConnection;
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
        catch { const alt = await this.getOrCreateSharedAesKey(); b64 = await chatCrypto.decryptWithKey(payload, alt); }
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
          const frame = document.createElement('iframe');
          frame.src = url; frame.style.width = '100%'; frame.style.height = '380px'; frame.style.border = 'none';
          containerEl.appendChild(frame);
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
    async startVoiceCall(){ await this.startCall({ video:false }); }
    async startVideoCall(){ await this.startCall({ video:true }); }

    async startCall({ video }){
      try{
        const callId = `${this.activeConnection}_${Date.now()}`;
        const config = { audio: true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({ iceServers: await this.getIceServers() });
        stream.getTracks().forEach(t=> pc.addTrack(t, stream));
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; }
        pc.ontrack = (e)=>{ if (rv){ rv.srcObject = e.streams[0]; this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote'); } };
        if (ov){ ov.classList.remove('hidden'); }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'offer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        const offer = await pc.createOffer(); await pc.setLocalDescription(offer);
        await firebase.setDoc(firebase.doc(offersRef,'offer'), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
        await this.saveMessage({ text:`[call:${video?'video':'voice'}:${callId}]` });
        // Listen for answer
        if (firebase.onSnapshot){
          firebase.onSnapshot(firebase.doc(this.db,'calls',callId,'answers','answer'), async (doc)=>{
            if (doc.exists()){
              const data = doc.data();
              await pc.setRemoteDescription(new RTCSessionDescription({ type:'answer', sdp:data.sdp }));
            }
          });
          firebase.onSnapshot(candsRef, (snap)=>{
            snap.forEach(d=>{ const v=d.data(); if(v.type==='answer' && v.candidate){ pc.addIceCandidate(new RTCIceCandidate(v.candidate)); }});
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
      }catch(e){ console.warn('Call start failed', e); }
    }

    async answerCall(callId, { video }){
      try{
        const config = { audio:true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({ iceServers: await this.getIceServers() });
        stream.getTracks().forEach(t=> pc.addTrack(t, stream));
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; }
        pc.ontrack = (e)=>{ if (rv){ rv.srcObject = e.streams[0]; this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote'); } };
        if (ov){ ov.classList.remove('hidden'); }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const answersRef = firebase.collection(this.db,'calls',callId,'answers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'answer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        const offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers','offer'));
        if (!offerDoc.exists()) return;
        const offer = offerDoc.data();
        await pc.setRemoteDescription(new RTCSessionDescription({ type:'offer', sdp: offer.sdp }));
        const answer = await pc.createAnswer(); await pc.setLocalDescription(answer);
        await firebase.setDoc(firebase.doc(answersRef,'answer'), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
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
        const selfAvatar = (this.me && this.me.avatarUrl) || 'images/default-bird.png';
        const local = document.createElement('div'); local.className = 'avatar local connected'; local.innerHTML = `<img src="${selfAvatar}" alt="me"/>`;
        cont.appendChild(local);
        // Peer
        const peerUid = await this.getPeerUid();
        let peerAvatar = 'images/default-bird.png';
        try{ const d = await window.firebaseService.getUserData(peerUid); if (d && d.avatarUrl) peerAvatar = d.avatarUrl; }catch(_){ }
        const remote = document.createElement('div'); remote.className = 'avatar remote connected'; remote.innerHTML = `<img src="${peerAvatar}" alt="peer"/>`;
        cont.appendChild(remote);
      }catch(_){ }
    }

    _attachSpeakingDetector(stream, selector){
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
          if (avg > 30){ el.classList.add('speaking'); } else { el.classList.remove('speaking'); }
          rafId = requestAnimationFrame(tick);
        };
        tick();
        // Stop when stream ends
        const stop = ()=>{ try{ cancelAnimationFrame(rafId); }catch(_){} try{ ac.close(); }catch(_){} };
        stream.getTracks().forEach(t=> t.addEventListener('ended', stop));
      }catch(_){ }
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
