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
      this.init();
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
      this.storage = firebase.getStorage ? firebase.getStorage() : null;
      this.currentUser = await window.firebaseService.getCurrentUser();
      try { this.me = await window.firebaseService.getUserData(this.currentUser.uid); } catch { this.me = null; }
      this.bindUI();
      await this.loadConnections();
    }

    bindUI(){
      document.getElementById('new-connection-btn').addEventListener('click', ()=> this.promptNewConnection());
      const actionBtn = document.getElementById('action-btn');
      actionBtn.addEventListener('click', ()=> this.handleActionButton());
      actionBtn.addEventListener('mousedown', (e)=> this.handleActionPressStart(e));
      actionBtn.addEventListener('touchstart', (e)=> this.handleActionPressStart(e));
      ['mouseup','mouseleave','touchend','touchcancel'].forEach(evt=> actionBtn.addEventListener(evt, ()=> this.handleActionPressEnd()));
      document.getElementById('attach-btn').addEventListener('click', ()=> document.getElementById('file-input').click());
      document.getElementById('file-input').addEventListener('change', (e)=> this.sendFiles(e.target.files));
      document.getElementById('user-search').addEventListener('input', (e)=> this.searchUsers(e.target.value.trim()));
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

    refreshActionButton(){
      const input = document.getElementById('message-input');
      const actionBtn = document.getElementById('action-btn');
      const hasContent = !!(input && input.value.trim().length);
      if (hasContent){
        actionBtn.title = 'Send';
        actionBtn.innerHTML = '<i class="fas fa-arrow-up"></i>';
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
      this._pressTimer = setTimeout(()=>{
        // TODO: start recording audio up to 60s (placeholder)
        console.log('Start recording (placeholder)');
      }, 400);
    }

    handleActionPressEnd(){
      if (this._pressTimer){ clearTimeout(this._pressTimer); this._pressTimer = null; }
      // TODO: stop recording if active and send
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
          const participantUids = [this.currentUser.uid, ...members.map(m=> m.uid||m.id)];
          const participantNames = [ (this.me&&this.me.username) || (this.currentUser.email||'me'), ...members.map(m=> m.username||m.email) ];
          const newRef = firebase.doc(firebase.collection(this.db,'chatConnections'));
          const connId = newRef.id;
          await firebase.setDoc(newRef,{
            id: connId,
            participants: participantUids,
            participantUsernames: participantNames,
            pairKey: null,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            lastMessage:''
          });
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
      this.connections.forEach(c=>{
        const li = document.createElement('li');
        let label = c.id;
        const myNameLower = ((this.me && this.me.username) || '').toLowerCase();
        if (Array.isArray(c.participantUsernames) && c.participantUsernames.length){
          const other = c.participantUsernames.find(n=> (n||'').toLowerCase() !== myNameLower);
          if (other) label = other;
        }
        li.textContent = label;
        li.addEventListener('click',()=>{
          this.setActive(c.id);
          const sidebar = document.querySelector('.sidebar');
          if (sidebar) sidebar.classList.remove('open');
        });
        listEl.appendChild(li);
      });
    }

    async setActive(connId, displayName){
      this.activeConnection = connId;
      document.getElementById('active-connection-name').textContent = displayName||connId;
      await this.loadMessages();
    }

    async loadMessages(){
      const box = document.getElementById('messages');
      box.innerHTML='';
      if (!this.activeConnection) return;
      try{
        if (this._unsubMessages) { this._unsubMessages(); this._unsubMessages = null; }
        const q = firebase.query(
          firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'),
          firebase.orderBy('createdAt','asc'),
          firebase.limit(200)
        );
        const handleSnap = async (snap)=>{
          box.innerHTML='';
          let aesKey = await this.getFallbackKey();
          snap.forEach(async d=>{
            const m=d.data();
            let text='';
            try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey);}catch{
              try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch { text='[unable to decrypt]'; }
            }
            const el = document.createElement('div');
            el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
            const hasFile = !!m.fileUrl && !!m.fileName;
            // Render call invites as buttons
            let bodyHtml = this.renderText(text);
            const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
            if (callMatch){
              const kind = callMatch[1]; const callId = callMatch[2];
              const btnLabel = kind==='voice' ? 'Join voice call' : 'Join video call';
              bodyHtml = `<button class=\"btn secondary\" data-call-id=\"${callId}\" data-kind=\"${kind}\">${btnLabel}</button>`;
            }
            el.innerHTML = `<div>${bodyHtml}</div>${hasFile?`<div class=\"file-link\"><a href=\"${m.fileUrl}\" target=\"_blank\" rel=\"noopener noreferrer\">${m.fileName}</a></div><div class=\"file-preview\"></div>`:''}<div class=\"meta\">${new Date(m.createdAt).toLocaleString()}</div>`;
            box.appendChild(el);
            const joinBtn = el.querySelector('button[data-call-id]');
            if (joinBtn){ joinBtn.addEventListener('click', ()=> this.answerCall(joinBtn.dataset.callId, { video: joinBtn.dataset.kind === 'video' })); }
            if (hasFile){
              const preview = el.querySelector('.file-preview');
              if (preview) this.renderEncryptedAttachment(preview, m.fileUrl, m.fileName, aesKey);
            }
          });
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
          const hasFile = !!m.fileUrl && !!m.fileName;
          el.innerHTML = `<div>${this.renderText(text)}</div>${hasFile?`<div class="file-link"><a href="${m.fileUrl}" target="_blank" rel="noopener noreferrer">${m.fileName}</a></div><div class="file-preview"></div>`:''}<div class="meta">${new Date(m.createdAt).toLocaleString()}</div>`;
          box.appendChild(el);
          if (hasFile){
            const preview = el.querySelector('.file-preview');
            if (preview) this.renderEncryptedAttachment(preview, m.fileUrl, m.fileName, aesKey);
          }
        });
        box.scrollTop = box.scrollHeight;
      }
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
      if (!files || !files.length || !this.activeConnection) return;
      for (const f of files){
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
        const s = this.storage; if (!s) continue;
        const safeName = f.name.replace(/[^a-zA-Z0-9._-]/g,'_');
        const r = firebase.ref(s, `chat/${this.activeConnection}/${Date.now()}_${safeName}.enc.json`);
        await firebase.uploadBytes(r, blob, { contentType: 'application/json' });
        const url = await firebase.getDownloadURL(r);
        await this.saveMessage({text:`[file] ${f.name}`, fileUrl:url, fileName:f.name});
      }
    }

    async saveMessage({text,fileUrl,fileName}){
      const aesKey = await this.getFallbackKey();
      const cipher = await chatCrypto.encryptWithKey(text, aesKey);
      const msgRef = firebase.doc(firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'));
      await firebase.setDoc(msgRef,{
        id: msgRef.id,
        sender: this.currentUser.uid,
        cipher,
        fileUrl: fileUrl||null,
        fileName: fileName||null,
        createdAt: new Date().toISOString()
      });
      await firebase.updateDoc(firebase.doc(this.db,'chatConnections',this.activeConnection),{
        lastMessage: text.slice(0,200),
        updatedAt: new Date().toISOString()
      });
      // Notify recipients (best-effort)
      this.notifyParticipants(text);
      await this.loadMessages();
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

      // Email notification (serverless via Mailgun from client config). Only send to peer(s), not self
      try{
        const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
        const data = connSnap.exists()? connSnap.data():null;
        const participantUids = (data && Array.isArray(data.participants)) ? data.participants : [];
        for (const uid of participantUids){
          if (uid === this.currentUser.uid) continue;
          try{
            const u = await window.firebaseService.getUserData(uid);
            const email = u && u.email;
            if (email && window.emailService && typeof window.emailService.sendEmail === 'function'){
              const safeText = plaintext.replace(/[<>]/g,'');
              const html = `<p>You have a new message</p><p>${safeText}</p>`;
              window.emailService.sendEmail(email, 'New message on LIBER/Connections', html).catch(()=>{});
            }
          }catch(_){/* ignore per-user errors */}
        }
      }catch(_){/* ignore email errors */}
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
          const filtered = (users||[]).filter(u=> u.uid !== (this.currentUser&&this.currentUser.uid));
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
                const pairKey = [myName.toLowerCase(), (u.username||'').toLowerCase()].sort().join('|');
                let existingId = null;
                try {
                  const q = firebase.query(
                    firebase.collection(this.db,'chatConnections'),
                    firebase.where('pairKey','==', pairKey),
                    firebase.limit(1)
                  );
                  const snap = await firebase.getDocs(q);
                  snap.forEach(d=> existingId = d.id);
                } catch {}
                let connId = existingId;
                if (!connId){
                  const newRef = firebase.doc(firebase.collection(this.db,'chatConnections'));
                  connId = newRef.id;
                  await firebase.setDoc(newRef,{
                    id: connId,
                    participants:[this.currentUser.uid, u.uid||u.id],
                    participantUsernames:[myName, u.username||u.email],
                    pairKey,
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString(),
                    lastMessage:''
                  });
                }
                await this.loadConnections();
                this.setActive(connId, u.username||u.email);
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
        pc.ontrack = (e)=>{ if (rv){ rv.srcObject = e.streams[0]; } };
        if (ov){ ov.classList.remove('hidden'); }
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'offer', candidate:e.candidate.toJSON() }); }};
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
        if (endBtn){ endBtn.onclick = ()=>{ try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ stream.getVideoTracks().forEach(t=> t.enabled = !t.enabled); }; }
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
        pc.ontrack = (e)=>{ if (rv){ rv.srcObject = e.streams[0]; } };
        if (ov){ ov.classList.remove('hidden'); }
        const answersRef = firebase.collection(this.db,'calls',callId,'answers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'answer', candidate:e.candidate.toJSON() }); }};
        const offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers','offer'));
        if (!offerDoc.exists()) return;
        const offer = offerDoc.data();
        await pc.setRemoteDescription(new RTCSessionDescription({ type:'offer', sdp: offer.sdp }));
        const answer = await pc.createAnswer(); await pc.setLocalDescription(answer);
        await firebase.setDoc(firebase.doc(answersRef,'answer'), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString() });
        const endBtn = document.getElementById('end-call-btn');
        const micBtn = document.getElementById('toggle-mic-btn');
        const camBtn = document.getElementById('toggle-camera-btn');
        if (endBtn){ endBtn.onclick = ()=>{ try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ stream.getVideoTracks().forEach(t=> t.enabled = !t.enabled); }; }
      }catch(e){ console.warn('Answer call failed', e); }
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
        await this.captureMedia(stream, { mimeType: 'video/webm;codecs=vp9' }, 30_000, 'video.webm');
      }catch(e){ console.warn('Video record unavailable', e); }
    }

    async captureMedia(stream, options, maxMs, filename){
      return new Promise((resolve)=>{
        const rec = new MediaRecorder(stream, options);
        const chunks = [];
        let stopped = false;
        const stopAll = ()=>{ if (stopped) return; stopped = true; rec.stop(); stream.getTracks().forEach(t=> t.stop()); };
        rec.ondataavailable = (e)=>{ if (e.data && e.data.size) chunks.push(e.data); };
        rec.onstop = async ()=>{
          try{
            const blob = new Blob(chunks, { type: options.mimeType || 'application/octet-stream' });
            const buf = await blob.arrayBuffer();
            const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)));
            const aesKey = await this.getFallbackKey();
            const cipher = await chatCrypto.encryptWithKey(b64, aesKey);
            const s = this.storage; if (!s) return resolve();
            const r = firebase.ref(s, `chat/${this.activeConnection}/${Date.now()}_${filename}.enc.json`);
            await firebase.uploadBytes(r, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
            const url = await firebase.getDownloadURL(r);
            await this.saveMessage({text:`[file] ${filename}`, fileUrl:url, fileName:filename});
          }catch(_){}
          resolve();
        };
        rec.start();
        setTimeout(stopAll, maxMs);
      });
    }
  }

  window.secureChatApp = new SecureChatApp();
})();


