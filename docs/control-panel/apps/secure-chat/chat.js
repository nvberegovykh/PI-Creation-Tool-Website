(() => {
  class SecureChatApp {
    constructor() {
      this.db = null;
      this.storage = null;
      this.currentUser = null;
      this.activeConnection = null;
      this.connections = [];
      this.sharedKeyCache = {}; // connId -> CryptoKey
      this.init();
    }

    async init() {
      // Wait for firebase
      let attempts = 0; while((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < 150){ await new Promise(r=>setTimeout(r,100)); attempts++; }
      if (!window.firebaseService || !window.firebaseService.isInitialized) return;
      this.db = window.firebaseService.db;
      this.storage = firebase.getStorage ? firebase.getStorage() : null;
      this.currentUser = await window.firebaseService.getCurrentUser();
      this.bindUI();
      await this.loadConnections();
    }

    bindUI(){
      document.getElementById('new-connection-btn').addEventListener('click', ()=> this.promptNewConnection());
      document.getElementById('send-btn').addEventListener('click', ()=> this.sendCurrent());
      document.getElementById('attach-btn').addEventListener('click', ()=> document.getElementById('file-input').click());
      document.getElementById('file-input').addEventListener('change', (e)=> this.sendFiles(e.target.files));
      document.getElementById('user-search').addEventListener('input', (e)=> this.searchUsers(e.target.value.trim()));
      document.getElementById('message-search').addEventListener('input', (e)=> this.searchMessages(e.target.value.trim()));
    }

    async promptNewConnection(){
      const name = prompt('Enter username to connect');
      if (!name) return;
      const results = await window.firebaseService.searchUsers(name.toLowerCase());
      const exact = (results||[]).find(u=> (u.username||'').toLowerCase() === name.toLowerCase());
      if (!exact) return alert('User not found');
      const connId = [this.currentUser.uid, exact.uid||exact.id].sort().join('_');
      const connRef = firebase.doc(this.db,'chatConnections',connId);
      await firebase.setDoc(connRef,{
        id: connId,
        participants:[this.currentUser.uid, exact.uid||exact.id],
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        lastMessage:''
      },{merge:true});
      await this.loadConnections();
      this.setActive(connId, exact.username||exact.email);
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
        li.textContent = c.id.replace(this.currentUser.uid,'').replace(/_/g,'').slice(0,32);
        li.addEventListener('click',()=> this.setActive(c.id));
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
      const q = firebase.query(
        firebase.collection(this.db,'chatMessages',this.activeConnection,'messages'),
        firebase.orderBy('createdAt','asc'),
        firebase.limit(200)
      );
      const aesKey = await this.getOrCreateSharedAesKey();
      const snap = await firebase.getDocs(q);
      snap.forEach(async d=>{
        const m=d.data();
        let text='';
        try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey);}catch{ text='[unable to decrypt]'; }
        const el = document.createElement('div');
        el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
        el.innerHTML = `<div>${this.renderText(text)}</div>${m.fileName?`<div><a href="${m.fileUrl}" target="_blank">${m.fileName}</a></div>`:''}<div class="meta">${new Date(m.createdAt).toLocaleString()}</div>`;
        box.appendChild(el);
      });
      box.scrollTop = box.scrollHeight;
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
        const array = new Uint8Array(await f.arrayBuffer());
        const aesKey = await this.getOrCreateSharedAesKey();
        // Encrypt file content (base64 of bytes) with shared AES key
        const cipher = await chatCrypto.encryptWithKey(btoa(String.fromCharCode(...array)), aesKey);
        const blob = new Blob([JSON.stringify(cipher)], {type:'application/octet-stream'});
        const s = this.storage; if (!s) continue;
        const r = firebase.ref(s, `chat/${this.activeConnection}/${Date.now()}_${f.name}.enc`);
        await firebase.uploadBytes(r, blob);
        const url = await firebase.getDownloadURL(r);
        await this.saveMessage({text:`[file] ${f.name}`, fileUrl:url, fileName:f.name});
      }
    }

    async saveMessage({text,fileUrl,fileName}){
      const aesKey = await this.getOrCreateSharedAesKey();
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
      await this.loadMessages();
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
      const parts = this.activeConnection.split('_');
      const peerUid = parts[0] === myUid ? parts[1] : parts[0];

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

    async searchUsers(term){
      const resultsEl = document.getElementById('user-results');
      if (!resultsEl) return;
      resultsEl.innerHTML = '';
      if (!term){ resultsEl.style.display='none'; return; }
      try{
        if (window.firebaseService && window.firebaseService.isFirebaseAvailable()){
          const users = await window.firebaseService.searchUsers(term.toLowerCase());
          const filtered = (users||[]).filter(u=> u.uid !== (this.currentUser&&this.currentUser.uid));
          if (filtered.length === 0){ resultsEl.style.display='none'; return; }
          filtered.slice(0,20).forEach(u=>{
            const li=document.createElement('li');
            li.textContent = `${u.username||u.email}`;
            li.addEventListener('click', async ()=>{
              resultsEl.style.display='none';
              document.getElementById('user-search').value = '';
              // Create/open connection with selected user
              const connId = [this.currentUser.uid, u.uid||u.id].sort().join('_');
              const connRef = firebase.doc(this.db,'chatConnections',connId);
              await firebase.setDoc(connRef,{
                id: connId,
                participants:[this.currentUser.uid, u.uid||u.id],
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                lastMessage:''
              },{merge:true});
              await this.loadConnections();
              this.setActive(connId, u.username||u.email);
            });
            resultsEl.appendChild(li);
          });
          resultsEl.style.display='block';
        }
      }catch(e){ console.warn('User search failed', e); resultsEl.style.display='none'; }
    }

    async searchMessages(term){
      term = term.toLowerCase();
      const items = document.querySelectorAll('.messages .message');
      items.forEach(i=>{
        const vis = i.textContent.toLowerCase().includes(term);
        i.style.opacity = vis? '1':'0.3';
      });
    }
  }

  window.secureChatApp = new SecureChatApp();
})();


