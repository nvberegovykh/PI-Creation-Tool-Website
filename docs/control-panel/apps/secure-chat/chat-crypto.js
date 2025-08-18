// Chat encryption built on the existing crypto manager
class ChatCrypto {
    constructor() {
        this.ivLength = 12;
        this.identityCache = {};
    }

    async deriveChatKey(secret) {
        const encoder = new TextEncoder();
        const keyMaterial = await crypto.subtle.importKey('raw', encoder.encode(secret), {name:'PBKDF2'}, false, ['deriveKey']);
        return crypto.subtle.deriveKey(
            {name:'PBKDF2', salt: encoder.encode('liber_chat_salt_v1'), iterations: 100000, hash:'SHA-256'},
            keyMaterial,
            {name:'AES-GCM', length:256},
            true,
            ['encrypt','decrypt']
        );
    }

    randomIV(){
        const iv = new Uint8Array(this.ivLength);
        crypto.getRandomValues(iv);
        return iv;
    }

    async encryptMessage(plaintext, secret){
        const key = await this.deriveChatKey(secret);
        const iv = this.randomIV();
        const encoded = new TextEncoder().encode(plaintext);
        const ct = await crypto.subtle.encrypt({name:'AES-GCM', iv}, key, encoded);
        return {
            iv: Array.from(iv, b=>b.toString(16).padStart(2,'0')).join(''),
            data: Array.from(new Uint8Array(ct), b=>b.toString(16).padStart(2,'0')).join('')
        };
    }

    async decryptMessage(cipher, secret){
        const key = await this.deriveChatKey(secret);
        const iv = new Uint8Array(cipher.iv.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const data = new Uint8Array(cipher.data.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const pt = await crypto.subtle.decrypt({name:'AES-GCM', iv}, key, data);
        return new TextDecoder().decode(pt);
    }

    // === New E2EE identity and shared-key helpers (ECDH P-256 + HKDF → AES-GCM) ===
    async loadOrCreateIdentity(uid){
        if (this.identityCache[uid]) return this.identityCache[uid];
        const pubKeyKey = `secure_chat_pub_${uid}_v1`;
        const privKeyKey = `secure_chat_priv_${uid}_v1`;
        let pubJwk = null, encPriv = null;
        try { pubJwk = JSON.parse(localStorage.getItem(pubKeyKey)||'null'); } catch {}
        try { encPriv = JSON.parse(localStorage.getItem(privKeyKey)||'null'); } catch {}
        if (!pubJwk || !encPriv){
            const pair = await crypto.subtle.generateKey({name:'ECDH', namedCurve:'P-256'}, true, ['deriveBits']);
            const publicJwk = await crypto.subtle.exportKey('jwk', pair.publicKey);
            const privateJwk = await crypto.subtle.exportKey('jwk', pair.privateKey);
            const enc = await this.encryptJsonForDevice(privateJwk, uid);
            localStorage.setItem(pubKeyKey, JSON.stringify(publicJwk));
            localStorage.setItem(privKeyKey, JSON.stringify(enc));
            this.identityCache[uid] = { publicJwk: publicJwk, privateJwkEncrypted: enc };
            return this.identityCache[uid];
        }
        this.identityCache[uid] = { publicJwk: pubJwk, privateJwkEncrypted: encPriv };
        return this.identityCache[uid];
    }

    async getPrivateKey(uid){
        const id = await this.loadOrCreateIdentity(uid);
        const privJwk = await this.decryptJsonForDevice(id.privateJwkEncrypted, uid);
        return crypto.subtle.importKey('jwk', privJwk, {name:'ECDH', namedCurve:'P-256'}, true, ['deriveBits']);
    }

    async getPublicKeyFromJwk(jwk){
        return crypto.subtle.importKey('jwk', jwk, {name:'ECDH', namedCurve:'P-256'}, true, []);
    }

    async deriveSharedAesKey(myPrivateKey, peerPublicJwk){
        const peerPubKey = await this.getPublicKeyFromJwk(peerPublicJwk);
        const sharedBits = await crypto.subtle.deriveBits({name:'ECDH', public: peerPubKey}, myPrivateKey, 256);
        // HKDF to AES-GCM
        const hkdfKey = await crypto.subtle.importKey('raw', sharedBits, 'HKDF', false, ['deriveKey']);
        return crypto.subtle.deriveKey(
            { name: 'HKDF', salt: new TextEncoder().encode('liber_secure_chat_v1'), info: new TextEncoder().encode('conn_shared_key') },
            hkdfKey,
            { name: 'AES-GCM', length: 256 },
            false,
            ['encrypt','decrypt']
        );
    }

    async encryptWithKey(plaintext, aesKey){
        const iv = this.randomIV();
        const encoded = new TextEncoder().encode(plaintext);
        const ct = await crypto.subtle.encrypt({name:'AES-GCM', iv}, aesKey, encoded);
        return {
            iv: Array.from(iv, b=>b.toString(16).padStart(2,'0')).join(''),
            data: Array.from(new Uint8Array(ct), b=>b.toString(16).padStart(2,'0')).join('')
        };
    }

    async decryptWithKey(cipher, aesKey){
        const iv = new Uint8Array(cipher.iv.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const data = new Uint8Array(cipher.data.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const pt = await crypto.subtle.decrypt({name:'AES-GCM', iv}, aesKey, data);
        return new TextDecoder().decode(pt);
    }

    // Device-scoped encryption for private key persistence (PBKDF2 → AES-GCM)
    async encryptJsonForDevice(obj, uid){
        const saltKey = 'secure_chat_device_salt_v1';
        let salt = localStorage.getItem(saltKey);
        if (!salt){
            const arr = new Uint8Array(16); crypto.getRandomValues(arr);
            salt = Array.from(arr).map(b=>b.toString(16).padStart(2,'0')).join('');
            localStorage.setItem(saltKey, salt);
        }
        const material = await crypto.subtle.importKey('raw', new TextEncoder().encode(`${uid}:${salt}`), {name:'PBKDF2'}, false, ['deriveKey']);
        const aes = await crypto.subtle.deriveKey({name:'PBKDF2', salt:new TextEncoder().encode('secure_chat_identity_v1'), iterations:100000, hash:'SHA-256'}, material, {name:'AES-GCM', length:256}, false, ['encrypt','decrypt']);
        const iv = this.randomIV();
        const pt = new TextEncoder().encode(JSON.stringify(obj));
        const ct = await crypto.subtle.encrypt({name:'AES-GCM', iv}, aes, pt);
        return { iv: Array.from(iv, b=>b.toString(16).padStart(2,'0')).join(''), data: Array.from(new Uint8Array(ct), b=>b.toString(16).padStart(2,'0')).join('') };
    }

    async decryptJsonForDevice(payload, uid){
        const saltKey = 'secure_chat_device_salt_v1';
        const salt = localStorage.getItem(saltKey) || '';
        const material = await crypto.subtle.importKey('raw', new TextEncoder().encode(`${uid}:${salt}`), {name:'PBKDF2'}, false, ['deriveKey']);
        const aes = await crypto.subtle.deriveKey({name:'PBKDF2', salt:new TextEncoder().encode('secure_chat_identity_v1'), iterations:100000, hash:'SHA-256'}, material, {name:'AES-GCM', length:256}, false, ['encrypt','decrypt']);
        const iv = new Uint8Array(payload.iv.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const data = new Uint8Array(payload.data.match(/.{1,2}/g).map(h=>parseInt(h,16)));
        const pt = await crypto.subtle.decrypt({name:'AES-GCM', iv}, aes, data);
        return JSON.parse(new TextDecoder().decode(pt));
    }
}

window.chatCrypto = new ChatCrypto();


