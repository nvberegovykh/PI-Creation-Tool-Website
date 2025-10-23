// Firestore-backed storage for Invoice Generator
class InvoiceStore {
    constructor() {
        this.db = null;
    }

    ensureReady() {
        if (!window.firebaseService || !window.firebaseService.isInitialized) {
            throw new Error('Firebase not initialized');
        }
        this.db = window.firebaseService.db;
    }

    // Utility: normalized id from name
    toId(name) {
        return (name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64) || ('item-' + Date.now());
    }

    // Providers
    async getProviders(uid) {
        this.ensureReady();
        const col = firebase.collection(this.db, 'users', uid, 'invoiceProviders');
        const snap = await firebase.getDocs(col);
        const items = [];
        snap.forEach(d => items.push(d.data()));
        return items;
    }

    async saveProvider(uid, provider) {
        this.ensureReady();
        const id = this.toId(provider.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceProviders', id);
        const payload = { id, name: provider.name, updatedAt: new Date().toISOString() };
        await firebase.setDoc(ref, payload, { merge: true });
        return true;
    }

    async deleteProvider(uid, provider) {
        this.ensureReady();
        // Try by id first, else by name
        let id = provider.id || this.toId(provider.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceProviders', id);
        try { await firebase.deleteDoc(ref); return true; } catch(_) { return false; }
    }

    // Clients
    async getClients(uid) {
        this.ensureReady();
        const col = firebase.collection(this.db, 'users', uid, 'invoiceClients');
        const snap = await firebase.getDocs(col);
        const items = [];
        snap.forEach(d => items.push(d.data()));
        return items;
    }

    async saveClient(uid, client) {
        this.ensureReady();
        const id = this.toId(client.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceClients', id);
        const payload = { id, name: client.name, updatedAt: new Date().toISOString() };
        await firebase.setDoc(ref, payload, { merge: true });
        return true;
    }

    async deleteClient(uid, client) {
        this.ensureReady();
        const id = client.id || this.toId(client.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceClients', id);
        try { await firebase.deleteDoc(ref); return true; } catch(_) { return false; }
    }

    // Services
    async getServices(uid) {
        this.ensureReady();
        const col = firebase.collection(this.db, 'users', uid, 'invoiceServices');
        const snap = await firebase.getDocs(col);
        const items = [];
        snap.forEach(d => items.push(d.data()));
        return items;
    }

    async saveService(uid, service) {
        this.ensureReady();
        const id = this.toId(service.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceServices', id);
        const payload = { id, name: service.name, cost: Number(service.cost||0), rate: (service.rate!=null? Number(service.rate): null), updatedAt: new Date().toISOString() };
        await firebase.setDoc(ref, payload, { merge: true });
        return true;
    }

    async deleteService(uid, service) {
        this.ensureReady();
        const id = service.id || this.toId(service.name);
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceServices', id);
        try { await firebase.deleteDoc(ref); return true; } catch(_) { return false; }
    }

    // Invoices
    async getInvoices(uid) {
        this.ensureReady();
        let q;
        try {
            q = firebase.query(
                firebase.collection(this.db, 'users', uid, 'invoices'),
                firebase.orderBy('createdAtTS', 'desc')
            );
        } catch(_) {
            q = firebase.query(firebase.collection(this.db, 'users', uid, 'invoices'));
        }
        const snap = await firebase.getDocs(q);
        const items = [];
        snap.forEach(d => items.push(d.data()));
        return items;
    }

    async getNextCounterPreview(uid) {
        this.ensureReady();
        const ref = firebase.doc(this.db, 'users', uid, 'invoiceMeta', 'counter');
        const snap = await firebase.getDoc(ref);
        const next = snap.exists() ? (snap.data().nextInvoiceNumber || 1) : 1;
        return next;
    }

    async saveInvoice(uid, data, onUpload) {
        this.ensureReady();
        // Atomically increment counter and write invoice
        const metaRef = firebase.doc(this.db, 'users', uid, 'invoiceMeta', 'counter');
        const invoicesCol = firebase.collection(this.db, 'users', uid, 'invoices');
        // Compute date components from data.invoiceDate
        function computeId(prefix, n){ return `${prefix}-${String(n).padStart(3,'0')}`; }
        const date = new Date();
        const y = date.getFullYear();
        const m = String(date.getMonth()+1).padStart(2,'0');
        const d = String(date.getDate()).padStart(2,'0');
        const base = `LIBER-${y}${m}${d}`;

        // Use a simple two-step approach to avoid requiring full transactions API exposure
        // 1) Read current
        let nextNum = 1;
        try {
            const snap = await firebase.getDoc(metaRef);
            nextNum = snap.exists() ? (Number(snap.data().nextInvoiceNumber||1)) : 1;
        } catch(_) { nextNum = 1; }
        const invoiceId = data.invoiceId && data.invoiceId.startsWith('LIBER-') ? data.invoiceId : computeId(base, nextNum);
        // 2) Write invoice
        const invRef = firebase.doc(this.db, 'users', uid, 'invoices', invoiceId);
        const payload = {
            ...data,
            invoiceId,
            id: invoiceId,
            ownerId: uid,
            createdAt: data.createdAt || new Date().toISOString(),
            createdAtTS: firebase.serverTimestamp()
        };
        await firebase.setDoc(invRef, payload, { merge: true });
        // 3) Bump counter
        try { await firebase.setDoc(metaRef, { nextInvoiceNumber: nextNum + 1, updatedAt: new Date().toISOString() }, { merge: true }); } catch(_){ }

        // Optional upload
        if (typeof onUpload === 'function') {
            try { await onUpload(`${invoiceId}.pdf`); } catch(_){ }
        }
        return true;
    }

    async deleteInvoice(uid, invoiceId) {
        this.ensureReady();
        try {
            const ref = firebase.doc(this.db, 'users', uid, 'invoices', invoiceId);
            await firebase.deleteDoc(ref);
            // Best-effort: also delete stored PDF
            try { await this.deleteInvoicePdf(uid, `${invoiceId}.pdf`); } catch(_){ }
            return true;
        } catch(_) { return false; }
    }

    // Storage helpers
    getStorage() {
        try { return window.firebase.getStorage(window.firebaseService.app); } catch(_) { return null; }
    }

    async uploadInvoicePdf(uid, fileName, blob) {
        const storage = this.getStorage(); if (!storage) return false;
        const path = `user-content/${uid}/invoices/${fileName}`;
        const r = window.firebase.ref(storage, path);
        await window.firebase.uploadBytes(r, blob);
        const url = await window.firebase.getDownloadURL(r);
        // Update invoice record with URL
        try {
            const id = fileName.replace(/\.pdf$/i, '');
            const ref = firebase.doc(this.db, 'users', uid, 'invoices', id);
            await firebase.updateDoc(ref, { pdfUrl: url, pdfPath: path, updatedAt: new Date().toISOString() });
        } catch(_){ }
        return true;
    }

    async deleteInvoicePdf(uid, fileName) {
        const storage = this.getStorage(); if (!storage) return false;
        const path = `user-content/${uid}/invoices/${fileName}`;
        const r = window.firebase.ref(storage, path);
        try { await window.firebase.deleteObject(r); return true; } catch(_) { return false; }
    }
}

window.invoiceStore = new InvoiceStore();


