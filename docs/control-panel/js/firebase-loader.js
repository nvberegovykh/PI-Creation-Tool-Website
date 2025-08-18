// Dynamic Firebase SDK loader with version fallback
// Attempts latest first, falls back if CDN path is unavailable

const FIREBASE_VERSIONS = [
	'12.1.0',
	'13.1.0'
];

async function loadFirebaseVersion(version) {
	const base = `https://www.gstatic.com/firebasejs/${version}`;

	const appMod = await import(`${base}/firebase-app.js`);
	const authMod = await import(`${base}/firebase-auth.js`);
	const fsMod = await import(`${base}/firebase-firestore.js`);
	const storageMod = await import(`${base}/firebase-storage.js`);

	const {
		initializeApp
	} = appMod;

	const {
		getAuth,
		createUserWithEmailAndPassword,
		signInWithEmailAndPassword,
		sendPasswordResetEmail,
		sendEmailVerification,
		onAuthStateChanged,
		fetchSignInMethodsForEmail,
		verifyPasswordResetCode,
		confirmPasswordReset,
		updatePassword,
		browserLocalPersistence,
		setPersistence,
		signOut,
		updateProfile,
		deleteUser,
		reauthenticateWithCredential,
		EmailAuthProvider
	} = authMod;

	const {
		getFirestore,
		enableIndexedDbPersistence,
		enableMultiTabIndexedDbPersistence,
		serverTimestamp,
		collection,
		doc,
		setDoc,
		getDoc,
		getDocs,
		query,
		where,
		orderBy,
		enableNetwork,
		disableNetwork,
		updateDoc,
		increment,
		limit,
		startAfter,
		deleteDoc
	} = fsMod;

	const {
		getStorage,
		ref,
		uploadBytes,
		getDownloadURL,
		deleteObject
	} = storageMod;

	// Expose compat-style object expected by existing code
	window.firebase = {
		initializeApp,
		auth: getAuth,
		firestore: getFirestore,
		SDK_VERSION: version,
		// Auth
		createUserWithEmailAndPassword,
		signInWithEmailAndPassword,
		sendPasswordResetEmail,
		sendEmailVerification,
		onAuthStateChanged,
		fetchSignInMethodsForEmail,
		verifyPasswordResetCode,
		confirmPasswordReset,
		updatePassword,
		browserLocalPersistence,
		setPersistence,
		signOut,
		updateProfile,
		deleteUser,
		reauthenticateWithCredential,
		EmailAuthProvider,
		// Firestore
		collection,
		doc,
		setDoc,
		getDoc,
		getDocs,
		query,
		where,
		orderBy,
		enableNetwork,
		disableNetwork,
		updateDoc,
		increment,
		limit,
		startAfter,
		deleteDoc,
		enableIndexedDbPersistence,
		enableMultiTabIndexedDbPersistence,
		serverTimestamp,
		// Storage
		getStorage,
		ref,
		uploadBytes,
		getDownloadURL,
		deleteObject
	};

	// Also expose modular functions directly
	window.firebaseModular = {
		initializeApp,
		getAuth,
		getFirestore,
		createUserWithEmailAndPassword,
		signInWithEmailAndPassword,
		sendPasswordResetEmail,
		sendEmailVerification,
		onAuthStateChanged,
		fetchSignInMethodsForEmail,
		verifyPasswordResetCode,
		confirmPasswordReset,
		updatePassword,
		browserLocalPersistence,
		setPersistence,
		signOut,
		updateProfile,
		deleteUser,
		reauthenticateWithCredential,
		EmailAuthProvider,
		collection,
		doc,
		setDoc,
		getDoc,
		getDocs,
		query,
		where,
		orderBy,
		enableNetwork,
		disableNetwork,
		updateDoc,
		increment,
		limit,
		startAfter,
		deleteDoc,
		enableIndexedDbPersistence,
		enableMultiTabIndexedDbPersistence,
		serverTimestamp,
		getStorage,
		ref,
		uploadBytes,
		getDownloadURL,
		deleteObject
	};

	console.log(`✅ Firebase Modular SDK v${version} loaded successfully`);
	console.log('Available services: Auth, Firestore');
}

(async () => {
	let lastError = null;
	for (const v of FIREBASE_VERSIONS) {
		try {
			await loadFirebaseVersion(v);
			return; // success
		} catch (e) {
			lastError = e;
			console.warn(`⚠️ Failed to load Firebase SDK v${v}:`, e?.message || e);
		}
	}
	console.error('❌ All Firebase SDK versions failed to load');
	window.firebaseLoadError = lastError;
})();


