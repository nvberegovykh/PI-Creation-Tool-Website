/**
 * When chat runs in iframe: wait for parent's Firebase and expose it.
 * Avoids loading our own firebase scripts, preventing SDK instance mismatch.
 */
(function(){
  if (window.self === window.top) return;
  function apply() {
    try {
      var p = window.parent;
      if (p && p.firebaseService && p.firebaseService.isInitialized && p.firebaseService.db && p.firebase) {
        window.firebaseService = p.firebaseService;
        window.firebase = p.firebase;
        if (p.firebaseModular) window.firebaseModular = p.firebaseModular;
        return true;
      }
    } catch (_) {}
    return false;
  }
  if (apply()) return;
  var attempts = 0;
  var iv = setInterval(function(){
    if (apply() || ++attempts > 100) {
      clearInterval(iv);
    }
  }, 100);
})();
