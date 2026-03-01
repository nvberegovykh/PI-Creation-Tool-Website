(function(){
  if (window.LiberAutoUiTranslator) return;

  const CACHE_KEY = 'liber_ui_translate_cache_v2';
  const SOURCE_ATTR = 'data-i18n-source';
  let cache = null;
  let saveTimer = 0;
  const inFlightByLang = new Map();

  function normalizeLang(lang){
    const code = String(lang || 'en').trim().toLowerCase();
    return code || 'en';
  }

  function loadCache(){
    if (cache) return cache;
    try{
      const raw = localStorage.getItem(CACHE_KEY);
      cache = raw ? (JSON.parse(raw) || {}) : {};
    }catch(_){
      cache = {};
    }
    return cache;
  }

  function saveCacheSoon(){
    if (saveTimer) clearTimeout(saveTimer);
    saveTimer = setTimeout(()=>{
      saveTimer = 0;
      try{
        localStorage.setItem(CACHE_KEY, JSON.stringify(cache || {}));
      }catch(_){ }
    }, 120);
  }

  function getCacheKey(lang, text){
    return `${lang}::${text}`;
  }

  function shouldSkipElement(el){
    if (!(el instanceof Element)) return true;
    const tag = String(el.tagName || '').toLowerCase();
    if (['script','style','code','pre','noscript','svg','path'].includes(tag)) return true;
    if (el.matches('[data-no-auto-translate],[contenteditable="true"]')) return true;
    if (el.closest('[data-no-auto-translate],.message,.post-item,#global-feed,#space-feed,.messages,.chat-app,.code-block,[contenteditable="true"]')) return true;
    if (el.hasAttribute('data-i18n')) return true;
    return false;
  }

  function isTranslatableLeaf(el){
    if (shouldSkipElement(el)) return false;
    if (el.childElementCount > 0) return false;
    const text = String(el.getAttribute(SOURCE_ATTR) || el.textContent || '').trim();
    if (!text) return false;
    if (text.length < 2 || text.length > 180) return false;
    if (/^[\d\s.,:;!?()+\-/%#@&]+$/.test(text)) return false;
    return true;
  }

  function collectLeafNodes(root){
    const out = [];
    const base = (root instanceof Element || root instanceof Document) ? root : document.body;
    if (!base) return out;
    base.querySelectorAll('*').forEach((el)=>{
      if (!isTranslatableLeaf(el)) return;
      if (!el.getAttribute(SOURCE_ATTR)){
        const src = String(el.textContent || '').trim();
        if (src) el.setAttribute(SOURCE_ATTR, src);
      }
      const source = String(el.getAttribute(SOURCE_ATTR) || '').trim();
      if (!source) return;
      out.push({ el, source });
    });
    return out;
  }

  function extractJsonObject(text){
    const raw = String(text || '').trim();
    if (!raw) return null;
    try{ return JSON.parse(raw); }catch(_){ }
    const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    if (fenced && fenced[1]){
      try{ return JSON.parse(fenced[1]); }catch(_){ }
    }
    const obj = raw.match(/\{[\s\S]*\}/);
    if (obj && obj[0]){
      try{ return JSON.parse(obj[0]); }catch(_){ }
    }
    return null;
  }

  function getTranslatorEndpoints(){
    const projectId = String(window.firebaseService?.app?.options?.projectId || '').trim();
    if (!projectId) return [];
    const pref = String(localStorage.getItem('liber_functions_region') || '').trim();
    const regions = Object.keys(window.firebaseService?.functionsByRegion || {});
    const order = Array.from(new Set([pref, ...regions, 'europe-west1', 'us-central1'].filter(Boolean)));
    return order.map((r)=> `https://${r}-${projectId}.cloudfunctions.net/openaiProxy/v1/chat/completions`);
  }

  async function translateBatch(texts, targetLang){
    const list = Array.isArray(texts) ? texts.map((t)=> String(t || '').trim()).filter(Boolean) : [];
    if (!list.length) return {};
    const endpoints = getTranslatorEndpoints();
    if (!endpoints.length) return {};

    for (const endpoint of endpoints){
      try{
        const resp = await fetch(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            temperature: 0.1,
            messages: [
              {
                role: 'system',
                content: `You are a native professional UI translator. Translate each string into ${targetLang}. Keep meaning, tone, punctuation and concise UI style. Output strict JSON only with this shape: {"translations":["..."]}. No explanations.`
              },
              {
                role: 'user',
                content: JSON.stringify({ texts: list })
              }
            ]
          })
        });
        if (!resp.ok) continue;
        const data = await resp.json().catch(()=> ({}));
        const content = String(data?.choices?.[0]?.message?.content || '').trim();
        const parsed = extractJsonObject(content);
        const translations = Array.isArray(parsed?.translations) ? parsed.translations : null;
        if (!translations || translations.length !== list.length) continue;
        const out = {};
        for (let i = 0; i < list.length; i++){
          out[list[i]] = String(translations[i] || list[i]).trim() || list[i];
        }
        return out;
      }catch(_){ }
    }
    return {};
  }

  async function translateRoot(root, lang){
    const code = normalizeLang(lang);
    const runKey = `${code}::${root === document.body ? 'body' : 'node'}`;
    if (inFlightByLang.has(runKey)) return inFlightByLang.get(runKey);

    const p = (async ()=>{
      const nodes = collectLeafNodes(root || document.body);
      if (!nodes.length) return;
      const store = loadCache();

      // Restore original UI for English.
      if (code === 'en'){
        nodes.forEach(({ el, source })=>{ if (el && source) el.textContent = source; });
        return;
      }

      const uniq = [];
      const seen = new Set();
      nodes.forEach(({ source })=>{
        if (seen.has(source)) return;
        seen.add(source);
        uniq.push(source);
      });
      const missing = uniq.filter((s)=> !store[getCacheKey(code, s)]);
      const chunkSize = 20;
      for (let i = 0; i < missing.length; i += chunkSize){
        const chunk = missing.slice(i, i + chunkSize);
        const translated = await translateBatch(chunk, code);
        chunk.forEach((src)=>{
          const next = String(translated[src] || src).trim() || src;
          store[getCacheKey(code, src)] = next;
        });
        saveCacheSoon();
      }

      nodes.forEach(({ el, source })=>{
        const next = String(store[getCacheKey(code, source)] || source).trim() || source;
        if (el && next) el.textContent = next;
      });
    })().finally(()=>{
      inFlightByLang.delete(runKey);
    });

    inFlightByLang.set(runKey, p);
    return p;
  }

  window.LiberAutoUiTranslator = {
    translateRoot,
    clearCache: function(){
      cache = {};
      try{ localStorage.removeItem(CACHE_KEY); }catch(_){ }
    }
  };
})();

