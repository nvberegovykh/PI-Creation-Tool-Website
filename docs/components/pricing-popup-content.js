(function () {
  'use strict';
  function priceCard(name, amount, note) {
    const n = note ? `<p class="gc-price-note">${note}</p>` : '';
    return `<div class="gc-price-item"><p class="gc-price-name">${name}</p><h3 class="gc-price-amount">${amount}</h3>${n}</div>`;
  }

  function architecturePanel() {
    return (
      '<div class="gc-pricing-intro">' +
      '<p>Estimation is to be done on commercial offer stage and cannot be changed, unless extra services addition. For design and pre-design options we choose whichever rate results in less cost.</p>' +
      '</div>' +
      '<div class="gc-price-grid">' +
      priceCard('BIM', '$0.5/sf', 'or $0.7/sf detailed') +
      priceCard('CAD edit', '$0.3/sf', 'or $0.5/sf detailed') +
      priceCard('Visualisation', '$30/hr', 'average 60 hours') +
      priceCard('Supervision', '15%', 'of estimated project cost') +
      priceCard('Design project', '$2–$20/SF', 'or $50/hour | average 350 hours') +
      priceCard('Pre-design project', '$1/SF', 'or $40/hour | average 120 hours') +
      priceCard('Extra services', '$30/hr', '') +
      priceCard('Custom Model', 'from $50', '') +
      priceCard('As Built Conditions Surveying', 'from $200', 'with scan and model') +
      priceCard('BIM Coordination', '$1000', 'per project per month') +
      priceCard('Design Leading', '$1500', 'per project per month — includes BIM Coordination, Supervision, Construction Plans preparation, Design Book, Visualisations') +
      priceCard('Full Architectural Services', '$2000', 'per project per month — DOB Architectural Plans, Design Leading, Research and Analysis') +
      '</div>' +
      '<div class="gc-pricing-footer"><p>Design $/SF rate is calculated depending on project location, size to complexity ratio, amount of repetitions and expected modular components.</p></div>'
    );
  }

  function softwarePanel() {
    return (
      '<div class="gc-price-grid">' +
      priceCard('Business Website', 'from $500', '') +
      priceCard('Landing', 'from $50', '') +
      priceCard('UI/UX Development', '$30/hour', 'design optional but included') +
      priceCard('Web App', 'from $3000', '') +
      priceCard('Custom CAD Scripts', 'from $50', '') +
      priceCard('Custom Apps', 'from $5000', '') +
      '</div>'
    );
  }

  function brandingPanel() {
    return (
      '<div class="gc-price-grid">' +
      priceCard('Full Business Branding', 'from $1000', 'logos, color palettes, fonts, basic website, SEO optimization, design book (content to be provided, required)') +
      priceCard('Logo', 'from $100', 'in 3 options, 1 revision, 3 variations for each') +
      priceCard('UI/UX Design', '$30/hour', '') +
      priceCard('Product Design', '$50/hour', '') +
      '</div>'
    );
  }

  window.__getPricingPopupHTML = function () {
    return (
      '<h2 class="gc-pricing-title">Price List</h2>' +
      '<div class="gc-pricing-tabs">' +
      '<button type="button" class="gc-pricing-tab active" data-tab="architecture">Architecture</button>' +
      '<button type="button" class="gc-pricing-tab" data-tab="software">Software Development</button>' +
      '<button type="button" class="gc-pricing-tab" data-tab="branding">Branding</button>' +
      '</div>' +
      '<div class="gc-pricing-panels">' +
      '<div class="gc-pricing-panel active" data-panel="architecture">' + architecturePanel() + '</div>' +
      '<div class="gc-pricing-panel" data-panel="software">' + softwarePanel() + '</div>' +
      '<div class="gc-pricing-panel" data-panel="branding">' + brandingPanel() + '</div>' +
      '</div>'
    );
  };
})();
