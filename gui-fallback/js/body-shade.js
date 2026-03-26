/* ── Body Shade — pull-up content shade (all screen sizes) ──────────────────
   Self-contained IIFE module. No external dependencies.
   Works on every screen size — touch (mobile) + mouse (desktop).

   Handles live INSIDE tab panels at the data boundary.
   The handle is a child of .body-shade and moves with the shade automatically.
   Only .body-shade receives the --shade-y / translateY transform.

   States:
     down (default)  → shade in normal flow, translateY=0
     dragging        → translateY tracks pointer; transition suppressed
     up              → shade held at translateY(-maxTravel), is-up class applied

   No position:fixed switching. The shade stays in normal flow at all times.
   maxTravel = distance from handle to the bottom of .menu-zone
   (or the top of <main> if .menu-zone is absent). This ensures the handle
   never travels above the persistent nav — the menu zone stays accessible.

   Tab switching:
     window.switchTab is patched at init time (body-shade.js loads before
     app.js, so patch is in place before app.js DOMContentLoaded fires).
──────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var SNAP_VELO  = 250;  // px/s — velocity threshold for fast-flick snap
  var TRANSITION = 300;  // ms — must match CSS transition duration

  var shade;
  var handle    = null;  // active handle (inside currently visible tab panel)
  var shadeY    = 0;     // current translateY (0 = down, negative = up)
  var maxTravel = 0;     // max distance shade can travel upward
  var isUp      = false;

  var dragging      = false;
  var startPointerY = 0;
  var startShadeY   = 0;
  var lastPointerY  = 0;
  var lastPointerT  = 0;
  var vel           = 0;   // px/s, EMA (negative = moving up)

  /* ── Compute maxTravel for the current handle ───────────────────────────── */
  /* maxTravel = pixels the shade must slide up so the handle reaches the
     very top of the viewport (y=0), hiding header, menu zone, and description.
     Must be measured at scrollY=0 so getBoundingClientRect gives true
     page coordinates (handleTop == distance from viewport top in natural state). */
  function computeMaxTravel() {
    if (!handle) return 0;
    if (window.scrollY !== 0) window.scrollTo(0, 0);
    return Math.max(0, handle.getBoundingClientRect().top);
  }

  /* ── Apply translateY to shade only (handle rides along as a child) ────── */
  function applyTranslate(y, instant) {
    shadeY = y;
    shade.classList.toggle('is-dragging', instant);
    shade.style.setProperty('--shade-y', y + 'px');
  }

  /* ── Mark shade as held-up (translateY already at -maxTravel) ───────────── */
  function enterUp() {
    isUp = true;
    shade.classList.add('is-up');
    if (handle) handle.classList.add('is-up');
    shade.classList.remove('is-dragging');
    if (handle) handle.classList.remove('is-dragging');
    document.body.classList.add('shade-is-up');
    // Shade has settled at top — resize the fill table to the new position.
    sizeFillTable();
  }

  /* ── Release held-up state (shade stays at same translateY visually) ────── */
  function exitUp() {
    shade.classList.remove('is-up');
    if (handle) handle.classList.remove('is-up');
    isUp = false;
    document.body.classList.remove('shade-is-up');
    // shadeY and --shade-y are already set to -maxTravel; leave them as-is
    // so when drag resumes the position is continuous.
  }

  /* ── Shared drag start (touch and mouse) ───────────────────────────────── */
  function startDrag(clientY) {
    if (!handle) return false;
    if (isUp) {
      exitUp();
      // maxTravel is still valid from when we entered up — keep it
      startShadeY = shadeY;   // shadeY == -maxTravel
    } else {
      // Hide header and menu zone immediately on drag-start so fixed/stacked
      // elements don't paint over the shade during the drag animation.
      document.body.classList.add('shade-is-up');
      var prevScrollY = window.scrollY;
      maxTravel   = computeMaxTravel();
      startShadeY = shadeY;
      // If computeMaxTravel scrolled the page to top, the handle jumped in the
      // viewport.  Re-anchor so the first moveDrag delta starts from zero.
      if (prevScrollY !== 0) {
        clientY = handle.getBoundingClientRect().top + handle.offsetHeight / 2;
      }
    }
    dragging      = true;
    startPointerY = clientY;
    lastPointerY  = clientY;
    lastPointerT  = Date.now();
    vel           = 0;
    handle.classList.add('is-grabbing');
    return true;
  }

  /* ── Shared drag move ───────────────────────────────────────────────────── */
  function moveDrag(clientY) {
    var now = Date.now();
    var dt  = now - lastPointerT;
    if (dt > 0) {
      var inst = (clientY - lastPointerY) / (dt / 1000);
      vel = vel * 0.6 + inst * 0.4;
    }
    lastPointerY = clientY;
    lastPointerT = now;
    var newY = Math.min(0, Math.max(-maxTravel, startShadeY + (clientY - startPointerY)));
    applyTranslate(newY, true);
  }

  /* ── Shared drag end ────────────────────────────────────────────────────── */
  function endDrag() {
    if (!dragging) return;
    dragging = false;
    if (handle) handle.classList.remove('is-grabbing');

    if (maxTravel <= 0) {
      applyTranslate(0, false);
      return;
    }

    var goUp = Math.abs(vel) >= SNAP_VELO
      ? vel < 0
      : shadeY < -(maxTravel * 0.5);

    if (goUp) {
      applyTranslate(-maxTravel, false);
      // After transition completes, lock the up state
      setTimeout(enterUp, TRANSITION);
    } else {
      applyTranslate(0, false);
      // Snap went down — restore header and menu zone.
      document.body.classList.remove('shade-is-up');
      // After the CSS transition settles, resize fill table to restored position.
      setTimeout(sizeFillTable, TRANSITION + 50);
    }
  }

  /* ── Touch handlers ─────────────────────────────────────────────────────── */
  function onTouchStart(e) {
    if (e.touches.length !== 1) return;
    e.preventDefault();
    startDrag(e.touches[0].clientY);
  }

  function onTouchMove(e) {
    if (!dragging || e.touches.length !== 1) return;
    e.preventDefault();
    moveDrag(e.touches[0].clientY);
  }

  function onTouchEnd() { endDrag(); }

  /* ── Mouse handlers (desktop drag) ─────────────────────────────────────── */
  function onMouseDown(e) {
    if (e.button !== 0) return;
    e.preventDefault();
    if (startDrag(e.clientY)) {
      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup',   onMouseUp);
    }
  }

  function onMouseMove(e) {
    if (!dragging) return;
    moveDrag(e.clientY);
  }

  function onMouseUp() {
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup',   onMouseUp);
    endDrag();
  }

  /* ── Bind all drag events to a handle element ───────────────────────────── */
  function bindHandle(el) {
    if (!el) return;
    el.addEventListener('touchstart',  onTouchStart, { passive: false });
    el.addEventListener('touchmove',   onTouchMove,  { passive: false });
    el.addEventListener('touchend',    onTouchEnd,   { passive: true });
    el.addEventListener('touchcancel', onTouchEnd,   { passive: true });
    el.addEventListener('mousedown',   onMouseDown);
  }

  /* ── Update active handle when tab switches ─────────────────────────────── */
  function setActiveHandle(tabId) {
    var panel     = document.getElementById('tab-' + tabId);
    var newHandle = panel ? panel.querySelector('.body-shade-handle') : null;
    if (newHandle === handle) return;

    // Snap shade back to down before switching tab context
    if (isUp) {
      exitUp();
    }
    shade.classList.remove('is-dragging');
    applyTranslate(0, false);
    if (handle) handle.classList.remove('is-up', 'is-grabbing', 'is-dragging');

    handle    = newHandle;
    maxTravel = 0;  // will be recomputed on next drag-start
  }

  /* ── Size the fill table in the active panel ───────────────────────────────
     Old-school approach: measure exactly where .table-wrap--fill starts on
     screen, set its height to fill the remaining viewport minus the pager.
     No CSS variable arithmetic — just measure and set. ── */
  function sizeFillTable() {
    var panel = shade ? shade.querySelector('.tab-panel--fill.active') : null;
    // Toggle body class so page scroll is locked exactly when a fill tab is on.
    document.body.classList.toggle('has-fill-tab', !!panel);
    if (!panel) return;
    var fill = panel.querySelector('.table-wrap--fill');
    if (!fill) return;
    var pager = panel.querySelector('.table-pager');
    var pagerH = pager ? 44 : 0;
    // getBoundingClientRect gives viewport-relative position.
    // has-fill-tab sets overflow:hidden so scroll is locked at 0 — accurate.
    var top = fill.getBoundingClientRect().top;
    fill.style.height = Math.max(50, window.innerHeight - top - pagerH) + 'px';
  }

  var _fillTimer = null;
  function scheduleSizeFillTable() {
    clearTimeout(_fillTimer);
    _fillTimer = setTimeout(sizeFillTable, 50);
  }

  /* ─────────────────────────────────────────────────────────────────────── */
  function init() {
    shade = document.getElementById('body-shade');
    if (!shade) return;

    // Track header height and expose as --header-h so .menu-zone can
    // sticky-pin immediately below the header on all screen sizes.
    var siteHeader = document.querySelector('header');
    if (siteHeader && window.ResizeObserver) {
      var updateHeaderH = function () {
        document.documentElement.style.setProperty('--header-h', siteHeader.offsetHeight + 'px');
      };
      updateHeaderH();
      var roHeader = new ResizeObserver(updateHeaderH);
      roHeader.observe(siteHeader);
    }

    // Resize fill table on window resize (e.g. orientation change).
    window.addEventListener('resize', scheduleSizeFillTable);

    // Bind drag events to every handle inside the shade
    shade.querySelectorAll('.body-shade-handle').forEach(bindHandle);

    // Set the initially active tab's handle
    var activePanel = shade.querySelector('.tab-panel.active');
    handle = activePanel ? activePanel.querySelector('.body-shade-handle') : null;

    // Initial fill-table sizing (deferred so the page has fully laid out).
    scheduleSizeFillTable();

    // Patch window.switchTab to track handle changes on tab navigation.
    // We query the DOM after the switch rather than using the tab ID, so that
    // alias IDs (e.g. 'manual-links-table' → tab-manual-links) work correctly.
    if (typeof window.switchTab === 'function') {
      var orig = window.switchTab;
      window.switchTab = function (tab) {
        orig.apply(this, arguments);
        // Find whichever panel is now active and adopt its handle
        var activePanel = shade.querySelector('.tab-panel.active');
        var newHandle   = activePanel ? activePanel.querySelector('.body-shade-handle') : null;
        if (newHandle !== handle) {
          if (isUp) exitUp();
          shade.classList.remove('is-dragging');
          applyTranslate(0, false);
          if (handle) handle.classList.remove('is-up', 'is-grabbing', 'is-dragging');
          handle    = newHandle;
          maxTravel = 0;
        }
        // Resize fill table for the newly active panel.
        scheduleSizeFillTable();
      };
    }
  }

  document.readyState === 'loading'
    ? document.addEventListener('DOMContentLoaded', init)
    : init();

}());
