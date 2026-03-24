/* ── Body Shade — mobile pull-up content shade ─────────────────────────────
   Self-contained IIFE module. No external dependencies.
   Only active on touch devices — desktop ignores the handle via CSS.

   States:
     down (default)  → handle + shade in normal document flow, translateY=0
     dragging        → translateY follows finger; --shade-y drives CSS transform
     up (fixed)      → handle + shade position:fixed; shade has own scroll

   Scroll strategy:
     We call window.scrollTo(0, 0) before any snap-up animation so that
     the translateY math (which references the handle's viewport position)
     is consistent. In fixed-up state the shade provides its own overflow-y
     scroll, so users can continue reading without the page scrolling.

   Transition from fixed-up → dragging-down:
     We pre-set --shade-y = -maxTravel BEFORE removing .is-up, so that
     when position:fixed drops away, the translateY activates at the same
     visual position (no jump). This relies on scrollY = 0 at that moment.
──────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  var HANDLE_H   = 24;   // px — must match .body-shade-handle height in CSS
  var SNAP_VELO  = 250;  // px/s — velocity threshold for fast-flick snap
  var TRANSITION = 300;  // ms — must match CSS transition duration

  var handle, shade;
  var shadeY    = 0;     // current translateY value (0 = down, negative = up)
  var maxTravel = 0;     // px — handle's natural distance from viewport top (at scrollY=0)
  var isUp      = false;

  var dragging     = false;
  var startTouchY  = 0;
  var startShadeY  = 0;
  var lastTouchY   = 0;
  var lastTouchT   = 0;
  var vel          = 0;  // px/s, EMA (negative = moving up)

  /* ── Apply translateY to both elements via CSS custom property.
     instant=true  → suppress CSS transition (during drag)
     instant=false → allow CSS transition (snap animation)              */
  function applyTranslate(y, instant) {
    shadeY = y;
    handle.classList.toggle('is-dragging', instant);
    shade.classList.toggle('is-dragging',  instant);
    handle.style.setProperty('--shade-y', y + 'px');
    shade.style.setProperty('--shade-y',  y + 'px');
  }

  /* ── Switch to position:fixed (fully-up state).
     Called via setTimeout after the snap-up CSS transition completes.  */
  function enterFixed() {
    isUp = true;
    handle.classList.add('is-up');
    shade.classList.add('is-up');
    // --shade-y no longer needed; .is-up enforces transform:none via !important
    handle.style.removeProperty('--shade-y');
    shade.style.removeProperty('--shade-y');
    handle.classList.remove('is-dragging');
    shade.classList.remove('is-dragging');
    shadeY = 0;
  }

  /* ── Switch from position:fixed back to translateY without a visual jump.
     Pre-sets --shade-y while .is-up is still active (transform:none!important
     suppresses it visually). When .is-up is removed translateY activates
     at the same visual position. Only safe when window.scrollY === 0.     */
  function exitFixed() {
    handle.style.setProperty('--shade-y', (-maxTravel) + 'px');
    shade.style.setProperty('--shade-y',  (-maxTravel) + 'px');
    handle.classList.add('is-dragging'); // suppress transition during switch
    shade.classList.add('is-dragging');
    handle.classList.remove('is-up');
    shade.classList.remove('is-up');
    isUp   = false;
    shadeY = -maxTravel;
  }

  /* ── Touch handlers ───────────────────────────────────────────────────── */
  function onTouchStart(e) {
    if (e.touches.length !== 1) return;
    e.preventDefault();

    if (isUp) {
      // Coming from fixed-up: ensure page at top so translateY math aligns
      window.scrollTo(0, 0);
      exitFixed();
      startShadeY = -maxTravel;
    } else {
      // Ensure page at top for consistent maxTravel calculation
      if (window.scrollY !== 0) window.scrollTo(0, 0);
      // maxTravel = handle's natural distance from viewport top.
      // rect.top = naturalTop + shadeY (translate included).
      // naturalTop = rect.top - shadeY. The shadeY terms cancel correctly for
      // any current shadeY value (see: maxTravel = rect.top - shadeY = naturalTop).
      var rect = handle.getBoundingClientRect();
      maxTravel   = Math.max(0, rect.top - shadeY);
      startShadeY = shadeY;
    }

    dragging    = true;
    startTouchY = e.touches[0].clientY;
    lastTouchY  = startTouchY;
    lastTouchT  = Date.now();
    vel         = 0;
    handle.classList.add('is-grabbing');
  }

  function onTouchMove(e) {
    if (!dragging || e.touches.length !== 1) return;
    e.preventDefault();

    var ty  = e.touches[0].clientY;
    var now = Date.now();
    var dt  = now - lastTouchT;
    if (dt > 0) {
      var inst = (ty - lastTouchY) / (dt / 1000); // instantaneous px/s
      vel = vel * 0.6 + inst * 0.4;               // exponential moving average
    }
    lastTouchY = ty;
    lastTouchT = now;

    var newY = Math.min(0, Math.max(-maxTravel, startShadeY + (ty - startTouchY)));
    applyTranslate(newY, true /* instant — no transition during drag */);
  }

  function onTouchEnd() {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('is-grabbing');

    if (maxTravel <= 0) {
      applyTranslate(0, false);
      return;
    }

    // Snap decision: velocity takes priority; position (50% threshold) as fallback
    var goUp = Math.abs(vel) >= SNAP_VELO
      ? vel < 0         // fast upward flick
      : shadeY < -(maxTravel * 0.5); // past halfway

    if (goUp) {
      window.scrollTo(0, 0); // ensure scrollY=0 before fixed switch
      applyTranslate(-maxTravel, false); // animate to fully up
      setTimeout(enterFixed, TRANSITION); // switch to fixed after transition
    } else {
      applyTranslate(0, false); // animate to fully down
    }
  }

  /* ── Initialise ───────────────────────────────────────────────────────── */
  function init() {
    handle = document.getElementById('body-shade-handle');
    shade  = document.getElementById('body-shade');
    if (!handle || !shade) return;

    handle.addEventListener('touchstart',  onTouchStart, { passive: false });
    handle.addEventListener('touchmove',   onTouchMove,  { passive: false });
    handle.addEventListener('touchend',    onTouchEnd,   { passive: true });
    handle.addEventListener('touchcancel', onTouchEnd,   { passive: true });
  }

  document.readyState === 'loading'
    ? document.addEventListener('DOMContentLoaded', init)
    : init();

}());
