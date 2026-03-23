/* ================================================================
   responsive-layout.js — JS-Driven Responsive Header + Page Controls
   xarta-node Blueprints GUI

   Responsibilities:
     1. Detect when header elements would overlap and toggle
        html.header-compact via ResizeObserver (no fixed breakpoint).
     2. Register per-tab page control groups and show/hide them
        as tabs switch.
     3. Hysteresis buffer prevents flickering at the boundary.
     4. 500ms debounce — at most one resize measurement per debounce
        window, even during rapid drag-resize.

   Usage:
     - Call ResponsiveLayout.init() once from app.js DOMContentLoaded.
     - Call ResponsiveLayout.destroy() on page unload (optional, no-op
       on SPA that never unloads, but good practice).
     - Register per-tab controls from each page's JS module:
         ResponsiveLayout.registerTabControls('bookmarks-main', 'pg-ctrl-bookmarks-main');
     - Call after every tab switch (wired in app.js switchTab):
         ResponsiveLayout.updateControlsForTab(tabId);

   Memory-leak safety:
     - Single ResizeObserver instance; disconnect() called on destroy().
     - clearTimeout on destroy() to cancel any pending debounce timer.
     - No window.onresize fallback — ResizeObserver only.
     - No closure captures of DOM elements removed later (elements are
       looked up by ID at call time, not captured at init time).
   ================================================================ */

const ResponsiveLayout = (() => {

    /* ── State ────────────────────────────────────────────────── */

    let _ro = null;                 // ResizeObserver instance
    let _timer = null;              // debounce timer handle
    let _pending = false;           // true while one measurement is queued
    let _storedMetaWidth = 0;       // width of .node-meta when compact mode was triggered
    let _storedBrandWidth = 0;      // width of .brand when compact mode was triggered
    const _tabMap = new Map();      // tabId → groupElementId

    /* ── Hysteresis buffer (px) ───────────────────────────────── */
    // Enter compact if gap < ENTER_BUFFER; exit compact if free space > EXIT_BUFFER
    const ENTER_BUFFER = 8;
    const EXIT_BUFFER  = 60;

    /* ── Debounce ─────────────────────────────────────────────── */

    function _scheduleCheck() {
        if (_pending) return;       // already one measurement queued — skip
        _pending = true;
        clearTimeout(_timer);
        _timer = setTimeout(() => {
            _pending = false;
            _checkHeaderOverlap();
        }, 500);
    }

    /* ── Overlap detection with hysteresis ────────────────────── */

    function _checkHeaderOverlap() {
        const header      = document.querySelector('header');
        const headerInner = document.querySelector('.header-inner');
        const headerLeft  = document.querySelector('.header-left');
        const headerRight = document.querySelector('.header-right');
        const nodeMeta    = document.querySelector('.node-meta');
        if (!header || !headerInner || !headerLeft || !headerRight) return;

        const isCompact = document.documentElement.classList.contains('header-compact');

        if (!isCompact) {
            // Measure the gap between header-left right edge and header-right left edge
            const leftRect  = headerLeft.getBoundingClientRect();
            const rightRect = headerRight.getBoundingClientRect();

            // If right has wrapped below left, or they've nearly collided → go compact
            const hasWrapped  = rightRect.top > leftRect.top + 4;
            const gapTooSmall = (rightRect.left - leftRect.right) < ENTER_BUFFER;

            if (hasWrapped || gapTooSmall) {
                // Snapshot widths BEFORE hiding header-left (they'd be 0 after display:none)
                const brand = document.querySelector('.header-left .brand');
                _storedBrandWidth = brand ? brand.offsetWidth : 0;
                _storedMetaWidth  = nodeMeta ? nodeMeta.offsetWidth : 0;
                document.documentElement.classList.add('header-compact');

                // Immediately re-evaluate after CSS applies (RAF fires before next paint).
                // If the trigger was transient (e.g. brief scrollbar reflow), this exits
                // compact before the user sees it.  If compact is genuinely needed, the
                // exit check below will correctly keep it.
                requestAnimationFrame(_checkHeaderOverlap);
            }
        } else {
            // Already compact — measure if there's enough room to restore full header.
            // Use stored widths captured at compact-entry time: in compact mode,
            // .header-left is display:none so live offsetWidth would be 0.
            // Measure headerInner (constrained flex row) not the full-bleed header.
            const innerStyle    = window.getComputedStyle(headerInner);
            const hPadding      = parseFloat(innerStyle.paddingLeft) + parseFloat(innerStyle.paddingRight);
            const selector      = document.querySelector('blueprints-node-selector');
            const selectorWidth = selector ? selector.offsetWidth : 0;

            // Available = inner content width minus selector width minus gap
            const available = headerInner.clientWidth - hPadding - selectorWidth - 16; // 16px gap
            const needed    = _storedBrandWidth + _storedMetaWidth + EXIT_BUFFER;

            if (available >= needed) {
                document.documentElement.classList.remove('header-compact');
            }
        }
    }

    /* ── Tab control registration ─────────────────────────────── */

    /**
     * Register a page control group for a specific tab.
     * @param {string} tabId      - The tab ID (e.g. 'bookmarks-main')
     * @param {string} groupElId  - The element ID of the .page-control-group div
     */
    function registerTabControls(tabId, groupElId) {
        _tabMap.set(tabId, groupElId);
    }

    /**
     * Show the page control group for the given tab; hide all others.
     * Called from app.js switchTab() after each tab switch.
     * @param {string} tabId - The newly active tab ID
     */
    function updateControlsForTab(tabId) {
        _tabMap.forEach((groupElId, tid) => {
            const el = document.getElementById(groupElId);
            if (!el) return;
            if (tid === tabId) {
                el.hidden = false;
            } else {
                el.hidden = true;
            }
        });
    }

    /* ── Lifecycle ────────────────────────────────────────────── */

    /**
     * Initialise the responsive layout system. Call once from DOMContentLoaded.
     */
    function init() {
        const header      = document.querySelector('header');
        const headerInner = document.querySelector('.header-inner');
        if (!header || !headerInner) return;

        _ro = new ResizeObserver(_scheduleCheck);
        _ro.observe(headerInner);  // observe inner (constrained) row, not the full-bleed shell

        // Also catch viewport-width changes (scrollbar appearing/disappearing,
        // browser window drag) which the header RO may miss if header height
        // doesn't change.
        window.addEventListener('resize', _scheduleCheck);

        // Run the first check after layout has settled
        requestAnimationFrame(() => _checkHeaderOverlap());
    }

    /**
     * Tear down the responsive layout system. Call on page unload to prevent
     * memory leaks (ResizeObserver + pending debounce timer).
     */
    function destroy() {
        if (_ro) { _ro.disconnect(); _ro = null; }
        window.removeEventListener('resize', _scheduleCheck);
        clearTimeout(_timer);
        _timer = null;
        _pending = false;
    }

    /* ── Public API ───────────────────────────────────────────── */

    return { init, destroy, registerTabControls, updateControlsForTab };

})();
