/* ================================================================
   hub-select.js — Custom Select Widget
   xarta-node Blueprints GUI

   Replaces a native <select> with a custom dropdown that uses
   position:fixed for its popup panel — bypassing all overflow and
   z-index stacking contexts so the dropdown always opens downward
   and is never clipped regardless of ancestor CSS.

   The native <select> is kept hidden in the DOM so all existing
   JS code that reads .value or listens to 'change' events continues
   to work without modification.

   A MutationObserver on the native select's childList detects when
   external code rebuilds the <option> elements (e.g. _loadBookmarkTags
   in bookmarks.js) and re-syncs the custom dropdown automatically.

   Usage (from a page module's DOMContentLoaded handler):
     HubSelect.init('my-select-id');

   Cleanup (if the element is ever removed from the DOM):
     HubSelect.destroy('my-select-id');

   Recipe for new pages:
     1. Keep or add a native <select id="foo"> in the HTML.
     2. Call HubSelect.init('foo') in the page JS.
     3. Keep all existing .value reads and change listeners as-is.
   ================================================================ */

const HubSelect = (() => {

    const _instances = new Map(); // selectId → cleanup fn

    /* ── Position the popup below the trigger button ──────────── */
    //
    // Horizontal rule: anchor to the left of the button by default.
    // If it would overflow the right edge of the viewport, flip and
    // anchor to the right edge of the button instead.
    //
    // To measure the popup's natural width without a paint flash, we
    // briefly make it visible off-screen (visibility:hidden prevents
    // the user seeing it) then clear the inline overrides so the CSS
    // class controls display/visibility again.
    function _position(btnEl, menuEl) {
        const rect = btnEl.getBoundingClientRect();
        const vw   = window.innerWidth;

        // Clamp max-width so the popup never exceeds the viewport
        const maxW = Math.min(320, vw - 16);
        menuEl.style.top      = (rect.bottom + 4) + 'px';
        menuEl.style.minWidth = rect.width + 'px';
        menuEl.style.maxWidth = maxW + 'px';

        // Measure natural rendered width while invisible and off-screen
        menuEl.style.left       = '0';
        menuEl.style.right      = 'auto';
        menuEl.style.top        = '-9999px';
        menuEl.style.visibility = 'hidden';
        menuEl.style.display    = 'flex';
        menuEl.style.flexDirection = 'column';
        const menuW = menuEl.offsetWidth;       // forces synchronous layout
        // Clear temporary inline overrides — CSS class controls these
        menuEl.style.display       = '';
        menuEl.style.flexDirection = '';
        menuEl.style.visibility    = '';
        menuEl.style.top           = (rect.bottom + 4) + 'px';

        // Decide horizontal anchor
        if (rect.left + menuW > vw - 8) {
            // Would clip right edge — right-align to button's right edge
            menuEl.style.left  = 'auto';
            menuEl.style.right = (vw - rect.right) + 'px';
        } else {
            menuEl.style.left  = rect.left + 'px';
            menuEl.style.right = 'auto';
        }
    }

    /* ── Rebuild custom menu items from native select options ──── */
    function _syncOptions(selectEl, menuEl, labelEl) {
        menuEl.innerHTML = '';
        const currentVal = selectEl.value;

        Array.from(selectEl.options).forEach(opt => {
            const item = document.createElement('button');
            item.type = 'button';
            item.className = 'hub-select-item';
            item.dataset.value = opt.value;
            item.textContent = opt.text;
            if (opt.value === currentVal) {
                item.classList.add('hub-select-item-active');
            }
            item.addEventListener('click', () => {
                selectEl.value = opt.value;

                // Update label and active highlight
                labelEl.textContent = opt.text;
                menuEl.querySelectorAll('.hub-select-item')
                    .forEach(el => el.classList.remove('hub-select-item-active'));
                item.classList.add('hub-select-item-active');

                // Notify existing listeners on the native select
                selectEl.dispatchEvent(new Event('change', { bubbles: true }));

                // Close popup
                menuEl.classList.remove('hub-select-open');
                menuEl.previousElementSibling?.setAttribute('aria-expanded', 'false');
            });
            menuEl.appendChild(item);
        });

        // Sync label to current selected text
        const selOpt = selectEl.options[selectEl.selectedIndex];
        if (selOpt) labelEl.textContent = selOpt.text;
    }

    /* ── Public: initialise a custom select ───────────────────── */
    function init(selectId) {
        const selectEl = document.getElementById(selectId);
        if (!selectEl || selectEl.tagName !== 'SELECT') return;
        if (_instances.has(selectId)) return; // already initialised

        /* ── Build wrapper div ── */
        const wrapper = document.createElement('div');
        wrapper.className = 'hub-select';

        /* ── Build trigger button ── */
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'hub-ctrl hub-select-btn';
        btn.setAttribute('aria-haspopup', 'listbox');
        btn.setAttribute('aria-expanded', 'false');

        const labelEl = document.createElement('span');
        labelEl.className = 'hub-select-label';

        const caretEl = document.createElement('span');
        caretEl.className = 'hub-select-caret';
        caretEl.setAttribute('aria-hidden', 'true');

        btn.appendChild(labelEl);
        btn.appendChild(caretEl);

        /* ── Insert wrapper, move select inside it ── */
        selectEl.parentNode.insertBefore(wrapper, selectEl);
        wrapper.appendChild(btn);

        // Hide native select: keep in DOM for JS compatibility, not visible
        selectEl.style.cssText =
            'position:absolute;opacity:0;pointer-events:none;width:0;height:0;';
        wrapper.appendChild(selectEl);

        /* ── Build fixed popup (appended to body to escape overflow) ── */
        const menuEl = document.createElement('div');
        menuEl.className = 'hub-select-menu';
        menuEl.setAttribute('role', 'listbox');
        document.body.appendChild(menuEl);

        /* ── Initial option sync ── */
        _syncOptions(selectEl, menuEl, labelEl);

        /* ── Open / close on button click ── */
        btn.addEventListener('click', () => {
            const isOpen = menuEl.classList.contains('hub-select-open');

            // Close any other open hub-select menus
            document.querySelectorAll('.hub-select-menu.hub-select-open')
                .forEach(m => m.classList.remove('hub-select-open'));
            document.querySelectorAll('.hub-select-btn[aria-expanded="true"]')
                .forEach(b => b.setAttribute('aria-expanded', 'false'));

            if (!isOpen) {
                _position(btn, menuEl);
                menuEl.classList.add('hub-select-open');
                btn.setAttribute('aria-expanded', 'true');
            }
        });

        /* ── Close on outside click ── */
        function onOutsideClick(e) {
            if (!wrapper.contains(e.target) && !menuEl.contains(e.target)) {
                menuEl.classList.remove('hub-select-open');
                btn.setAttribute('aria-expanded', 'false');
            }
        }

        /* ── Close on Escape ── */
        function onEscape(e) {
            if (e.key === 'Escape') {
                menuEl.classList.remove('hub-select-open');
                btn.setAttribute('aria-expanded', 'false');
            }
        }

        document.addEventListener('click', onOutsideClick);
        document.addEventListener('keydown', onEscape);

        /* ── Reposition while open (scroll / resize) ── */
        function onReposition() {
            if (menuEl.classList.contains('hub-select-open')) _position(btn, menuEl);
        }
        window.addEventListener('scroll', onReposition, { passive: true });
        window.addEventListener('resize', onReposition, { passive: true });

        /* ── MutationObserver: re-sync when native options are rebuilt ── */
        // _loadBookmarkTags (and similar) rebuild innerHTML on the native select.
        // MutationObserver fires as a microtask after the synchronous JS
        // completes, so selectEl.value is already restored to the previous
        // value when _syncOptions reads it.
        const mo = new MutationObserver(() => _syncOptions(selectEl, menuEl, labelEl));
        mo.observe(selectEl, { childList: true });

        /* ── Register cleanup fn ── */
        _instances.set(selectId, function cleanup() {
            mo.disconnect();
            document.removeEventListener('click', onOutsideClick);
            document.removeEventListener('keydown', onEscape);
            window.removeEventListener('scroll', onReposition);
            window.removeEventListener('resize', onReposition);
            menuEl.remove();
        });
    }

    /* ── Public: tear down a custom select ────────────────────── */
    function destroy(selectId) {
        const cleanup = _instances.get(selectId);
        if (cleanup) { cleanup(); _instances.delete(selectId); }
    }

    return { init, destroy };

})();
