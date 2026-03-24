// form-control-manager.js — CMS-driven sound/icon assignments for form controls
// xarta-node Blueprints GUI
//
// Singleton IIFE: FormControlManager
//
//   FormControlManager.init()      — call once on DOMContentLoaded; sets up event delegation
//   FormControlManager.load()      — fetch /api/v1/form-controls, cache keys, preload sounds
//   FormControlManager.reload()    — clear cache and re-load (call after saving from settings page)
//   FormControlManager.getIconUrl(controlKey) → string|null
//
// HTML integration:
//   Add data-fc-key="my.key" to any interactive element (input, select, toggle,
//   button, etc.) to have its assigned sound play on interaction.
//
//   Optionally override the trigger event with data-fc-event="change|click|focus|input".
//
// Sound trigger rules (default if data-fc-event is absent):
//   <button>                          → "click"
//   <select>                          → "change"
//   <input type="checkbox|radio|range"> → "change"
//   <input type="text|search|…">      → "focus"
//   <textarea>                        → "focus"
//   anything else with data-fc-key    → "click"
//
// Event delegation uses the capture phase so it fires before any in-component
// stopPropagation calls.
//
// Sound playback delegates to SoundManager (sound-manager.js); if SoundManager
// is absent or sound_enabled is false, calls are silent no-ops.
//
// Icon support: FormControlManager.getIconUrl(key) returns a cache-busted URL
// for the assigned icon_asset, or null if none is assigned.  Rendering the
// icon in the DOM is intentionally left to the component — not all form controls
// will display an icon.

'use strict';

const FormControlManager = (() => {
    // { control_key: { control_id, icon_asset, sound_asset, updated_at } }
    let _controls = {};
    let _loaded = false;

    // ── Default trigger event per element ────────────────────────────────────

    function _eventFor(el) {
        const tag = el.tagName.toLowerCase();
        if (tag === 'button') return 'click';
        if (tag === 'select') return 'change';
        if (tag === 'input') {
            const type = (el.getAttribute('type') || 'text').toLowerCase();
            if (type === 'checkbox' || type === 'radio' || type === 'range') return 'change';
            return 'focus';   // text, search, email, number, etc.
        }
        if (tag === 'textarea') return 'focus';
        return 'click';
    }

    // ── Document-level event delegation ──────────────────────────────────────

    function _handleEvent(evt) {
        // Walk up from event target to find first [data-fc-key] ancestor
        let el = evt.target;
        while (el && el !== document) {
            if (el.dataset && el.dataset.fcKey) break;
            el = el.parentElement;
        }
        if (!el || !el.dataset || !el.dataset.fcKey) return;

        const key   = el.dataset.fcKey;
        const ctrl  = _controls[key];
        if (!ctrl) return;
        if (!ctrl.sound_asset && !ctrl.sound_asset_off) return;

        // Special case: hub-tab-caret buttons — only play sound when opening
        // (capture phase fires before the click handler toggles .open, so we
        // can inspect the current state and skip if the dropdown is already open)
        if (el.classList.contains('hub-tab-caret')) {
            const dropdown = el.closest('.hub-tab-dropdown');
            if (dropdown && dropdown.classList.contains('open')) return;
        }

        // Determine expected event for this element (allow per-element override)
        const expected = el.dataset.fcEvent || _eventFor(el);
        if (evt.type !== expected) return;

        // Choose sound: checkboxes/radios play sound_asset_off when turning off
        let soundPath = ctrl.sound_asset;
        if (evt.type === 'change' && ctrl.sound_asset_off) {
            const t = (el.tagName || '').toLowerCase();
            const type = (el.getAttribute('type') || '').toLowerCase();
            if (t === 'input' && (type === 'checkbox' || type === 'radio') && !el.checked) {
                soundPath = ctrl.sound_asset_off;
            }
        }
        if (!soundPath) return;

        const ts  = ctrl.updated_at ? new Date(ctrl.updated_at).getTime() : 0;
        const url = `/fallback-ui/assets/${soundPath}?v=${ts}`;
        if (typeof SoundManager !== 'undefined') {
            SoundManager.play(url);
        }
    }

    function _setupDelegation() {
        // Wire all three event types in capture phase to intercept before any
        // component can call stopPropagation.
        document.addEventListener('click',  _handleEvent, true);
        document.addEventListener('change', _handleEvent, true);
        document.addEventListener('focus',  _handleEvent, true);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    return {
        /** Set up event delegation.  Call once on DOMContentLoaded. */
        init() {
            _setupDelegation();
        },

        /** Fetch all form controls from the API, cache them, preload sounds. */
        async load() {
            if (_loaded) return;
            try {
                const resp = await apiFetch('/api/v1/form-controls');
                if (!resp.ok) return;
                const items = await resp.json();
                _controls = {};
                for (const item of items) {
                    _controls[item.control_key] = {
                        control_id:      item.control_id,
                        icon_asset:      item.icon_asset      || null,
                        sound_asset:     item.sound_asset     || null,
                        sound_asset_off: item.sound_asset_off || null,
                        updated_at:      item.updated_at      || null,
                    };
                }
                _loaded = true;
                // Preload all sound assets so the first interaction has no latency
                for (const ctrl of Object.values(_controls)) {
                    if (typeof SoundManager === 'undefined') break;
                    const ts = ctrl.updated_at ? new Date(ctrl.updated_at).getTime() : 0;
                    if (ctrl.sound_asset)
                        SoundManager.preload(`/fallback-ui/assets/${ctrl.sound_asset}?v=${ts}`);
                    if (ctrl.sound_asset_off)
                        SoundManager.preload(`/fallback-ui/assets/${ctrl.sound_asset_off}?v=${ts}`);
                }
            } catch (_e) {
                // Silent fail — sounds/icons degrade gracefully to nothing
            }
        },

        /** Clear cache and reload from API (call after editing controls in settings). */
        async reload() {
            _loaded   = false;
            _controls = {};
            return this.load();
        },

        /**
         * Return a cache-busted URL for the icon assigned to controlKey, or null.
         * Use this to optionally render an icon beside or inside a form control.
         */
        getIconUrl(controlKey) {
            const ctrl = _controls[controlKey];
            if (!ctrl || !ctrl.icon_asset) return null;
            const ts = ctrl.updated_at ? new Date(ctrl.updated_at).getTime() : 0;
            return `/fallback-ui/assets/${ctrl.icon_asset}?v=${ts}`;
        },

        /** Return the full cached control record for a key, or null. */
        getControl(controlKey) {
            return _controls[controlKey] || null;
        },
    };
})();
