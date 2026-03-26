// sound-manager.js — Web Audio API sound manager for Blueprints GUI
// xarta-node Blueprints GUI
//
// Singleton IIFE: SoundManager
//   SoundManager.init()              — call once on DOMContentLoaded
//   SoundManager.setEnabled(bool)    — enable/disable playback
//   SoundManager.play(url)           — play a sound by URL (no-op if disabled or missing)
//   SoundManager.preload(url)        — preload and cache an AudioBuffer
//
// Sound files are at /fallback-ui/assets/sounds/*.{wav,mp3,ogg,flac,webm}
// Global enable/disable is controlled by the 'fe.sound_enabled' setting in the
// settings table (fetched via getFrontendSetting, cached in localStorage).
//
// AudioContext must be resumed on a user gesture. The first click/keydown after
// page load will attempt to resume if the context was auto-suspended.

'use strict';

const SoundManager = (() => {
    let _ctx = null;
    let _gainNode = null;
    let _enabled = false;
    let _volume = 0.8;
    let _cache = {};    // { url: AudioBuffer }
    let _loading = {};  // { url: Promise<void> }  — dedup concurrent loads

    function _getCtx() {
        if (!_ctx) {
            try {
                _ctx = new (window.AudioContext || window.webkitAudioContext)();
            } catch (e) {
                console.warn('[SoundManager] AudioContext not available:', e);
            }
        }
        return _ctx;
    }

    // Resume AudioContext if suspended (browsers block until user gesture)
    function _resumeCtx() {
        const ctx = _getCtx();
        if (ctx && ctx.state === 'suspended') {
            ctx.resume().catch(() => {});
        }
    }

    // Returns the shared GainNode (created once per AudioContext)
    function _getGainNode() {
        const ctx = _getCtx();
        if (!ctx) return null;
        if (!_gainNode) {
            _gainNode = ctx.createGain();
            _gainNode.gain.value = _volume;
            _gainNode.connect(ctx.destination);
        }
        return _gainNode;
    }

    // Wire up a one-time resume-on-first-interaction handler
    function _setupResumeOnGesture() {
        const handler = () => {
            _resumeCtx();
            document.removeEventListener('click', handler, true);
            document.removeEventListener('keydown', handler, true);
        };
        document.addEventListener('click', handler, true);
        document.addEventListener('keydown', handler, true);
    }

    return {
        init() {
            // Read the setting from localStorage cache (set by loadFrontendSettings)
            if (typeof getFrontendSetting === 'function') {
                _enabled = getFrontendSetting('sound_enabled', 'false') === 'true';
            }
            // Read volume from localStorage (tab-local only — not synced to backend)
            const stored = parseFloat(localStorage.getItem('fe.sound_volume') ?? '0.8');
            _volume = isNaN(stored) ? 0.8 : Math.max(0, Math.min(1, stored));
            _setupResumeOnGesture();
        },

        setVolume(v) {
            _volume = Math.max(0, Math.min(1, v));
            if (_gainNode) _gainNode.gain.value = _volume;
        },

        getVolume() {
            return _volume;
        },

        setEnabled(v) {
            _enabled = Boolean(v);
        },

        async preload(url) {
            if (!url) return;
            if (_cache[url]) return;
            if (_loading[url]) return _loading[url];

            const ctx = _getCtx();
            if (!ctx) return;

            _loading[url] = (async () => {
                try {
                    const resp = await fetch(url);
                    if (!resp.ok) return;   // silent fail — asset may not exist yet
                    const buf = await resp.arrayBuffer();
                    _cache[url] = await ctx.decodeAudioData(buf);
                } catch (e) {
                    // Silently ignore missing or undecodable assets
                } finally {
                    delete _loading[url];
                }
            })();
            return _loading[url];
        },

        play(url) {
            if (!_enabled || !url) return;
            this._playNow(url);
        },

        // preview() bypasses the sound_enabled flag — for test/preview buttons in settings.
        preview(url) {
            if (!url) return;
            this._playNow(url);
        },

        _playNow(url) {
            const ctx = _getCtx();
            if (!ctx) return;
            _resumeCtx();

            const gainNode = _getGainNode();

            if (_cache[url]) {
                try {
                    const source = ctx.createBufferSource();
                    source.buffer = _cache[url];
                    source.connect(gainNode || ctx.destination);
                    source.start(0);
                } catch (e) {
                    // Silently ignore playback errors
                }
            } else {
                // Preload then play
                this.preload(url).then(() => {
                    if (_cache[url]) {
                        try {
                            const source = ctx.createBufferSource();
                            source.buffer = _cache[url];
                            source.connect(gainNode || ctx.destination);
                            source.start(0);
                        } catch (e) {}
                    }
                });
            }
        },
    };
})();
