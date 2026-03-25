/* ================================================================
   hub-modal.js — Modal Dialog Component
   xarta-node Blueprints GUI

   Portable, no external dependencies. Drop alongside modal.css.

   ── API ──────────────────────────────────────────────────────────

   HubModal.open(dialogEl [, opts])
     Show the dialog. opts:
       onOpen:  Function  — called immediately after showModal()
       onClose: Function  — called after dialog.close()

   HubModal.close(dialogEl)
     Close the dialog and fire onClose callback if set.

   HubModal.init([root = document])
     Auto-wire all .hub-modal dialogs found inside root:
       • .hub-modal-close buttons → close on click
       • Backdrop clicks (outside dialog rect) → close
     Call once after DOMContentLoaded, or again after dynamic inserts.

   ── HTML contract ─────────────────────────────────────────────────

   <dialog class="hub-modal" id="my-modal">
     <div class="hub-modal-header">
       <span class="hub-modal-icon" aria-hidden="true">⚙️</span>   <!-- optional -->
       <h2 class="hub-modal-title">Dialog Title</h2>
       <button class="hub-modal-close" type="button" data-fc-key="modal.close">CLOSE</button>
     </div>
     <div class="hub-modal-body">
       <!-- .field groups, paragraphs, etc. -->
     </div>
     <div class="hub-modal-footer">                                  <!-- optional -->
       <button class="hub-modal-btn secondary" type="button">Cancel</button>
       <button class="hub-modal-btn" type="button" data-fc-key="modal.confirm">Save</button>
     </div>
   </dialog>

   ── Sound hooks ───────────────────────────────────────────────────

   Add data-fc-key="some.key" to any button for form-control-manager
   integration. hub-modal.js itself does NOT play sounds — the host
   app's SoundManager picks up data-fc-key from DOM events.

   ── Backdrop click ────────────────────────────────────────────────

   A click on the dialog element itself (outside children) means the
   user clicked the ::backdrop. The handler computes whether the click
   coords fall outside the dialog's bounding rect — if so, close.

   ── Escape key ────────────────────────────────────────────────────

   The browser's native <dialog> already closes on Escape and fires a
   'cancel' event. We listen to 'cancel' to trigger the onClose callback.

   ================================================================ */

'use strict';

const HubModal = (() => {

    /* WeakMap keyed on dialog element → opts object.
       WeakMap: entries are GC'd when the dialog is removed from DOM. */
    const _opts = new WeakMap();

    /* ── open ─────────────────────────────────────────────────── */

    function open(dialog, opts) {
        if (!dialog || dialog.open) return;

        _opts.set(dialog, opts || {});
        dialog.showModal();

        if (typeof (opts || {}).onOpen === 'function') {
            opts.onOpen();
        }
    }

    /* ── close ────────────────────────────────────────────────── */

    function close(dialog) {
        if (!dialog || !dialog.open) return;
        dialog.close();
        const o = _opts.get(dialog) || {};
        if (typeof o.onClose === 'function') o.onClose();
    }

    /* ── Internal event handlers ──────────────────────────────── */

    /* Fired when a .hub-modal-close button is clicked */
    function _onCloseBtn(e) {
        const dialog = e.currentTarget.closest('dialog.hub-modal');
        if (dialog) close(dialog);
    }

    /* Fired on the <dialog> element; detects backdrop clicks */
    function _onDialogClick(e) {
        /* getBoundingClientRect returns the dialog's CONTENT box,
           not counting the ::backdrop. A click outside this rect
           originated on the backdrop. */
        const rect = e.currentTarget.getBoundingClientRect();
        const inRect = (
            e.clientX >= rect.left && e.clientX <= rect.right &&
            e.clientY >= rect.top  && e.clientY <= rect.bottom
        );
        if (!inRect) close(e.currentTarget);
    }

    /* Fired when the native Escape key closes the dialog (browser 'cancel' event).
       We use this to fire our onClose callback without double-closing. */
    function _onCancel(e) {
        /* Let the browser close the dialog natively; we just fire callback. */
        const o = _opts.get(e.currentTarget) || {};
        if (typeof o.onClose === 'function') o.onClose();
    }

    /* Guard against wiring the same element twice */
    const _wired = new WeakSet();

    /* ── init ─────────────────────────────────────────────────── */

    function init(root) {
        const scope = root || document;
        scope.querySelectorAll('dialog.hub-modal').forEach(dialog => {
            if (_wired.has(dialog)) return;
            _wired.add(dialog);

            /* Wire all CLOSE buttons within this dialog */
            dialog.querySelectorAll('.hub-modal-close').forEach(btn => {
                btn.addEventListener('click', _onCloseBtn);
            });

            /* Backdrop click */
            dialog.addEventListener('click', _onDialogClick);

            /* Native Escape key cancel event */
            dialog.addEventListener('cancel', _onCancel);
        });
    }

    return { open, close, init };

})();
