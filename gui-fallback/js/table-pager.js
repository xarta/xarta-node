(function () {
  'use strict';

  function readBool(key, fallback) {
    var raw = localStorage.getItem(key);
    if (raw === null) return fallback;
    return raw === '1' || raw === 'true';
  }

  function readInt(key, fallback, allowed) {
    var raw = localStorage.getItem(key);
    var num = parseInt(raw == null ? String(fallback) : raw, 10);
    return allowed.includes(num) ? num : fallback;
  }

  function notifyFillResize() {
    if (window.BodyShade && typeof window.BodyShade.scheduleSizeFillTable === 'function') {
      window.BodyShade.scheduleSizeFillTable();
    }
  }

  function createTablePager(cfg) {
    var pageSizeOptions = Array.isArray(cfg.pageSizeOptions) && cfg.pageSizeOptions.length
      ? cfg.pageSizeOptions.slice()
      : [25, 50, 100];
    var defaultPageSize = pageSizeOptions.includes(cfg.defaultPageSize)
      ? cfg.defaultPageSize
      : pageSizeOptions[0];
    var state = {
      page: 1,
      pageSize: readInt(cfg.pageSizeStorageKey, defaultPageSize, pageSizeOptions),
      enabled: readBool(cfg.enabledStorageKey, cfg.defaultEnabled !== false),
    };

    function getEl() {
      return document.getElementById(cfg.pagerId);
    }

    function emitChange() {
      if (typeof cfg.onChange === 'function') cfg.onChange();
    }

    function hide() {
      var el = getEl();
      if (!el) return;
      el.innerHTML = '';
      el.hidden = true;
      notifyFillResize();
    }

    function getSlice(items) {
      var rows = Array.isArray(items) ? items : [];
      var totalItems = rows.length;
      if (!state.enabled) {
        state.page = 1;
        return {
          enabled: false,
          paged: false,
          totalItems: totalItems,
          totalPages: 1,
          page: 1,
          pageSize: state.pageSize,
          items: rows,
          from: totalItems ? 1 : 0,
          to: totalItems,
        };
      }

      var totalPages = Math.max(1, Math.ceil(totalItems / state.pageSize));
      state.page = Math.max(1, Math.min(state.page, totalPages));

      var fromIndex = (state.page - 1) * state.pageSize;
      var toIndex = fromIndex + state.pageSize;
      var pageItems = totalItems > state.pageSize ? rows.slice(fromIndex, toIndex) : rows;

      return {
        enabled: true,
        paged: totalItems > state.pageSize,
        totalItems: totalItems,
        totalPages: totalPages,
        page: state.page,
        pageSize: state.pageSize,
        items: pageItems,
        from: totalItems ? fromIndex + 1 : 0,
        to: Math.min(state.page * state.pageSize, totalItems),
      };
    }

    function render(totalItems) {
      var el = getEl();
      if (!el) return;
      if (!state.enabled) {
        hide();
        return;
      }

      var totalPages = Math.max(1, Math.ceil((totalItems || 0) / state.pageSize));
      state.page = Math.max(1, Math.min(state.page, totalPages));
      el.hidden = false;
      el.innerHTML = '';

      if (totalPages > 1) {
        var prevBtn = document.createElement('button');
        prevBtn.className = 'secondary';
        prevBtn.style.cssText = 'padding:2px 10px;font-size:12px';
        prevBtn.innerHTML = '&#8592; Prev';
        prevBtn.disabled = state.page <= 1;
        prevBtn.addEventListener('click', function () {
          state.page -= 1;
          emitChange();
        });

        var info = document.createElement('span');
        info.innerHTML = 'Page <strong style="color:var(--text)">' + state.page + '</strong> of <strong style="color:var(--text)">' + totalPages + '</strong>';

        var nextBtn = document.createElement('button');
        nextBtn.className = 'secondary';
        nextBtn.style.cssText = 'padding:2px 10px;font-size:12px';
        nextBtn.innerHTML = 'Next &#8594;';
        nextBtn.disabled = state.page >= totalPages;
        nextBtn.addEventListener('click', function () {
          state.page += 1;
          emitChange();
        });

        el.appendChild(prevBtn);
        el.appendChild(info);
        el.appendChild(nextBtn);
      }

      var sizeLabel = document.createElement('label');
      sizeLabel.style.cssText = 'display:flex;align-items:center;gap:6px;cursor:pointer';
      sizeLabel.appendChild(document.createTextNode('Per page:'));

      var select = document.createElement('select');
      select.style.cssText = 'font-size:12px;padding:2px 6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text)';
      pageSizeOptions.forEach(function (optionValue) {
        var opt = document.createElement('option');
        opt.value = String(optionValue);
        opt.textContent = String(optionValue);
        opt.selected = optionValue === state.pageSize;
        select.appendChild(opt);
      });
      select.addEventListener('change', function () {
        var nextSize = parseInt(select.value, 10);
        if (!pageSizeOptions.includes(nextSize)) return;
        state.pageSize = nextSize;
        localStorage.setItem(cfg.pageSizeStorageKey, String(nextSize));
        state.page = 1;
        emitChange();
      });

      sizeLabel.appendChild(select);
      el.appendChild(sizeLabel);
      notifyFillResize();
    }

    return {
      getSlice: getSlice,
      render: render,
      hide: hide,
      isEnabled: function () { return state.enabled; },
      resetPage: function () {
        state.page = 1;
      },
      setEnabled: function (enabled) {
        var next = !!enabled;
        if (state.enabled === next) return;
        state.enabled = next;
        localStorage.setItem(cfg.enabledStorageKey, next ? '1' : '0');
        state.page = 1;
        emitChange();
      },
      toggleEnabled: function () {
        this.setEnabled(!state.enabled);
      },
      getState: function () {
        return { page: state.page, pageSize: state.pageSize, enabled: state.enabled };
      },
    };
  }

  window.TablePager = {
    create: createTablePager,
  };
}());
