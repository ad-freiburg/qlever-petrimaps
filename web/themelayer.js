L.Control.ThemeLayerSwitcher = L.Control.extend({
  options: {
    position: 'topright',
    collapsed: false,
    defaultTheme: null
  },

  initialize: function (themes, options) {
    this.themes = themes;
    L.setOptions(this, options);

    this._activeTheme = null;
    this._activeOverlays = new Set();
  },

  onAdd: function (map) {
    this._map = map;

    const container = this._container = L.DomUtil.create(
      'div',
      'leaflet-control-layers'
    );

    L.DomEvent.disableClickPropagation(container);
    L.DomEvent.disableScrollPropagation(container);

    // Toggle button (hamburger)
    this._toggleButton = L.DomUtil.create(
      'a',
      'leaflet-control-layers-toggle',
      container
    );
    this._toggleButton.href = '#';

    // Form
    this._form = L.DomUtil.create(
      'form',
      'leaflet-control-layers-list',
      container
    );

    this._baseList = L.DomUtil.create(
      'div',
      'leaflet-control-layers-base',
      this._form
    );

    L.DomUtil.create(
      'div',
      'leaflet-control-layers-separator',
      this._form
    );

    this._overlayList = L.DomUtil.create(
      'div',
      'leaflet-control-layers-overlays',
      this._form
    );

    this._createThemeRadios();

    const initial =
      this.options.defaultTheme || Object.keys(this.themes)[0];
    this.applyTheme(initial);

    if (!this.options.collapsed) {
      this._expand();
    } else {
      this._initToggleBehavior();
      this._initHoverBehavior();
    }

    return container;
  },

  /* ---------------- TOGGLE BEHAVIOR ---------------- */

  _initToggleBehavior: function () {
    L.DomEvent
      .on(this._toggleButton, 'click', L.DomEvent.stop)
      .on(this._toggleButton, 'click', this._toggle, this);

	this._map.on('click', this._collapse, this);
  },

  _toggle: function () {
    if (L.DomUtil.hasClass(this._container, 'leaflet-control-layers-expanded')) {
      this._collapse();
    } else {
      this._expand();
    }
  },

  _expand: function () {
    L.DomUtil.addClass(this._container, 'leaflet-control-layers-expanded');
  },

  _collapse: function () {
    L.DomUtil.removeClass(this._container, 'leaflet-control-layers-expanded');
  },

  /* ---------------- THEMES ---------------- */

  _createThemeRadios: function () {
    Object.keys(this.themes).forEach(key => {
      const label = L.DomUtil.create('label', '', this._baseList);
      const input = L.DomUtil.create('input', '', label);

      input.type = 'radio';
      input.name = 'leaflet-theme';
      input.value = key;

      L.DomEvent.on(input, 'click', () => {
        this.applyTheme(key);
      });

      label.append(` ${this.themes[key].name}`);
    });
  },

  applyTheme: function (themeKey) {
    if (this._activeTheme === themeKey) return;

    const theme = this.themes[themeKey];
    if (!theme) return;

    this._activeOverlays.forEach(layer => {
      this._map.removeLayer(layer);
    });
    this._activeOverlays.clear();


    this._overlayList.innerHTML = '';
    (theme.overlays || []).forEach(group => {
      this._buildOverlayGroup(group, themeKey);
    });

    this._activeTheme = themeKey;
    this._syncThemeUI();
  },

  _syncThemeUI: function () {
    const radios = this._baseList.querySelectorAll('input');
    radios.forEach(radio => {
      radio.checked = radio.value === this._activeTheme;
    });
  },

  /* ---------------- OVERLAYS ---------------- */

  _buildOverlayGroup: function (group, themeKey) {
    const header = L.DomUtil.create(
      'div',
      'leaflet-control-layers-group',
      this._overlayList
    );
    header.innerHTML = `<strong>${group.name}</strong>`;

	let have = false;

    group.layers.forEach(entry => {
      const label = L.DomUtil.create('label', '', this._overlayList);
      const input = L.DomUtil.create('input', '', label);

      input.type = group.type === 'radio' ? 'radio' : 'checkbox';
      input.name =
        group.type === 'radio'
          ? `overlay-${themeKey}-${group.name}`
          : null;

	  if (!have) {
		input.checked = true;
        group.type === 'radio'
          ? this._handleRadioGroup(group, entry.layer)
          : this._handleCheckbox(entry.layer, true);
        have = true;
      }

      L.DomEvent.on(input, 'click', () => {
        group.type === 'radio'
          ? this._handleRadioGroup(group, entry.layer)
          : this._handleCheckbox(entry.layer, input.checked);
      });

      label.append(` ${entry.name}`);
    });
  },

  _handleCheckbox: function (layer, enabled) {
    enabled
      ? this._map.addLayer(layer)
      : this._map.removeLayer(layer);

    enabled
      ? this._activeOverlays.add(layer)
      : this._activeOverlays.delete(layer);
  },

  _handleRadioGroup: function (group, selectedLayer) {
    group.layers.forEach(entry => {
      if (this._activeOverlays.has(entry.layer)) {
        this._map.removeLayer(entry.layer);
        this._activeOverlays.delete(entry.layer);
      }
    });

    this._map.addLayer(selectedLayer);
    this._activeOverlays.add(selectedLayer);
  }
});

