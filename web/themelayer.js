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

    // themes without any layer are not shown at all
    this._themeKeys = Object.keys(this.themes).filter(
      key => this._nonEmptyGroups(this.themes[key]).length
    );

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

    // with a single theme, there is nothing to choose from
    if (this._themeKeys.length > 1) {
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

      this._createThemeRadios();
    }

    this._overlayList = L.DomUtil.create(
      'div',
      'leaflet-control-layers-overlays',
      this._form
    );

    const initial =
      this._themeKeys.indexOf(this.options.defaultTheme) > -1
        ? this.options.defaultTheme
        : this._themeKeys[0];
    if (initial) this.applyTheme(initial);

    // with only a single layer overall, there is nothing to switch at all, the
    // layer itself was already added to the map by applyTheme
    if (this._allLayers().size < 2) {
      container.style.display = 'none';
      return container;
    }

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

  // the overlay groups of a theme which contain at least one layer
  _nonEmptyGroups: function (theme) {
    if (!theme) return [];
    return (theme.overlays || []).filter(
      group => group.layers && group.layers.length
    );
  },

  // all distinct layers over all themes
  _allLayers: function () {
    const layers = new Set();
    Object.keys(this.themes).forEach(key => {
      this._nonEmptyGroups(this.themes[key]).forEach(group => {
        group.layers.forEach(entry => layers.add(entry.layer));
      });
    });
    return layers;
  },

  _createThemeRadios: function () {
    this._themeKeys.forEach(key => {
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

    // empty overlay groups are not shown, and if only a single group is left,
    // there is no need to name it
    const groups = this._nonEmptyGroups(theme);
    groups.forEach(group => {
      this._buildOverlayGroup(group, themeKey, groups.length > 1);
    });

    this._activeTheme = themeKey;
    this._syncThemeUI();
  },

  _syncThemeUI: function () {
    if (!this._baseList) return;
    const radios = this._baseList.querySelectorAll('input');
    radios.forEach(radio => {
      radio.checked = radio.value === this._activeTheme;
    });
  },

  /* ---------------- OVERLAYS ---------------- */

  _buildOverlayGroup: function (group, themeKey, showHeader) {
    if (showHeader) {
      const header = L.DomUtil.create(
        'div',
        'leaflet-control-layers-group',
        this._overlayList
      );
      header.innerHTML = `<strong>${group.name}</strong>`;
    }

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

