(function () {
  const STORAGE_KEY = "bemcs-bg-enabled";
  const SLOT_HOURS = 12;

  /*
    Tumia picha zako mbili tu hapa.
    Weka picha hizi ndani ya:
    public/engine-bg/

    Example:
    public/engine-bg/customs-port-01.jpg
    public/engine-bg/customs-port-02.jpg
  */
  const BACKGROUNDS = [
    "/engine-bg/customs-port-01.jpg",
    "/engine-bg/customs-port-02.jpg"
  ];

  function getCurrentSlotIndex() {
    const slot = Math.floor(Date.now() / (SLOT_HOURS * 60 * 60 * 1000));
    return slot % BACKGROUNDS.length;
  }

  function ensureLayer() {
    let layer = document.getElementById("engine-bg-layer");
    if (layer) return layer;

    layer = document.createElement("div");
    layer.id = "engine-bg-layer";
    layer.className = "engine-bg-layer";

    const image = document.createElement("div");
    image.className = "engine-bg-image";

    const overlay = document.createElement("div");
    overlay.className = "engine-bg-overlay";

    layer.appendChild(image);
    layer.appendChild(overlay);

    document.body.prepend(layer);
    document.body.classList.add("engine-bg-enabled");

    return layer;
  }

  function setBackgroundImage(url) {
    const layer = ensureLayer();
    const image = layer.querySelector(".engine-bg-image");
    if (!image) return;

    image.style.backgroundImage = `url("${url}")`;
    image.setAttribute("data-bg", url);
  }

  function applyScheduledBackground() {
    const enabled = localStorage.getItem(STORAGE_KEY);
    if (enabled === "false") return;

    const index = getCurrentSlotIndex();
    const url = BACKGROUNDS[index];
    setBackgroundImage(url);
  }

  function scheduleRefresh() {
    const intervalMs = 15 * 60 * 1000;
    setInterval(applyScheduledBackground, intervalMs);
  }

  function init() {
    if (!document.body) return;
    applyScheduledBackground();
    scheduleRefresh();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.BEMCSBackground = {
    refresh: applyScheduledBackground,
    disable: function () {
      localStorage.setItem(STORAGE_KEY, "false");
      const layer = document.getElementById("engine-bg-layer");
      if (layer) layer.remove();
      document.body.classList.remove("engine-bg-enabled");
    },
    enable: function () {
      localStorage.setItem(STORAGE_KEY, "true");
      applyScheduledBackground();
    }
  };
})();