// Applies a remembered theme before first paint. Loaded as a classic script
// (not a module) so it runs before the stylesheet renders.
(function () {
  try {
    var t = localStorage.getItem("fleet-router-theme");
    if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
  } catch (e) {}
})();
