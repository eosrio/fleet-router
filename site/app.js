(function () {
  "use strict";

  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  /* ---------- theme ---------- */

  var toggle = document.getElementById("theme-toggle");
  var root = document.documentElement;

  function currentTheme() {
    var set = root.getAttribute("data-theme");
    if (set === "light" || set === "dark") return set;
    return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  }

  function labelToggle() {
    if (!toggle) return;
    var next = currentTheme() === "light" ? "dark" : "light";
    toggle.setAttribute("aria-label", "Switch to " + next + " theme");
  }

  if (toggle) {
    toggle.addEventListener("click", function () {
      var next = currentTheme() === "light" ? "dark" : "light";
      root.setAttribute("data-theme", next);
      try { localStorage.setItem("fleet-router-theme", next); } catch (e) {}
      labelToggle();
    });
    labelToggle();
  }

  /* ---------- copy ---------- */

  var status = document.getElementById("copy-status");

  function announce(message) {
    if (!status) return;
    status.textContent = "";
    window.setTimeout(function () { status.textContent = message; }, 30);
  }

  function fallbackCopy(text) {
    var area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.top = "0";
    area.style.left = "0";
    area.style.opacity = "0";
    document.body.appendChild(area);
    area.focus();
    area.select();
    var ok = false;
    try { ok = document.execCommand("copy"); } catch (err) { ok = false; }
    document.body.removeChild(area);
    return ok;
  }

  document.querySelectorAll(".copy").forEach(function (button) {
    var timer = null;
    function setState(state, message) {
      button.setAttribute("data-state", state);
      announce(message);
      window.clearTimeout(timer);
      timer = window.setTimeout(function () { button.setAttribute("data-state", "idle"); }, 1600);
    }
    button.addEventListener("click", function () {
      var id = button.getAttribute("data-copy-target");
      var node = id ? document.getElementById(id) : null;
      var text = node ? node.textContent.replace(/^\n/, "").replace(/\s+$/, "") : "";
      var holder = button.closest("[data-copy-name]");
      var name = holder ? holder.getAttribute("data-copy-name") : (button.getAttribute("aria-label") || "snippet").replace(/^Copy /, "");
      var done = function () { setState("done", "Copied " + name + "."); };
      var failed = function () { setState("failed", "Could not copy " + name + "."); };
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(done, function () {
          if (fallbackCopy(text)) done(); else failed();
        });
      } else if (fallbackCopy(text)) {
        done();
      } else {
        failed();
      }
    });
  });

  /* ---------- panel ---------- */

  var NS = "http://www.w3.org/2000/svg";

  function el(name, attrs, text) {
    var node = document.createElementNS(NS, name);
    for (var k in attrs) {
      if (Object.prototype.hasOwnProperty.call(attrs, k)) node.setAttribute(k, attrs[k]);
    }
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function pointsToPath(pts) {
    return pts.map(function (p, i) { return (i ? "L" : "M") + p[0] + " " + p[1]; }).join(" ");
  }

  var BLOCK_HYPERION = "#394,810,203";
  var BLOCK_HYPERION_NEXT = "#394,810,204";
  var BLOCK_ATOMIC = "#394,809,977";

  var platforms = [
    { id: "hyperion", name: "Hyperion", sub: "get_blocks_request_v1", shortSub: "get_blocks v1" },
    { id: "atomic", name: "Atomic", sub: "get_blocks_request_v0", shortSub: "get_blocks v0" },
    { id: "indexer", name: "Indexer", sub: "any SHiP consumer", shortSub: "any SHiP consumer" }
  ];

  var stations = [
    { id: "a", name: "SHiP A", addr: "10.0.1.10:8080", range: "trace 2 … 394,810,205" },
    { id: "b", name: "SHiP B", addr: "10.0.1.11:8080", range: "trace 394,000,000 … 394,810,205" },
    { id: "c", name: "SHiP C", addr: "10.0.1.12:8080", range: "chain state not advancing" }
  ];

  function landscape() {
    var R = [60, 170, 280];
    return {
      cls: "board-l",
      hatch: "hatch-l",
      viewBox: "0 0 1200 340",
      platforms: platforms.map(function (p, i) {
        return { id: p.id, x: 40, y: R[i] - 24, w: 180, h: 48, name: p.name, sub: p.sub };
      }),
      junction: { x: 380, y: 110, w: 220, h: 120, title: "fleet-router", l1: "range-aware · least-connections", l2: "ws :17000 · /ready/eos" },
      stations: stations.map(function (s, i) {
        return { id: s.id, x: 900, y: R[i] - 33, w: 260, h: 66, name: s.name, addr: s.addr, range: s.range, lamp: [1142, R[i] - 16] };
      }),
      tracks: [
        { id: "indexer", pts: [[220, R[2]], [250, R[2]], [360, R[1]], [380, R[1]]] },
        { id: "hyperion", pts: [[220, R[0]], [250, R[0]], [360, R[1]], [380, R[1]]] },
        { id: "atomic", pts: [[220, R[1]], [380, R[1]]] },
        { id: "trunk", pts: [[600, R[1]], [660, R[1]]] },
        { id: "c", pts: [[660, R[1]], [770, R[2]], [900, R[2]]] },
        { id: "b", pts: [[660, R[1]], [900, R[1]]] },
        { id: "a", pts: [[660, R[1]], [770, R[0]], [900, R[0]]] }
      ],
      ticks: [[800, R[0]], [850, R[0]], [800, R[1]], [850, R[1]], [800, R[2]], [850, R[2]]],
      labels: { a: [888, R[0] - 12, "end"], b: [888, R[1] - 12, "end"], c: [888, R[2] + 24, "end"] },
      mark: [860, R[0]]
    };
  }

  function portrait() {
    var SY = [364, 468, 572];
    return {
      cls: "board-p",
      hatch: "hatch-p",
      viewBox: "0 0 390 652",
      platforms: platforms.map(function (p, i) {
        return { id: p.id, x: 20 + i * 120, y: 20, w: 110, h: 46, name: p.name, sub: p.shortSub, small: true };
      }),
      junction: { x: 40, y: 170, w: 310, h: 96, title: "fleet-router", l1: "range-aware · least-connections", l2: "ws :17000 · /ready/eos" },
      stations: stations.map(function (s, i) {
        return { id: s.id, x: 100, y: SY[i], w: 270, h: 66, name: s.name, addr: s.addr, range: s.range, lamp: [348, SY[i] + 17] };
      }),
      tracks: [
        { id: "indexer", pts: [[315, 66], [315, 96], [261, 150], [195, 150], [195, 170]] },
        { id: "hyperion", pts: [[75, 66], [75, 96], [129, 150], [195, 150], [195, 170]] },
        { id: "atomic", pts: [[195, 66], [195, 170]] },
        { id: "trunk", pts: [[195, 266], [195, 274], [135, 334], [60, 334]] },
        { id: "c", pts: [[60, 334], [60, SY[2] + 33], [100, SY[2] + 33]] },
        { id: "b", pts: [[60, 334], [60, SY[1] + 33], [100, SY[1] + 33]] },
        { id: "a", pts: [[60, 334], [60, SY[0] + 33], [100, SY[0] + 33]] }
      ],
      ticks: [],
      labels: { a: [70, SY[0] - 12, "start"], b: [70, SY[1] - 12, "start"], c: [70, SY[2] - 12, "start"] },
      mark: [80, SY[0] + 33]
    };
  }

  var scenarios = {
    steady: {
      tracks: { hyperion: "set", atomic: "set", indexer: "idle", trunk: "set", a: "set", b: "set", c: "stale" },
      lamps: { a: "online", b: "online", c: "stale" },
      labels: {
        a: ["Hyperion · " + BLOCK_HYPERION, "set"],
        b: ["Atomic · " + BLOCK_ATOMIC, "set"],
        c: ["stale · deprioritized", "stale"]
      },
      mark: false,
      caption: "Hyperion's stream is set to SHiP A, whose trace range covers its start block. Atomic took the least-loaded healthy node, SHiP B. SHiP C stopped advancing its chain state, so it is flagged stale and deprioritized, not excluded."
    },
    range: {
      tracks: { hyperion: "set", atomic: "set", indexer: "set", trunk: "set", a: "set", b: "set", c: "stale" },
      lamps: { a: "online", b: "online", c: "stale" },
      labels: {
        a: ["Indexer · start #12,000,000 · in range", "set"],
        b: ["trace begins 394,000,000", "stale"],
        c: ["stale · deprioritized", "stale"]
      },
      mark: false,
      caption: "A new indexer asks for blocks from #12,000,000. SHiP B has fewer connections, but its trace history begins at 394,000,000. Nodes whose trace range covers the block rank first, fresh before stale; least-connections only breaks ties inside a rank. The stream is set to SHiP A."
    },
    drop: {
      tracks: { hyperion: "set", atomic: "set", indexer: "idle", trunk: "set", a: "failed", b: "diverted", c: "stale" },
      lamps: { a: "offline", b: "online", c: "stale" },
      labels: {
        a: ["dropped at " + BLOCK_HYPERION, "failed"],
        b: ["resume " + BLOCK_HYPERION_NEXT + " · 1 dup dropped", "diverted"],
        c: ["stale · deprioritized", "stale"]
      },
      mark: true,
      caption: "SHiP A closed the socket mid-stream. fleet-router re-sent Hyperion's <code>get_blocks</code> from the next block on SHiP B, dropped the one replayed block Hyperion already had, and replayed the frames Hyperion sent during the swap. The client socket never closed."
    },
    corrupt: {
      tracks: { hyperion: "set", atomic: "set", indexer: "idle", trunk: "set", a: "quarantined", b: "diverted", c: "stale" },
      lamps: { a: "quarantined", b: "online", c: "stale" },
      labels: {
        a: ["quarantined " + BLOCK_HYPERION + " · 600 s", "quarantined"],
        b: ["resume " + BLOCK_HYPERION_NEXT + " on B", "diverted"],
        c: ["stale · deprioritized", "stale"]
      },
      mark: false,
      caption: "A damaged SHiP log makes SHiP A drop every client at " + BLOCK_HYPERION + " with no close frame. After two unclean drops there within 300 s, opt-in <code>block_failure_quarantine</code> skips SHiP A for that block for 600 s; it stays online for every other range. A session that keeps dying there backs off and is closed with code 1011 after five drops."
    }
  };

  var STATE_CLASSES = ["is-idle", "is-set", "is-diverted", "is-failed", "is-stale", "is-quarantined", "is-online", "is-offline"];

  function setState(node, state) {
    STATE_CLASSES.forEach(function (c) { node.classList.remove(c); });
    if (state) node.classList.add("is-" + state);
  }

  function buildBoard(layout) {
    var svg = el("svg", { viewBox: layout.viewBox, class: "board " + layout.cls, "aria-hidden": "true", focusable: "false" });
    var defs = el("defs", {});
    var pattern = el("pattern", { id: layout.hatch, class: "hatch", width: "6", height: "6", patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" });
    pattern.appendChild(el("rect", { width: "2.5", height: "6" }));
    defs.appendChild(pattern);
    svg.appendChild(defs);

    var refs = { tracks: {}, lamps: {}, labels: {}, mark: null, paths: [] };

    layout.ticks.forEach(function (t) {
      svg.appendChild(el("line", { class: "tick", x1: t[0], y1: t[1] - 7, x2: t[0], y2: t[1] + 7 }));
    });

    layout.tracks.forEach(function (t) {
      var path = el("path", { class: "track", d: pointsToPath(t.pts) });
      svg.appendChild(path);
      refs.tracks[t.id] = path;
      refs.paths.push(path);
    });

    layout.platforms.forEach(function (p) {
      var g = el("g", {});
      g.appendChild(el("rect", { class: "box", x: p.x, y: p.y, width: p.w, height: p.h, rx: "3" }));
      g.appendChild(el("text", { class: "t-name", x: p.x + 12, y: p.y + (p.small ? 19 : 20) }, p.name));
      g.appendChild(el("text", { class: "t-mono", x: p.x + 12, y: p.y + (p.small ? 35 : 37) }, p.sub));
      svg.appendChild(g);
    });

    var j = layout.junction;
    var jg = el("g", {});
    jg.appendChild(el("rect", { class: "box box-junction", x: j.x, y: j.y, width: j.w, height: j.h, rx: "3" }));
    jg.appendChild(el("text", { class: "t-junction", x: j.x + 20, y: j.y + 40 }, j.title));
    jg.appendChild(el("text", { class: "t-mono", x: j.x + 20, y: j.y + 64 }, j.l1));
    jg.appendChild(el("text", { class: "t-mono", x: j.x + 20, y: j.y + 84 }, j.l2));
    svg.appendChild(jg);

    layout.stations.forEach(function (s) {
      var g = el("g", {});
      g.appendChild(el("rect", { class: "box", x: s.x, y: s.y, width: s.w, height: s.h, rx: "3" }));
      g.appendChild(el("text", { class: "t-name", x: s.x + 14, y: s.y + 22 }, s.name));
      g.appendChild(el("text", { class: "t-mono", x: s.x + 14, y: s.y + 40 }, s.addr));
      g.appendChild(el("text", { class: "t-mono", x: s.x + 14, y: s.y + 56 }, s.range));
      var lamp = el("circle", { class: "lamp", cx: s.lamp[0], cy: s.lamp[1], r: "5" });
      g.appendChild(lamp);
      svg.appendChild(g);
      refs.lamps[s.id] = lamp;
    });

    Object.keys(layout.labels).forEach(function (id) {
      var l = layout.labels[id];
      var text = el("text", { class: "t-label", x: l[0], y: l[1], "text-anchor": l[2] }, "");
      svg.appendChild(text);
      refs.labels[id] = text;
    });

    var m = layout.mark;
    var mark = el("g", { class: "mark" });
    mark.appendChild(el("path", { d: "M" + (m[0] - 5) + " " + (m[1] - 5) + " L" + (m[0] + 5) + " " + (m[1] + 5) + " M" + (m[0] + 5) + " " + (m[1] - 5) + " L" + (m[0] - 5) + " " + (m[1] + 5) }));
    svg.appendChild(mark);
    refs.mark = mark;

    refs.svg = svg;
    return refs;
  }

  function applyScenario(refs, sc) {
    Object.keys(sc.tracks).forEach(function (id) { if (refs.tracks[id]) setState(refs.tracks[id], sc.tracks[id]); });
    Object.keys(sc.lamps).forEach(function (id) { if (refs.lamps[id]) setState(refs.lamps[id], sc.lamps[id]); });
    Object.keys(sc.labels).forEach(function (id) {
      var node = refs.labels[id];
      if (!node) return;
      node.textContent = sc.labels[id][0];
      setState(node, sc.labels[id][1]);
    });
    refs.mark.classList.toggle("is-on", !!sc.mark);
  }

  function drawIn(refs) {
    if (reduceMotion || !refs.paths.length || typeof refs.paths[0].getTotalLength !== "function") return;
    var order = ["indexer", "hyperion", "atomic", "trunk", "c", "b", "a"];
    order.forEach(function (id, i) {
      var path = refs.tracks[id];
      if (!path || !path.animate) return;
      var len = path.getTotalLength();
      path.style.strokeDasharray = len + " " + len;
      var anim = path.animate(
        [{ strokeDashoffset: len }, { strokeDashoffset: 0 }],
        { duration: 520, delay: 90 + i * 110, easing: "cubic-bezier(0.16, 1, 0.3, 1)", fill: "backwards" }
      );
      anim.onfinish = function () { path.style.strokeDasharray = ""; };
    });
  }

  var board = document.getElementById("panel-board");
  var caption = document.getElementById("panel-caption");
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".scenario [role=tab]"));

  if (board) {
    var boards = [buildBoard(landscape()), buildBoard(portrait())];
    boards.forEach(function (b) { board.appendChild(b.svg); });

    function show(name) {
      var sc = scenarios[name] || scenarios.steady;
      boards.forEach(function (b) { applyScenario(b, sc); });
      if (caption) caption.innerHTML = sc.caption;
      tabs.forEach(function (tab) {
        var on = tab.getAttribute("data-scenario") === name;
        tab.setAttribute("aria-selected", on ? "true" : "false");
        tab.setAttribute("tabindex", on ? "0" : "-1");
      });
    }

    show("steady");
    boards.forEach(drawIn);

    tabs.forEach(function (tab, i) {
      tab.addEventListener("click", function () { show(tab.getAttribute("data-scenario")); });
      tab.addEventListener("keydown", function (e) {
        var next = null;
        if (e.key === "ArrowRight" || e.key === "ArrowDown") next = tabs[(i + 1) % tabs.length];
        if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = tabs[(i - 1 + tabs.length) % tabs.length];
        if (e.key === "Home") next = tabs[0];
        if (e.key === "End") next = tabs[tabs.length - 1];
        if (next) {
          e.preventDefault();
          next.focus();
          show(next.getAttribute("data-scenario"));
        }
      });
    });
  }
})();
