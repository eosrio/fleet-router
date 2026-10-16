(function () {
  "use strict";

  var status = document.getElementById("copy-status");

  function announce(message) {
    if (!status) {
      return;
    }
    status.textContent = "";
    window.setTimeout(function () {
      status.textContent = message;
    }, 30);
  }

  function flash(button, label) {
    var original = button.getAttribute("data-label") || "Copy";
    button.textContent = label;
    window.setTimeout(function () {
      button.textContent = original;
    }, 1600);
  }

  function fallback(text) {
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
    try {
      ok = document.execCommand("copy");
    } catch (err) {
      ok = false;
    }
    document.body.removeChild(area);
    return ok;
  }

  function textFor(button) {
    var targetId = button.getAttribute("data-copy-target");
    var node = targetId ? document.getElementById(targetId) : null;
    if (node) {
      return node.textContent.replace(/^\n/, "").replace(/\s+$/, "");
    }
    return button.getAttribute("data-copy-text") || "";
  }

  document.querySelectorAll(".copy").forEach(function (button) {
    button.addEventListener("click", function () {
      var text = textFor(button);
      var name = button.getAttribute("data-copy-name") || "snippet";

      function success() {
        flash(button, "Copied");
        announce("Copied " + name + ".");
      }

      function failure() {
        flash(button, "Failed");
        announce("Could not copy " + name + ".");
      }

      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(success, function () {
          if (fallback(text)) {
            success();
          } else {
            failure();
          }
        });
      } else if (fallback(text)) {
        success();
      } else {
        failure();
      }
    });
  });
})();
