// Culprit SQL — culpritsql.com. Theme sync (culprit:theme), popover dismissal, OS-aware downloads,
// accessible tabs, release category filtering, and clipboard buttons. Every page works without JS.

(() => {
  window.plausible = window.plausible || function () { (window.plausible.q = window.plausible.q || []).push(arguments); };
  window.plausible.init = window.plausible.init || function (options) { window.plausible.o = options || {}; };
  window.plausible.init();

  const THEME_KEY = "culprit:theme";
  const LEGACY_BLOG_KEY = "culprit-blog:theme";

  const syncPictureThemes = () => {
    const explicit = document.documentElement.dataset.theme;
    for (const source of document.querySelectorAll("source[data-dark-srcset]")) {
      if (explicit === "dark") {
        source.media = "all";
      } else if (explicit === "light") {
        source.media = "not all";
      } else {
        source.media = "(prefers-color-scheme: dark)";
      }
    }
  };
  syncPictureThemes();

  const themeBtn = document.getElementById("theme-toggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", () => {
      const root = document.documentElement;
      const current =
        root.dataset.theme ||
        (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
      const next = current === "dark" ? "light" : "dark";
      root.dataset.theme = next;
      try {
        localStorage.setItem(THEME_KEY, next);
        localStorage.setItem(LEGACY_BLOG_KEY, next);
      } catch {}
      syncPictureThemes();
    });
  }

  // Close <details> popovers (Products, Language, Mobile drawer) on outside click or Escape.
  const popovers = [...document.querySelectorAll("details.nav-dropdown, details.lang-menu, details.mobile-menu")];
  document.addEventListener("click", (e) => {
    for (const d of popovers) {
      if (d.open && !d.contains(e.target)) d.open = false;
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      for (const d of popovers) d.open = false;
    }
  });

  // OS-aware primary studio download button where present.
  const RELEASE = "https://github.com/culpritsql/culprit-sql-studio/releases/latest/download/";
  const PLATFORMS = {
    mac: {
      file: "Culprit-SQL-Studio-macOS.dmg",
      icon: '<svg viewBox="0 0 24 24"><path fill="currentColor" d="M16.4 12.6c0-2.3 1.9-3.4 2-3.5-1.1-1.6-2.8-1.8-3.4-1.8-1.4-.2-2.8.8-3.5.8s-1.8-.8-3-.8C7 7.3 5.6 8.2 4.8 9.6c-1.7 2.9-.4 7.2 1.2 9.6.8 1.2 1.8 2.5 3 2.4 1.2 0 1.7-.8 3.1-.8s1.9.8 3.1.8c1.3 0 2.1-1.2 2.9-2.3.9-1.3 1.3-2.6 1.3-2.7-.1 0-2.5-1-2.5-3.9ZM14.1 5.8c.6-.8 1.1-1.9 1-3-.9 0-2.1.6-2.7 1.4-.6.7-1.1 1.8-1 2.9 1 .1 2.1-.5 2.7-1.3Z"/></svg>',
    },
    windows: {
      file: "Culprit-SQL-Studio-Windows-x64-setup.exe",
      icon: '<svg viewBox="0 0 24 24"><path fill="currentColor" d="M3 5.5 10.5 4.4v7.2H3V5.5Zm0 13 7.5 1.1v-7.1H3v6Zm8.4 1.2L21 21v-8.5h-9.6v7.2Zm0-15.4v7.3H21V3l-9.6 1.3Z"/></svg>',
    },
    linux: {
      file: "Culprit-SQL-Studio-Linux-x86_64.AppImage",
      icon: '<svg viewBox="0 0 24 24"><path fill="currentColor" d="M4 5h16a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2Zm2.3 4.3L9.6 12l-3.3 2.7 1.3 1.5L12.7 12 7.6 7.8 6.3 9.3ZM12 15h6v-2h-6v2Z"/></svg>',
    },
  };

  const ua = navigator.userAgent || "";
  const platform = (navigator.userAgentData && navigator.userAgentData.platform) || navigator.platform || "";
  const mobile =
    (navigator.userAgentData && navigator.userAgentData.mobile) ||
    /iphone|ipad|ipod|android|mobile/i.test(ua) ||
    (/mac/i.test(platform) && navigator.maxTouchPoints > 1);
  const os = mobile ? null : /mac/i.test(platform) ? "mac" : /win/i.test(platform) ? "windows" : /linux|x11/i.test(platform) ? "linux" : null;

  if (os && PLATFORMS[os]) {
    const p = PLATFORMS[os];
    for (const button of document.querySelectorAll("[data-os-download]")) {
      button.href = RELEASE + p.file;
      const customLabel = button.getAttribute(`data-label-${os}`);
      const labelSpan = button.querySelector("[data-os-label]");
      if (labelSpan && customLabel) labelSpan.textContent = customLabel;
      const iconSpan = button.querySelector(".os-icon");
      if (iconSpan) iconSpan.innerHTML = p.icon;
    }
    document.querySelector(`.dl[data-os="${os}"]`)?.classList.add("current");
  }

  // GitHub star count when >= 25.
  const stars = document.querySelector("[data-stars]");
  if (stars) {
    fetch("https://api.github.com/repos/culpritsql/culprit-sql-studio", { headers: { Accept: "application/vnd.github+json" } })
      .then((r) => (r.ok ? r.json() : null))
      .then((repo) => {
        const n = repo && Number(repo.stargazers_count);
        if (!Number.isFinite(n) || n < 25) return;
        stars.textContent = `★ ${n >= 1000 ? (n / 1000).toFixed(1) + "k" : n}`;
        stars.hidden = false;
      })
      .catch(() => {});
  }

  // Accessible tablists (platform surface switcher, sample diagnoses, agent setup).
  for (const tablist of document.querySelectorAll('[role="tablist"]')) {
    const tabs = [...tablist.querySelectorAll('[role="tab"]')];
    if (!tabs.length) continue;
    const select = (tab) => {
      for (const t of tabs) {
        const on = t === tab;
        t.setAttribute("aria-selected", String(on));
        t.tabIndex = on ? 0 : -1;
        const panel = document.getElementById(t.getAttribute("aria-controls"));
        if (panel) panel.hidden = !on;
      }
    };
    for (const tab of tabs) {
      tab.addEventListener("click", () => select(tab));
      tab.addEventListener("keydown", (e) => {
        const i = tabs.indexOf(tab);
        const next =
          e.key === "ArrowRight" ? tabs[(i + 1) % tabs.length] : e.key === "ArrowLeft" ? tabs[(i - 1 + tabs.length) % tabs.length] : null;
        if (next) {
          e.preventDefault();
          select(next);
          next.focus();
        }
      });
    }
  }

  // Release Notes category filter pills.
  const filterBtns = [...document.querySelectorAll("[data-release-filter]")];
  if (filterBtns.length) {
    const entries = [...document.querySelectorAll("[data-release-group]")];
    for (const btn of filterBtns) {
      btn.addEventListener("click", () => {
        const target = btn.dataset.releaseFilter;
        for (const b of filterBtns) b.setAttribute("aria-pressed", String(b === btn));
        for (const el of entries) {
          el.hidden = target !== "all" && el.dataset.releaseGroup !== target;
        }
      });
    }
  }

  // Copy buttons for terminal commands.
  const announce = document.querySelector("[data-announce]");
  const extractCommands = (pre) =>
    pre.textContent
      .split("\n")
      .map((line) => line.replace(/^\$ /, "").replace(/\s+#.*$/, ""))
      .filter((line) => line.trim() && !line.trim().startsWith("#") && !line.trim().startsWith("//"))
      .join("\n");

  for (const button of document.querySelectorAll("[data-copy-target]")) {
    button.addEventListener("click", async () => {
      const target = button.dataset.copyTarget;
      const pre =
        target === "setup"
          ? document.querySelector("pre[data-setup]:not([hidden])")
          : target === "active-tab"
            ? button.closest(".terminal")?.querySelector("pre:not([hidden])")
            : document.getElementById(target) || button.closest(".terminal")?.querySelector("pre:not([hidden])");
      if (!pre) return;
      const original = button.textContent;
      try {
        await navigator.clipboard.writeText(extractCommands(pre));
        button.textContent = "Copied";
        if (announce) announce.textContent = "Copied to the clipboard";
      } catch {
        const range = document.createRange();
        range.selectNodeContents(pre);
        const selection = getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
        button.textContent = "Selected";
      }
      setTimeout(() => {
        button.textContent = original;
      }, 1500);
    });
  }
})();
