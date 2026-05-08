#!/usr/bin/env python3
"""
tape_index.py — generate a browsable, searchable HTML index of one or more
parent directories. Designed for cataloging tape-library backups: each parent
directory ("chapter") becomes one HTML page, and a master index.html lists
every chapter and runs a global search across all of them.

Runs anywhere with Python 3.8+; tested on Windows over CIFS-mapped drives.

USAGE
    # Pass directories directly (chapter name = directory's basename):
    python tools\\tape_index.py -o C:\\tape-catalog Z:\\Tape001 Z:\\Tape002

    # Or list them in a text file, one per line. Lines may be either
    #     <path>
    # or
    #     <path>|<chapter name>
    # Blank lines and lines starting with # are ignored.
    python tools\\tape_index.py -o C:\\tape-catalog --from-file dirs.txt

OUTPUT
    <output>/
        index.html              master view + global search
        tape_index_app.js       shared client-side JS (filter / sort / search)
        manifest.json           machine-readable list of known chapters
        <chapter>.html          per-chapter browser (one per parent dir)
        <chapter>.data.js       per-chapter file list, loaded by both views

Open index.html by double-clicking it; no web server is required.

RE-RUNNING / INCREMENTAL UPDATES
    Re-running with the same -o keeps existing chapters in manifest.json
    (as long as their <chapter>.data.js files are still on disk) and
    overwrites any chapter you re-index in this run. To remove a chapter,
    delete its <chapter>.html and <chapter>.data.js then re-run with -o
    pointing at the same directory (the manifest will drop it).

OVER CIFS / NETWORK DRIVES
    Indexing reads directory listings and one stat() per file. Throughput
    is bound by the network filesystem; expect roughly a few thousand
    files/sec over gigabit CIFS, vs. tens to hundreds of thousands locally.
    UNC paths (\\\\server\\share\\...) work directly. Transient read errors
    on individual subdirectories or files are logged to stderr and skipped.

LIMITS
    The browser loads each chapter's data file in one piece. Plan for
    roughly 1-2 million files per chapter (data files in the 100-300 MB
    range stay responsive). If a single tape contains many millions of
    files, split it into multiple chapters by sub-directory.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path


PROGRESS_INTERVAL_FILES = 50_000
PROGRESS_INTERVAL_SECONDS = 5.0


# ---------- file scanning ----------

def scan_directory(root, on_progress=None):
    """
    Walk root recursively. Yield (rel_posix_path, size_bytes, mtime_unix).
    Symlinks are not followed. Read errors on subdirectories or individual
    files are logged to stderr and skipped.
    """
    root_abs = os.path.abspath(root)
    stack = [root_abs]
    count = 0
    last_progress = time.monotonic()
    while stack:
        current = stack.pop()
        try:
            it = os.scandir(current)
        except OSError as e:
            print(f"  warning: cannot list {current}: {e}", file=sys.stderr)
            continue
        with it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    try:
                        st = entry.stat()
                    except OSError as e:
                        print(f"  warning: cannot stat {entry.path}: {e}", file=sys.stderr)
                        continue
                    rel = os.path.relpath(entry.path, root_abs).replace("\\", "/")
                    yield (rel, int(st.st_size), float(st.st_mtime))
                    count += 1
                    if on_progress is not None:
                        now = time.monotonic()
                        if (count % PROGRESS_INTERVAL_FILES == 0
                                or now - last_progress >= PROGRESS_INTERVAL_SECONDS):
                            on_progress(count)
                            last_progress = now
                except OSError as e:
                    print(f"  warning: error on {entry.path}: {e}", file=sys.stderr)
                    continue
    if on_progress is not None:
        on_progress(count)


# ---------- chapter naming ----------

_INVALID_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_id(raw):
    """Reduce raw to a filename-safe id (letters/digits/._-)."""
    s = _INVALID_CHARS.sub("_", raw).strip("._-")
    return s or "chapter"


def unique_id(base, used):
    """Return base if free, otherwise base_2, base_3, etc."""
    if base not in used:
        return base
    n = 2
    while f"{base}_{n}" in used:
        n += 1
    return f"{base}_{n}"


# ---------- input parsing ----------

def parse_from_file(path):
    """
    Read a list of directories from a text file. Returns list of
    (path, optional_name). Format per line:
        <path>
        <path>|<chapter name>
    Blank lines and lines beginning with # are ignored.
    """
    out = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "|" in line:
                p, name = line.split("|", 1)
                out.append((p.strip(), name.strip() or None))
            else:
                out.append((line, None))
    return out


# ---------- chapter writing ----------

def js_safe_dump(obj):
    """JSON dump for embedding in a <script> block — escape </ to avoid breaking out."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def write_chapter_data(out_dir, chapter_id, label, source_path, entries):
    """
    Write <chapter>.data.js. Returns dict with metadata for the manifest.
    `entries` is a list of (rel_path, size, mtime) tuples.
    """
    total_size = sum(e[1] for e in entries)
    indexed_at = int(time.time())
    payload = {
        "id": chapter_id,
        "label": label,
        "sourcePath": source_path,
        "indexedAt": indexed_at,
        "totalFiles": len(entries),
        "totalSize": total_size,
        "entries": entries,
    }
    body = (
        "window.TAPE_INDEX_DATA = window.TAPE_INDEX_DATA || {};\n"
        f"window.TAPE_INDEX_DATA[{json.dumps(chapter_id)}] = {js_safe_dump(payload)};\n"
    )
    (out_dir / f"{chapter_id}.data.js").write_text(body, encoding="utf-8")
    return {
        "id": chapter_id,
        "label": label,
        "sourcePath": source_path,
        "indexedAt": indexed_at,
        "totalFiles": len(entries),
        "totalSize": total_size,
    }


def write_chapter_html(out_dir, chapter_id, label):
    html = (CHAPTER_HTML
            .replace("__LABEL__", html_escape(label))
            .replace("__CHAPTER_ID__", chapter_id)
            .replace("__CHAPTER_ID_JSON__", json.dumps(chapter_id)))
    (out_dir / f"{chapter_id}.html").write_text(html, encoding="utf-8")


def write_master(out_dir, title, manifest):
    html = (MASTER_HTML
            .replace("__TITLE__", html_escape(title))
            .replace("__MANIFEST_JSON__", js_safe_dump(manifest)))
    (out_dir / "index.html").write_text(html, encoding="utf-8")


def write_app_js(out_dir):
    (out_dir / "tape_index_app.js").write_text(APP_JS, encoding="utf-8")


def html_escape(s):
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


# ---------- formatting helpers (for stderr progress) ----------

def fmt_size(b):
    if b < 1024:
        return f"{b} B"
    units = ("KB", "MB", "GB", "TB", "PB")
    v = float(b)
    i = -1
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    if v >= 100:
        return f"{v:.0f} {units[i]}"
    if v >= 10:
        return f"{v:.1f} {units[i]}"
    return f"{v:.2f} {units[i]}"


def fmt_elapsed(sec):
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m {sec % 60}s"
    return f"{sec // 3600}h {(sec % 3600) // 60}m {sec % 60}s"


# ---------- main ----------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-o", "--output", required=True,
                        help="Output directory for the catalog. Created if missing.")
    parser.add_argument("dirs", nargs="*",
                        help="Parent directories to index (one chapter per directory).")
    parser.add_argument("--from-file", dest="from_file", metavar="FILE",
                        help='Read directories from a text file. One entry per line, '
                             'either "<path>" or "<path>|<chapter name>". '
                             'Blank/# lines ignored.')
    parser.add_argument("--title", default="Tape Library Index",
                        help='Title for the master index page (default: "Tape Library Index").')
    args = parser.parse_args(argv)

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    work = [(p, None) for p in args.dirs]
    if args.from_file:
        work.extend(parse_from_file(args.from_file))
    if not work:
        parser.error("no directories given (pass paths as args or use --from-file)")

    # Validate input dirs
    valid = []
    for raw_path, custom_name in work:
        path = Path(raw_path)
        if not path.is_dir():
            print(f"  skipping (not a directory or unreadable): {path}", file=sys.stderr)
            continue
        valid.append((path, custom_name))
    if not valid:
        sys.exit("No valid directories to index.")

    # Load existing manifest
    manifest_path = output / "manifest.json"
    existing = []
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
        except (OSError, json.JSONDecodeError) as e:
            print(f"  warning: could not read existing manifest: {e}", file=sys.stderr)
            existing = []
    by_id = {e["id"]: e for e in existing if isinstance(e, dict) and "id" in e}
    existing_by_path = {e.get("sourcePath"): e["id"]
                        for e in by_id.values() if e.get("sourcePath")}

    # Index each directory
    used_ids = set(by_id.keys())
    for i, (path, custom_name) in enumerate(valid, start=1):
        source_path = str(path.resolve())
        label = custom_name or path.name or source_path
        if source_path in existing_by_path:
            chapter_id = existing_by_path[source_path]
        else:
            chapter_id = unique_id(sanitize_id(label), used_ids)
            used_ids.add(chapter_id)

        print(f"[{i}/{len(valid)}] Indexing {source_path}", file=sys.stderr)
        print(f"        chapter: {chapter_id} ({label!r})", file=sys.stderr)
        start = time.monotonic()

        def progress(n, _start=start):
            elapsed = time.monotonic() - _start
            rate = n / elapsed if elapsed > 0 else 0
            print(f"  scanned {n:,} files ({rate:,.0f}/s, elapsed {fmt_elapsed(elapsed)})",
                  file=sys.stderr)

        entries = list(scan_directory(path, on_progress=progress))
        elapsed = time.monotonic() - start
        meta = write_chapter_data(output, chapter_id, label, source_path, entries)
        write_chapter_html(output, chapter_id, label)
        by_id[chapter_id] = meta
        print(f"  done: {meta['totalFiles']:,} files, {fmt_size(meta['totalSize'])} "
              f"in {fmt_elapsed(elapsed)}", file=sys.stderr)

    # Drop manifest entries whose data file is gone (lets users prune by deleting files)
    pruned = []
    final = []
    for entry in by_id.values():
        if (output / f"{entry['id']}.data.js").exists():
            final.append(entry)
        else:
            pruned.append(entry["id"])
    if pruned:
        print(f"  pruned {len(pruned)} chapter(s) with missing data files: "
              f"{', '.join(pruned)}", file=sys.stderr)

    # Stable order: by label, case-insensitive
    final.sort(key=lambda e: (e.get("label") or e["id"]).lower())

    manifest_path.write_text(json.dumps(final, ensure_ascii=False, indent=2),
                             encoding="utf-8")
    write_app_js(output)
    write_master(output, args.title, final)

    total_files = sum(e["totalFiles"] for e in final)
    total_size = sum(e["totalSize"] for e in final)
    print(f"\nWrote catalog to {output}", file=sys.stderr)
    print(f"  {len(final)} chapter(s), {total_files:,} files, "
          f"{fmt_size(total_size)} total", file=sys.stderr)
    print(f"  open: {output / 'index.html'}", file=sys.stderr)
    return 0


# ---------- embedded client-side templates ----------

APP_JS = r"""
;(function () {
  'use strict';

  var RENDER_CAP = 5000;

  function fmtSize(b) {
    if (b < 1024) return b + ' B';
    var u = ['KB','MB','GB','TB','PB'], i = -1, v = b;
    do { v /= 1024; i++; } while (v >= 1024 && i < u.length - 1);
    return v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2) + ' ' + u[i];
  }
  function fmtDate(s) {
    if (!s) return '';
    var d = new Date(s * 1000);
    if (isNaN(d.getTime())) return '';
    var p = function (n) { return String(n).padStart(2, '0'); };
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate())
         + ' ' + p(d.getHours()) + ':' + p(d.getMinutes());
  }
  function basename(p) { var i = p.lastIndexOf('/'); return i >= 0 ? p.slice(i + 1) : p; }
  function dirname(p)  { var i = p.lastIndexOf('/'); return i >= 0 ? p.slice(0, i) : ''; }
  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function fmtNum(n) { try { return n.toLocaleString(); } catch (e) { return String(n); } }

  // Query syntax:
  //   /regex/        regular expression (add trailing i for case-insensitive)
  //   *.ext          extension match
  //   word1 word2    AND of substrings (case-insensitive)
  function compileQuery(q) {
    q = q.trim();
    if (!q) return null;
    var m = q.match(/^\/(.+)\/([i]?)$/);
    if (m) {
      try { return new RegExp(m[1], m[2] || ''); }
      catch (e) { return null; }
    }
    if (/^\*\.[A-Za-z0-9]+$/.test(q)) {
      var ext = q.slice(1).toLowerCase();
      return { test: function (s) { return s.toLowerCase().endsWith(ext); } };
    }
    var terms = q.toLowerCase().split(/\s+/).filter(Boolean);
    return { test: function (s) {
      var t = s.toLowerCase();
      for (var i = 0; i < terms.length; i++) if (t.indexOf(terms[i]) < 0) return false;
      return true;
    }};
  }

  function filterEntries(entries, q) {
    if (!q) return entries.slice();
    var out = [];
    for (var i = 0, n = entries.length; i < n; i++) {
      if (q.test(entries[i][0])) out.push(entries[i]);
    }
    return out;
  }

  function sortEntries(arr, key, desc) {
    var cmp;
    if (key === 'size')       cmp = function (a, b) { return a[1] - b[1]; };
    else if (key === 'mtime') cmp = function (a, b) { return a[2] - b[2]; };
    else if (key === 'path')  cmp = function (a, b) {
      var x = dirname(a[0]).toLowerCase(), y = dirname(b[0]).toLowerCase();
      return x < y ? -1 : x > y ? 1 : 0;
    };
    else if (key === 'chapter') cmp = function (a, b) {
      var x = String(a[3] || '').toLowerCase(), y = String(b[3] || '').toLowerCase();
      return x < y ? -1 : x > y ? 1 : 0;
    };
    else cmp = function (a, b) {
      var x = basename(a[0]).toLowerCase(), y = basename(b[0]).toLowerCase();
      return x < y ? -1 : x > y ? 1 : 0;
    };
    arr.sort(cmp);
    if (desc) arr.reverse();
    return arr;
  }

  function rowsHtml(entries, withChapter) {
    var n = Math.min(entries.length, RENDER_CAP), parts = new Array(n);
    for (var i = 0; i < n; i++) {
      var e = entries[i], p = e[0];
      var name = basename(p), dir = dirname(p);
      var chap = withChapter ? '<td>' + escHtml(e[3] || '') + '</td>' : '';
      parts[i] =
        '<tr>' +
          '<td class="name">' + escHtml(name) + '</td>' +
          '<td class="path">' + escHtml(dir) + '</td>' +
          '<td class="size">' + fmtSize(e[1]) + '</td>' +
          '<td class="mtime">' + fmtDate(e[2]) + '</td>' +
          chap +
        '</tr>';
    }
    return parts.join('');
  }

  function setSortIndicator(scope, th) {
    scope.querySelectorAll('th[data-sort]').forEach(function (x) {
      x.classList.remove('sort-asc', 'sort-desc');
    });
    th.classList.add(th._desc ? 'sort-desc' : 'sort-asc');
  }

  window.tapeIndexApp = {
    renderChapter: function (chapterId) {
      var data = (window.TAPE_INDEX_DATA || {})[chapterId];
      var metaEl = document.getElementById('meta');
      if (!data) { metaEl.textContent = 'No data loaded for ' + chapterId + '.'; return; }
      var entries = data.entries;
      var qInput = document.getElementById('q');
      var rowsEl = document.getElementById('rows');
      var countEl = document.getElementById('count');
      var sortKey = 'name', sortDesc = false;

      var indexedStr = data.indexedAt
        ? new Date(data.indexedAt * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' UTC'
        : '';
      metaEl.innerHTML =
        '<b>' + escHtml(data.label || chapterId) + '</b> &mdash; ' +
        fmtNum(data.totalFiles) + ' files, ' + fmtSize(data.totalSize) +
        (data.sourcePath ? ' &middot; <span class="path">' + escHtml(data.sourcePath) + '</span>' : '') +
        (indexedStr ? ' &middot; indexed ' + indexedStr : '');

      function rerender() {
        var q = compileQuery(qInput.value);
        var filtered = filterEntries(entries, q);
        sortEntries(filtered, sortKey, sortDesc);
        rowsEl.innerHTML = rowsHtml(filtered, false);
        var msg = fmtNum(filtered.length) + ' of ' + fmtNum(entries.length) + ' files';
        if (filtered.length > RENDER_CAP) {
          msg += ' (showing first ' + fmtNum(RENDER_CAP) + ' &mdash; refine search to see more)';
        }
        countEl.innerHTML = msg;
      }

      var debounceId = null;
      qInput.addEventListener('input', function () {
        clearTimeout(debounceId);
        debounceId = setTimeout(rerender, 200);
      });

      document.querySelectorAll('th[data-sort]').forEach(function (th) {
        th.addEventListener('click', function () {
          var k = th.getAttribute('data-sort');
          if (k === sortKey) sortDesc = !sortDesc;
          else { sortKey = k; sortDesc = (k === 'size' || k === 'mtime'); }
          th._desc = sortDesc;
          setSortIndicator(document, th);
          rerender();
        });
      });

      rerender();
    },

    renderMaster: function (manifest) {
      var listEl = document.getElementById('chapters');
      var summaryEl = document.getElementById('summary');
      var totalFiles = 0, totalSize = 0;
      var rows = manifest.map(function (c) {
        totalFiles += c.totalFiles || 0;
        totalSize  += c.totalSize  || 0;
        var idStr = encodeURIComponent(c.id);
        var idx = c.indexedAt ? new Date(c.indexedAt * 1000).toISOString().slice(0, 10) : '';
        return '<tr>' +
          '<td><a href="' + escHtml(idStr) + '.html">' + escHtml(c.label || c.id) + '</a></td>' +
          '<td class="size">' + fmtNum(c.totalFiles || 0) + '</td>' +
          '<td class="size">' + fmtSize(c.totalSize || 0) + '</td>' +
          '<td class="mtime">' + escHtml(idx) + '</td>' +
          (c.sourcePath ? '<td class="path">' + escHtml(c.sourcePath) + '</td>' : '<td></td>') +
        '</tr>';
      });
      listEl.innerHTML = rows.join('');
      summaryEl.innerHTML = manifest.length + ' chapter(s), ' +
        fmtNum(totalFiles) + ' files, ' + fmtSize(totalSize) + ' total';

      // Global search
      var loaded = {};
      var qInput = document.getElementById('q');
      var goBtn = document.getElementById('go');
      var resultsEl = document.getElementById('results');
      var statusEl = document.getElementById('status');
      var rowsEl = document.getElementById('result-rows');
      var sortKey = 'name', sortDesc = false;
      var lastEntries = null;

      function loadChapter(c) {
        return new Promise(function (resolve) {
          if (loaded[c.id]) { resolve(true); return; }
          var s = document.createElement('script');
          s.src = encodeURIComponent(c.id) + '.data.js';
          s.onload = function () { loaded[c.id] = true; resolve(true); };
          s.onerror = function () {
            console.warn('Failed to load ' + s.src);
            resolve(false);
          };
          document.head.appendChild(s);
        });
      }

      async function runSearch() {
        var qStr = qInput.value.trim();
        if (!qStr) { statusEl.textContent = 'Enter a search query.'; return; }
        var q = compileQuery(qStr);
        if (!q) { statusEl.textContent = 'Invalid query.'; return; }
        goBtn.disabled = true;
        resultsEl.style.display = 'block';
        var collected = [];
        for (var i = 0; i < manifest.length; i++) {
          var c = manifest[i];
          statusEl.textContent =
            'Searching ' + (i + 1) + ' / ' + manifest.length + ': ' + (c.label || c.id) +
            ' (' + fmtNum(collected.length) + ' matches so far)…';
          await loadChapter(c);
          var data = (window.TAPE_INDEX_DATA || {})[c.id];
          if (!data) continue;
          var ents = data.entries, label = c.label || c.id;
          for (var j = 0; j < ents.length; j++) {
            var e = ents[j];
            if (q.test(e[0])) collected.push([e[0], e[1], e[2], label]);
          }
        }
        lastEntries = collected;
        sortEntries(collected, sortKey, sortDesc);
        rowsEl.innerHTML = rowsHtml(collected, true);
        var msg = fmtNum(collected.length) + ' matches across ' + manifest.length + ' chapter(s)';
        if (collected.length > RENDER_CAP) {
          msg += ' (showing first ' + fmtNum(RENDER_CAP) + ' &mdash; refine to narrow)';
        }
        statusEl.innerHTML = msg;
        goBtn.disabled = false;
      }

      goBtn.addEventListener('click', runSearch);
      qInput.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') runSearch();
      });

      document.querySelectorAll('#results th[data-sort]').forEach(function (th) {
        th.addEventListener('click', function () {
          if (!lastEntries) return;
          var k = th.getAttribute('data-sort');
          if (k === sortKey) sortDesc = !sortDesc;
          else { sortKey = k; sortDesc = (k === 'size' || k === 'mtime'); }
          th._desc = sortDesc;
          setSortIndicator(resultsEl, th);
          sortEntries(lastEntries, sortKey, sortDesc);
          rowsEl.innerHTML = rowsHtml(lastEntries, true);
        });
      });
    }
  };
})();
"""


_BASE_STYLE = """
  body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif;
         margin: 1em 1.2em; color: #222; }
  h1 { font-size: 1.3em; margin: 0.2em 0; }
  h2 { font-size: 1.1em; margin: 1.4em 0 0.4em; }
  header { display: flex; align-items: baseline; gap: 1em; flex-wrap: wrap; }
  .meta { color: #555; font-size: 0.95em; margin: 0.4em 0; }
  .controls { display: flex; gap: 0.5em; align-items: center;
              flex-wrap: wrap; margin: 0.6em 0; }
  input[type=text] { padding: 0.4em 0.6em; font: inherit; min-width: 30em; flex: 1; }
  button { padding: 0.4em 1em; font: inherit; cursor: pointer; }
  table { border-collapse: collapse; width: 100%; font-size: 0.95em; }
  th, td { padding: 0.3em 0.6em; border-bottom: 1px solid #eee;
           vertical-align: top; }
  th { text-align: left; cursor: pointer; user-select: none;
       background: #f5f5f5; position: sticky; top: 0; }
  th.sort-asc::after  { content: " \\25B2"; }
  th.sort-desc::after { content: " \\25BC"; }
  td.size, th.size   { text-align: right; white-space: nowrap; }
  td.mtime, th.mtime { white-space: nowrap; }
  td.path  { color: #666; font-size: 0.85em; word-break: break-all; max-width: 50em; }
  td.name  { font-weight: 500; word-break: break-all; }
  .footer  { margin-top: 1.5em; color: #888; font-size: 0.85em; }
  a { color: #0a58ca; text-decoration: none; }
  a:hover { text-decoration: underline; }
""".strip()


CHAPTER_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Tape Index: __LABEL__</title>
<style>
""" + _BASE_STYLE + """
</style>
</head>
<body>
<header>
  <a href="index.html">&larr; All chapters</a>
  <h1>__LABEL__</h1>
</header>
<div class="meta" id="meta">Loading…</div>
<div class="controls">
  <input id="q" type="text" placeholder='Filter (substring; multiple words = AND; "*.ext" for extension; "/regex/i" for regex)' autofocus>
  <span id="count"></span>
</div>
<table>
<thead><tr>
  <th data-sort="name" class="sort-asc">Name</th>
  <th data-sort="path">Path</th>
  <th data-sort="size" class="size">Size</th>
  <th data-sort="mtime" class="mtime">Modified</th>
</tr></thead>
<tbody id="rows"></tbody>
</table>
<div class="footer">Generated by tools/tape_index.py</div>
<script src="__CHAPTER_ID__.data.js"></script>
<script src="tape_index_app.js"></script>
<script>tapeIndexApp.renderChapter(__CHAPTER_ID_JSON__);</script>
</body>
</html>
"""


MASTER_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>__TITLE__</title>
<style>
""" + _BASE_STYLE + """
</style>
</head>
<body>
<h1>__TITLE__</h1>
<div class="meta" id="summary">Loading…</div>

<h2>Chapters</h2>
<table>
<thead><tr>
  <th>Chapter</th>
  <th class="size">Files</th>
  <th class="size">Total size</th>
  <th class="mtime">Indexed</th>
  <th>Source path</th>
</tr></thead>
<tbody id="chapters"></tbody>
</table>

<h2>Search across all chapters</h2>
<div class="meta">
  Loads each chapter's data file in turn (network-cheap if everything is local).
  Query syntax: substring (case-insensitive); multiple words = AND;
  <code>*.ext</code> matches extension; <code>/regex/i</code> for regular expressions.
</div>
<div class="controls">
  <input id="q" type="text" placeholder='e.g. invoice 2024  |  *.psd  |  /IMG_\\d{4}\\.cr2/i'>
  <button id="go">Search</button>
</div>
<div id="results" style="display:none">
<div class="meta" id="status"></div>
<table>
<thead><tr>
  <th data-sort="name" class="sort-asc">Name</th>
  <th data-sort="path">Path</th>
  <th data-sort="size" class="size">Size</th>
  <th data-sort="mtime" class="mtime">Modified</th>
  <th data-sort="chapter">Chapter</th>
</tr></thead>
<tbody id="result-rows"></tbody>
</table>
</div>
<div class="footer">Generated by tools/tape_index.py</div>
<script src="tape_index_app.js"></script>
<script>tapeIndexApp.renderMaster(__MANIFEST_JSON__);</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main() or 0)
