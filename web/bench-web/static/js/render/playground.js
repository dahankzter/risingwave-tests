// The playground: browse what exists, write SQL, read results.
//
// Results render through the same block renderer the correctness tab uses, so a query's rows and a
// created view's streaming plan look identical wherever they came from — one way to read a result
// in this console, not two.

import { renderBlocks, statementCaption } from './scenarios.js';

/** Rows a "show data" peek returns. A peek, not a query — enough to see the shape and some real
 * values, few enough that it stays instant on a table under load. */
const PREVIEW_ROWS = 20;

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

/** A sink has no rows to read — `select * from <sink>` fails, and RisingWave's message for it is a
 * bare "Failed to prepare the statement" with nothing useful under it. Better to disable the button
 * and say why than to let someone spend a minute on that error. */
const SELECTABLE = new Set(['table', 'materialized view', 'source', 'view']);

/** Quote an identifier so a name RisingWave stored with capitals or punctuation still resolves.
 * The catalog reports names as stored, and unquoted identifiers fold to lower case. */
function quoteIdent(name) {
  return `"${name.replace(/"/g, '""')}"`;
}

/** Group the catalog by kind so the list reads as sections rather than a flat wall of names. */
function renderCatalog(dom, entries, onPick, selected) {
  const host = dom.catalogList;
  host.textContent = '';
  if (entries.length === 0) {
    host.append(el('p', 'playground__hint', 'Nothing yet — is the cluster up?'));
    return;
  }
  const byKind = new Map();
  for (const e of entries) {
    if (!byKind.has(e.kind)) byKind.set(e.kind, []);
    byKind.get(e.kind).push(e.name);
  }
  for (const [kind, names] of byKind) {
    host.append(el('h3', 'catalog-list__kind', `${kind}s`));
    for (const name of names) {
      const item = el('button', 'catalog-item', name);
      item.type = 'button';
      if (selected?.name === name && selected?.kind === kind) {
        item.dataset.selected = 'true';
        item.setAttribute('aria-current', 'true');
      }
      item.addEventListener('click', () => onPick({ name, kind }));
      host.append(item);
    }
  }
}

export function wirePlayground(dom, api, showMessage) {
  const input = dom.sqlInput;
  if (!input) return;

  const run = async (sql) => {
    if (!sql.trim()) return;
    dom.btnSqlRun.disabled = true;
    dom.btnSqlRun.textContent = 'running…';
    dom.sqlResults.textContent = '';
    dom.sqlResults.append(el('p', 'view__lead', 'running…'));
    try {
      const { ok, body } = await api.sqlRun(sql);
      if (!body) {
        dom.sqlResults.textContent = '';
        dom.sqlResults.append(el('p', 'result-card__error', `request failed (HTTP ${ok ? 200 : 'error'})`));
        return;
      }
      dom.sqlResults.textContent = '';
      if (body.blocks.length === 0) {
        // DDL and DML produce no result set. Saying so beats an empty pane that reads as a hang.
        dom.sqlResults.append(el('p', 'view__lead', 'ran; no result set to show'));
      } else {
        renderBlocks(dom.sqlResults, body.blocks, statementCaption);
      }
      showMessage(body.ok ? 'info' : 'error', body.ok ? 'SQL ran' : 'SQL failed — see the result');
      // Objects may have appeared or vanished; keep the browser honest without a manual refresh.
      refreshCatalog();
    } finally {
      dom.btnSqlRun.disabled = false;
      dom.btnSqlRun.textContent = 'run SQL';
    }
  };

  // What the catalog buttons act on. Held here rather than read back off the DOM so a refresh that
  // rebuilds the list does not lose it.
  let selected = null;
  let entries = [];

  /** Keep the "show data" button honest about what it would do, and about when it cannot. */
  const syncShowData = () => {
    const btn = dom.btnSqlShowData;
    if (!btn) return;
    const usable = selected != null && SELECTABLE.has(selected.kind);
    btn.disabled = !usable;
    btn.title = usable
      ? `select * from ${selected.name} limit ${PREVIEW_ROWS}`
      : selected == null
        ? 'pick an object on the left first'
        : `a ${selected.kind} has no rows to read`;
  };

  const select = (picked) => {
    selected = picked;
    renderCatalog(dom, entries, select, selected);
    syncShowData();
    // Describe on pick rather than pasting the name: the question a click asks is "what is this",
    // and answering it should not overwrite whatever the user is drafting.
    run(`describe ${quoteIdent(picked.name)};`);
  };

  const refreshCatalog = () => {
    api.catalog().then((list) => {
      entries = list;
      // An object that vanished cannot stay selected — the button would offer to read a table that
      // is no longer there.
      if (selected && !list.some((e) => e.name === selected.name && e.kind === selected.kind)) {
        selected = null;
      }
      renderCatalog(dom, entries, select, selected);
      syncShowData();
    });
  };

  dom.btnSqlRun.addEventListener('click', () => run(input.value));
  dom.btnSqlShowData?.addEventListener('click', () => {
    if (selected == null || !SELECTABLE.has(selected.kind)) return;
    run(`select * from ${quoteIdent(selected.name)} limit ${PREVIEW_ROWS};`);
  });
  dom.btnSqlClear?.addEventListener('click', () => {
    input.value = '';
    input.focus();
  });
  dom.btnCatalogRefresh?.addEventListener('click', refreshCatalog);

  // ⌘/Ctrl+Enter runs, because a SQL box without it is a SQL box people complain about.
  input.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      run(input.value);
    }
  });

  for (const btn of document.querySelectorAll('.playground__shortcuts [data-sql]')) {
    btn.addEventListener('click', () => run(btn.dataset.sql));
  }

  syncShowData();
  refreshCatalog();
  // Handed back so switching to this tab re-reads the catalog. Objects appear and vanish behind the
  // tab's back — a correctness check creates its own tables and drops them again, and a check that
  // FAILS leaves them behind (the run stops at the failing statement, so its trailing drops never
  // execute). A list last read on page load would show neither, and would name tables that no
  // longer exist.
  return { refresh: refreshCatalog };
}
