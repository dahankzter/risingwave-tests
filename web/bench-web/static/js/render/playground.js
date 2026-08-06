// The playground: browse what exists, write SQL, read results.
//
// Results render through the same block renderer the correctness tab uses, so a query's rows and a
// created view's streaming plan look identical wherever they came from — one way to read a result
// in this console, not two.

import { renderBlocks, statementCaption } from './scenarios.js';

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

/** Group the catalog by kind so the list reads as sections rather than a flat wall of names. */
function renderCatalog(dom, entries, onPick) {
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
      item.addEventListener('click', () => onPick(name));
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

  const refreshCatalog = () => {
    api.catalog().then((entries) =>
      renderCatalog(dom, entries, (name) => {
        // Describe on click rather than pasting the name: the question a click asks is "what is
        // this", and answering it should not overwrite whatever the user is drafting.
        run(`describe ${name};`);
      }),
    );
  };

  dom.btnSqlRun.addEventListener('click', () => run(input.value));
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

  refreshCatalog();
}
