// The correctness tab: a picker and the chosen check's own prose on the left, its results as real
// tables on the right.
//
// Descriptions come from the server, which reads each scenario file's leading comment — the page
// and the file cannot then disagree about what a check proves. Results arrive structured (columns
// and rows, not pre-formatted text) so each step can be a table with its expectation as the caption.

let catalogue = [];

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

/** A scenario step's caption: the assertion the file states. The files already phrase these as
 * "expect: …", so a duplicated prefix is stripped rather than printed as "expect: expect: …". */
const expectCaption = (block, index) =>
  `expect ${(block.expect ?? `step ${index + 1}`).replace(/^expect:\s*/i, '')}`;

/** A playground statement's caption: what it was, not what it should prove. */
export const statementCaption = (block, index) => block.expect ?? `statement ${index + 1}`;

/** One step: its caption, then what came back.
 *
 * The caption is caller-supplied because the two callers mean different things by it. A scenario
 * step carries an assertion, so "expect no rows" is the honest heading. A statement typed in the
 * playground asserts nothing — captioning it "expect …" would invent an assertion the user never
 * made and turn a plain query result into an apparent verdict. */
function renderBlock(block, index, caption) {
  const card = el('section', 'result-card');
  card.append(el('h3', 'result-card__expect', caption(block, index)));

  if (block.error) {
    const err = el('p', 'result-card__error', block.error);
    card.append(err);
    return card;
  }

  // A plan block: the operator tree the check's view compiles to. Collapsed by default — it is
  // the answer to "how does this work", not to "did it pass" — and the one worth expanding in
  // front of an audience, since the tree IS the architecture under discussion.
  if (block.plan) {
    const toggle = el('button', 'btn btn--outlined result-card__toggle', 'show graph');
    const tree = el('div', 'plan-tree');
    tree.hidden = true;
    for (const node of block.plan) {
      const row = el('div', 'plan-node');
      row.style.marginLeft = `${node.depth * 18}px`;
      row.append(el('span', 'plan-node__op', node.op));
      if (node.detail) row.append(el('span', 'plan-node__detail', node.detail));
      // Highlight the two operators this whole exercise is about.
      if (node.op === 'StreamMatchRecognize' || node.op === 'StreamWatermarkSort') {
        row.dataset.highlight = 'true';
      }
      tree.append(row);
    }
    toggle.addEventListener('click', () => {
      tree.hidden = !tree.hidden;
      toggle.textContent = tree.hidden ? 'show graph' : 'hide graph';
    });
    card.append(toggle, tree);
    return card;
  }

  // A statement with no result set: DDL, an insert, a flush. Reported so the playground accounts
  // for every line the user typed rather than skipping the ones that worked quietly.
  if (block.status) {
    card.append(el('p', 'result-card__status', block.status));
    return card;
  }

  if (block.rows.length === 0) {
    // "Nothing was emitted" is frequently the assertion itself — a held match, a rejected
    // pattern — so it is stated, not left as an empty frame.
    card.append(el('p', 'result-card__empty', 'no rows'));
    return card;
  }

  const wrap = el('div', 'result-card__scroll');
  const table = el('table', 'result-table');
  const thead = el('thead');
  const hrow = el('tr');
  for (const name of block.columns) hrow.append(el('th', null, name));
  thead.append(hrow);
  const tbody = el('tbody');
  for (const row of block.rows) {
    const tr = el('tr');
    for (const cell of row) {
      const td = el('td', cell === 'NULL' ? 'result-table__null' : null, cell);
      tr.append(td);
    }
    tbody.append(tr);
  }
  table.append(thead, tbody);
  wrap.append(table);
  card.append(wrap);
  card.append(el('p', 'result-card__count', `${block.rows.length} row${block.rows.length === 1 ? '' : 's'}`));
  return card;
}

/** Append a run's blocks to a host. Exported so the playground tab renders hand-written SQL the
 * same way a bundled scenario renders — a `select`'s rows and a view's plan look the same wherever
 * the statement came from. */
export function renderBlocks(host, blocks, caption = expectCaption) {
  for (const [i, block] of blocks.entries()) host.append(renderBlock(block, i, caption));
}

function renderResults(dom, name, result) {
  const host = dom.scenarioResults;
  host.textContent = '';

  const head = el('div', 'results__head');
  const title = el('h2', 'results__title', name.replace(/_/g, ' '));
  const verdict = el('span', 'results__verdict', result.ok ? 'passed' : 'failed');
  verdict.dataset.state = result.ok ? 'good' : 'bad';
  head.append(title, verdict);
  host.append(head);

  renderBlocks(host, result.blocks, expectCaption);
}

function renderRunning(dom, name) {
  dom.scenarioResults.textContent = '';
  const head = el('div', 'results__head');
  head.append(el('h2', 'results__title', name.replace(/_/g, ' ')));
  const verdict = el('span', 'results__verdict', 'running…');
  verdict.dataset.state = 'unknown';
  head.append(verdict);
  dom.scenarioResults.append(head);
}

/** Populate the picker, keep the description in step with it, and wire the run button. */
export function wireScenarios(dom, api, showMessage) {
  const select = dom.scenarioSelect;
  if (!select) return;

  const showDescription = () => {
    const chosen = catalogue.find((s) => s.name === select.value);
    dom.scenarioDescription.textContent = chosen?.description ?? '';
  };

  api.scenarioList().then((list) => {
    catalogue = list;
    select.textContent = '';
    for (const { name } of list) {
      const opt = el('option', null, name.replace(/_/g, ' '));
      opt.value = name;
      select.append(opt);
    }
    if (list.length === 0) {
      select.append(el('option', null, 'none embedded'));
      dom.btnScenarioRun.disabled = true;
    }
    showDescription();
  });

  select.addEventListener('change', showDescription);

  dom.btnScenarioRun?.addEventListener('click', async () => {
    const name = select.value;
    if (!name) return;
    dom.btnScenarioRun.disabled = true;
    dom.btnScenarioRun.textContent = 'running…';
    renderRunning(dom, name);
    try {
      const { ok, body } = await api.scenarioRun(name);
      if (body) {
        renderResults(dom, name, body);
        showMessage(body.ok ? 'info' : 'error', `${name}: ${body.ok ? 'passed' : 'failed'}`);
      } else {
        showMessage('error', `${name}: no result (HTTP ${ok ? 200 : 'error'})`);
      }
    } finally {
      dom.btnScenarioRun.disabled = false;
      dom.btnScenarioRun.textContent = 'run check';
    }
  });
}
