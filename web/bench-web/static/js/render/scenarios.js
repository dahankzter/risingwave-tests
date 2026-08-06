// The correctness tab: one card per check, each showing the prose the scenario file itself opens
// with, and a run button that prints the transcript below.
//
// Descriptions come from the server (which reads each file's leading comment) rather than being
// duplicated here — the page and the file cannot then disagree about what a check proves.

/** Render a transcript into the panel. `ok === null` means "still running". */
export function showTranscript(dom, name, lines, ok) {
  if (!dom.scenarioPanel) return;
  dom.scenarioPanel.hidden = false;
  dom.scenarioTitle.textContent =
    ok == null ? `${name} — running…` : `${name} — ${ok ? 'passed' : 'failed'}`;
  dom.scenarioTitle.dataset.state = ok == null ? 'unknown' : ok ? 'good' : 'bad';
  dom.scenarioOutput.textContent = lines.join('\n');
  dom.scenarioPanel.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

/** Build the card list once; wire each card's run button. */
export function renderScenarioList(dom, api, scenarios, showMessage) {
  const host = dom.scenarioList;
  if (!host) return;
  host.textContent = '';

  if (scenarios.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'view__lead';
    empty.textContent = 'No checks are embedded in this build.';
    host.append(empty);
    return;
  }

  for (const { name, description } of scenarios) {
    const card = document.createElement('article');
    card.className = 'check-card';

    const title = document.createElement('h3');
    title.className = 'check-card__title';
    // File names are the identifier; a reader wants the words.
    title.textContent = name.replace(/_/g, ' ');

    const body = document.createElement('p');
    body.className = 'check-card__body';
    body.textContent = description || 'No description in the scenario file.';

    const run = document.createElement('button');
    run.className = 'btn btn--tonal check-card__run';
    run.type = 'button';
    run.textContent = 'run';
    run.addEventListener('click', async () => {
      run.disabled = true;
      run.textContent = 'running…';
      showTranscript(dom, name, ['running…'], null);
      try {
        const { ok, body: result } = await api.scenarioRun(name);
        const lines = result?.output ?? ['(no output)'];
        const passed = result?.ok ?? ok;
        showTranscript(dom, name, lines, passed);
        card.dataset.state = passed ? 'good' : 'bad';
        showMessage(passed ? 'info' : 'error', `${name}: ${passed ? 'passed' : 'failed'}`);
      } finally {
        run.disabled = false;
        run.textContent = 'run';
      }
    });

    card.append(title, body, run);
    host.append(card);
  }
}
