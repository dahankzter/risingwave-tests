// Wires the top bar's buttons, the rate slider, and the clean-confirmation dialog to `api.js`.
// Every control call reports its outcome through `showMessage` (wired by app.js to the log strip)
// rather than failing silently — in particular `load/start`'s 409 ("a load is already running")
// and the clean endpoint's 400 (wrong/missing confirmation) must read as real messages, not a
// console-only failure.

/** Render a scenario transcript into its panel. */
function showScenario(dom, name, lines, ok) {
  if (!dom.scenarioPanel) return;
  dom.scenarioPanel.hidden = false;
  dom.scenarioTitle.textContent = ok == null ? name : `${name} — ${ok ? 'ran' : 'failed'}`;
  dom.scenarioTitle.dataset.state = ok == null ? 'unknown' : ok ? 'good' : 'bad';
  dom.scenarioOutput.textContent = lines.join('\n');
}

export function wireControls(dom, api, showMessage) {
  const disableWhile = (btn, fn) => async () => {
    btn.disabled = true;
    try {
      const res = await fn();
      if (res && !res.ok) {
        showMessage('warn', `${btn.textContent.trim()}: ${res.status} ${res.message}`);
      } else if (res) {
        showMessage('info', `${btn.textContent.trim()}: ok`);
      }
    } finally {
      btn.disabled = false;
    }
  };

  dom.btnClusterUp.addEventListener('click', disableWhile(dom.btnClusterUp, api.clusterUp));
  dom.btnClusterDown.addEventListener('click', disableWhile(dom.btnClusterDown, api.clusterDown));
  // Rebuild carries the selected watermark lateness: the dial that dominates reported latency, so
  // changing it is a rebuild rather than a live knob (the declaration lives in the table's DDL).
  dom.btnPipelineRebuild.addEventListener(
    'click',
    disableWhile(dom.btnPipelineRebuild, () => {
      const raw = dom.latenessSelect?.value ?? '';
      return api.pipelineRebuild(raw === '' ? null : Number(raw));
    }),
  );

  // Scenarios: the correctness half of a demo. Populated once at startup; running one prints its
  // transcript, including the scenario's own `\echo` expectations, so the panel reads as
  // "expected X, got Y" rather than a bare pass/fail.
  if (dom.scenarioSelect) {
    api.scenarioList().then((names) => {
      dom.scenarioSelect.textContent = '';
      for (const name of names) {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name.replace(/_/g, ' ');
        dom.scenarioSelect.append(opt);
      }
      if (names.length === 0) {
        const opt = document.createElement('option');
        opt.textContent = 'none embedded';
        dom.scenarioSelect.append(opt);
      }
    });
  }

  dom.btnScenarioRun?.addEventListener(
    'click',
    disableWhile(dom.btnScenarioRun, async () => {
      const name = dom.scenarioSelect?.value;
      if (!name) return { ok: false, status: 0, message: 'no scenario selected' };
      showScenario(dom, name, ['running…'], null);
      const { ok, body } = await api.scenarioRun(name);
      showScenario(dom, name, body?.output ?? ['(no output)'], body?.ok ?? ok);
      return { ok, status: ok ? 200 : 500, message: ok ? 'ok' : 'scenario failed' };
    }),
  );

  dom.btnScenarioClose?.addEventListener('click', () => {
    dom.scenarioPanel.hidden = true;
  });

  dom.btnLoadStart.addEventListener(
    'click',
    disableWhile(dom.btnLoadStart, () => api.loadStart({ rate: Number(dom.rateSlider.value) })),
  );
  dom.btnLoadStop.addEventListener('click', disableWhile(dom.btnLoadStop, api.loadStop));

  // Fixed at 3 rounds — enough to see a p50/p95 without a long wait; results stream back over
  // the socket as `probe` events (see state.js), not in this call's response.
  dom.btnProbeStart.addEventListener(
    'click',
    disableWhile(dom.btnProbeStart, () => api.probeStart(3)),
  );

  // Live: the slider posts on every change, throttled to animation frames so a drag doesn't
  // flood the endpoint with one request per pixel of mouse movement.
  let rateRequestPending = false;
  dom.rateSlider.addEventListener('input', () => {
    dom.rateValue.textContent = dom.rateSlider.value;
    if (rateRequestPending) return;
    rateRequestPending = true;
    requestAnimationFrame(async () => {
      rateRequestPending = false;
      const res = await api.loadRate(Number(dom.rateSlider.value));
      if (!res.ok) showMessage('warn', `rate: ${res.status} ${res.message}`);
    });
  });

  // Clean requires typing the word "clean" — a dialog with an OK button is not enough, because
  // the same rule the server enforces (an explicit {"confirm":"clean"} body) must be visibly true
  // of the UI action that triggers it, not just clickable past.
  dom.btnCleanOpen.addEventListener('click', () => {
    dom.cleanInput.value = '';
    dom.cleanConfirm.disabled = true;
    dom.cleanDialog.showModal();
    dom.cleanInput.focus();
  });
  dom.cleanInput.addEventListener('input', () => {
    dom.cleanConfirm.disabled = dom.cleanInput.value.trim() !== 'clean';
  });
  dom.cleanCancel.addEventListener('click', () => dom.cleanDialog.close());
  dom.cleanForm.addEventListener('submit', async (evt) => {
    evt.preventDefault();
    if (dom.cleanInput.value.trim() !== 'clean') return;
    dom.cleanDialog.close();
    const res = await api.clusterClean();
    showMessage(res.ok ? 'info' : 'warn', `clean: ${res.ok ? 'done' : `${res.status} ${res.message}`}`);
  });
}
