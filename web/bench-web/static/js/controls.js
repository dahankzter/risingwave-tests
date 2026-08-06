// Wires the top bar's buttons, the rate slider, and the clean-confirmation dialog to `api.js`.
// Every control call reports its outcome through `showMessage` (wired by app.js to the log strip)
// rather than failing silently — in particular `load/start`'s 409 ("a load is already running")
// and the clean endpoint's 400 (wrong/missing confirmation) must read as real messages, not a
// console-only failure.

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
  dom.btnPipelineRebuild.addEventListener(
    'click',
    disableWhile(dom.btnPipelineRebuild, api.pipelineRebuild),
  );

  dom.btnLoadStart.addEventListener(
    'click',
    disableWhile(dom.btnLoadStart, () => api.loadStart({ rate: Number(dom.rateSlider.value) })),
  );
  dom.btnLoadStop.addEventListener('click', disableWhile(dom.btnLoadStop, api.loadStop));

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
