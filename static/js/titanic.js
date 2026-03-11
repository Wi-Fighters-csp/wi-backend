document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('titanic-form');
    const status = document.getElementById('titanic-status');
    const surviveValue = document.getElementById('survive-value');
    const dieValue = document.getElementById('die-value');
    const surviveBar = document.getElementById('survive-bar');
    const dieBar = document.getElementById('die-bar');
    const factorsList = document.getElementById('titanic-factors');
    const changesList = document.getElementById('titanic-changes');
    const modelAccuracy = document.getElementById('model-accuracy');
    const featureSummary = document.getElementById('feature-summary');
    const improvementSummary = document.getElementById('improvement-summary');

    if (!form) {
        return;
    }

    const formatPercent = (value) => `${(value * 100).toFixed(1)}%`;

    const setStatus = (message, isError = false) => {
        status.textContent = message;
        status.classList.toggle('text-danger', isError);
        status.classList.toggle('text-light-emphasis', !isError);
    };

    const renderMeta = async () => {
        try {
            const response = await fetch('/api/titanic/meta');
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.message || 'Unable to load model metadata.');
            }

            modelAccuracy.textContent = formatPercent(data.accuracy || 0);
            const topWeights = Object.entries(data.feature_weights || {})
                .filter(([, value]) => value > 0)
                .slice(0, 4)
                .map(([feature, value]) => `${feature}: ${formatPercent(value)}`);
            featureSummary.textContent = topWeights.join(' • ') || 'Feature weights unavailable.';
        } catch (error) {
            modelAccuracy.textContent = 'Unavailable';
            featureSummary.textContent = error.message;
        }
    };

    const renderFactors = (factors) => {
        factorsList.innerHTML = '';
        if (!factors || !factors.length) {
            factorsList.innerHTML = '<li>No factor details available.</li>';
            return;
        }

        factors.forEach((factor) => {
            const item = document.createElement('li');
            item.textContent = `${factor.label} ${factor.direction === 'helps' ? 'helped' : 'hurt'} most (${factor.impact.toFixed(2)} log-odds).`;
            factorsList.appendChild(item);
        });
    };

    const renderChanges = (improvement) => {
        changesList.innerHTML = '';
        if (!improvement || !improvement.changes || !improvement.changes.length) {
            improvementSummary.textContent = 'No better scenario was found with the app’s limited actionable adjustments.';
            changesList.innerHTML = '<li>Current passenger profile is already close to the strongest historical conditions available in this demo.</li>';
            return;
        }

        improvementSummary.textContent = `Suggested scenario raises survival odds by ${formatPercent(improvement.delta)} to ${formatPercent(improvement.survive)}.`;

        improvement.changes.forEach((entry) => {
            const item = document.createElement('li');
            const fields = entry.changes
                .map((change) => `${change.field}: ${change.from} → ${change.to}`)
                .join(', ');
            item.textContent = `${entry.reason} Change set: ${fields}.`; 
            changesList.appendChild(item);
        });
    };

    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        setStatus('Running Titanic prediction...');

        const formData = new FormData(form);
        const payload = {
            pclass: Number(formData.get('pclass')),
            sex: formData.get('sex'),
            age: Number(formData.get('age')),
            sibsp: Number(formData.get('sibsp')),
            parch: Number(formData.get('parch')),
            fare: Number(formData.get('fare')),
            embarked: formData.get('embarked'),
            alone: formData.get('alone') === 'true',
        };

        try {
            const response = await fetch('/api/titanic/predict', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
            });
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.message || 'Prediction failed.');
            }

            surviveValue.textContent = formatPercent(data.survive);
            dieValue.textContent = formatPercent(data.die);
            surviveBar.style.width = `${(data.survive * 100).toFixed(1)}%`;
            dieBar.style.width = `${(data.die * 100).toFixed(1)}%`;
            renderFactors(data.top_factors || []);
            renderChanges(data.improvement);
            setStatus(`Model accuracy on held-out data: ${formatPercent(data.accuracy || 0)}.`);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    renderMeta();
});