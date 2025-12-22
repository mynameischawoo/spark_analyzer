document.addEventListener('DOMContentLoaded', () => {
    console.log("APP JS LOADED v100 - WITH CHECKBOXES");
    console.log("APP JS LOADED v999 - WITH CHECKBOXES");
    const listTableBody = document.querySelector('#log-table tbody');
    const resultTableHead = document.querySelector('#result-table thead');
    const resultTableBody = document.querySelector('#result-table tbody');

    // Buttons
    const btnStart = document.getElementById('btn-start-analysis');
    const btnStartDetail = document.getElementById('btn-start-detail-analysis');
    const btnViewRecent = document.getElementById('btn-view-recent');
    const btnBack = document.getElementById('btn-back');
    const btnBackFromDetail = document.getElementById('btn-back-from-detail');
    const btnDownloadCsv = document.getElementById('btn-download-csv');
    const btnCreateGraph = document.getElementById('btn-create-graph');

    // ...

    const panelList = document.getElementById('log-selection-view');
    const panelResult = document.getElementById('analysis-result-view');
    const summaryChartContainer = document.getElementById('summary-chart-container');
    const statsCardsContainer = document.getElementById('stats-cards-container');
    const panelDetail = document.getElementById('detail-result-view');
    const loadingOverlay = document.getElementById('loading-overlay');

    // Empty State Elements
    const emptyStateContainer = document.getElementById('empty-state-container');
    const logListPanel = document.getElementById('log-list-panel');

    // ...

    // Event Listeners
    btnStart.addEventListener('click', startAnalysis);
    btnStartDetail.addEventListener('click', startDetailAnalysis);
    btnViewRecent.addEventListener('click', fetchRecentResults);
    btnBack.addEventListener('click', showList);
    btnBack.addEventListener('click', showList);
    btnBackFromDetail.addEventListener('click', showList);
    btnCreateGraph.addEventListener('click', handleCreateGraph);
    // ...

    async function startDetailAnalysis() {
        const selected = Array.from(document.querySelectorAll('.file-checkbox:checked')).map(cb => cb.value);
        if (selected.length !== 1) {
            alert('상세 분석은 한 번에 하나의 애플리케이션(파일)만 가능합니다.');
            return;
        }
        // Ideally should check if they belong to same app, but for now simple check
        // Check if multiple *apps* selected? Hard to know without app grouping logic on frontend
        // Just let backend handle or warn user

        showLoading(true, "Loading Stage Details...");
        try {
            const res = await fetch('/api/analyze/detail', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ files: selected })
            });

            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.detail || 'Analysis failed');
            }

            const data = await res.json();
            renderDetailTable(data);
            showDetail();

        } catch (err) {
            alert('상세 분석 중 오류 발생: ' + err.message);
        } finally {
            showLoading(false);
        }
    }

    let executorChart = null;
    let summaryChart = null;

    function renderExecutorChart(timeSeries) {
        const canvas = document.getElementById('executorChart');
        if (!canvas) return;

        // Robustly check for existing chart on this canvas
        const existingChart = Chart.getChart(canvas);
        if (existingChart) {
            existingChart.destroy();
        }

        // Also clear local var if it exists (though getChart handles it)
        if (executorChart) {
            // executorChart.destroy(); // Already handled above usually
            executorChart = null;
        }

        const ctx = canvas.getContext('2d');

        if (!timeSeries || timeSeries.length === 0) {
            return;
        }

        // Sort by time just in case
        timeSeries.sort((a, b) => a.time - b.time);

        const startTime = timeSeries[0].time;

        const dataPoints = timeSeries.map(pt => ({
            x: (pt.time - startTime) / 1000.0, // Seconds relative to start
            y: pt.count
        }));

        // Add a final point for duration if needed, or just Step line
        // Stepped line is better for count changes

        executorChart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: 'Active Executors',
                    data: dataPoints,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    stepped: true, // Step chart logic
                    fill: true,
                    tension: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        type: 'linear',
                        title: {
                            display: true,
                            text: 'Time (Seconds)'
                        },
                        ticks: {
                            callback: function (value) { return value + 's'; }
                        }
                    },
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Executor Count'
                        },
                        ticks: {
                            stepSize: 1
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index',
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return `Executors: ${context.parsed.y}`;
                            }
                        }
                    }
                }
            }
        });
    }

    function renderDetailTable(data) {
        console.log("Detail Analysis Data:", data);

        // 1. Render Executor Chart
        if (data.executorTimeSeries) {
            renderExecutorChart(data.executorTimeSeries);
        }

        // 2. Render Read/Write Flow
        const sqlContainer = document.getElementById('sql-plan-container');
        const sqlContent = document.getElementById('sql-plan-content');

        sqlContainer.classList.remove('hidden');
        sqlContent.innerHTML = ''; // Clear previous

        // Container for Flow
        const flowContainer = document.createElement('div');
        flowContainer.className = 'read-write-flow-container';
        flowContainer.style.display = 'flex';
        flowContainer.style.alignItems = 'flex-start'; // Align top
        flowContainer.style.justifyContent = 'space-between';
        flowContainer.style.padding = '20px';
        flowContainer.style.gap = '50px';

        // Left: Reads
        const readCol = document.createElement('div');
        readCol.className = 'flow-column read-column';
        readCol.style.flex = '1';
        readCol.innerHTML = '<h3>Read</h3>';

        if (data.reads && data.reads.length > 0) {
            data.reads.forEach(read => {
                const card = document.createElement('div');
                card.className = 'flow-card';
                card.style.border = '1px solid #28a745';
                card.style.borderRadius = '8px';
                card.style.padding = '15px';
                card.style.marginBottom = '15px';
                card.style.backgroundColor = '#f0fff4';

                let html = `<strong>Use: ${read['Format'] || 'Unknown'}</strong><br>`;
                html += `<div style="font-size:0.9em; color:#555; word-break:break-all;">${read['File Path']}</div>`;
                html += `<hr style="margin:8px 0; border:0; border-top:1px dashed #ccc;">`;
                html += `<div>Rows: ${read['Rows'] ? read['Rows'].toLocaleString() : '0'}</div>`;
                html += `<div>Bytes: ${formatBytes(read['Bytes Read'])}</div>`;
                if (read['Partition Filters'] && read['Partition Filters'] !== 'None') {
                    html += `<div style="font-size:0.8em; color:#d63384; margin-top:5px;">Filter: ${read['Partition Filters']}</div>`;
                }

                card.innerHTML = html;
                readCol.appendChild(card);
            });
        } else {
            readCol.innerHTML += '<div class="no-data">No Reads Detected</div>';
        }

        // Center: Arrow
        const arrowCol = document.createElement('div');
        arrowCol.className = 'flow-arrow';
        arrowCol.style.display = 'flex';
        arrowCol.style.alignItems = 'center';
        arrowCol.style.justifyContent = 'center';
        arrowCol.style.alignSelf = 'stretch'; // Match height
        arrowCol.innerHTML = '<div style="font-size: 50px; color: #555; margin-top: 50px;">&rarr;</div>';

        // Right: Writes
        const writeCol = document.createElement('div');
        writeCol.className = 'flow-column write-column';
        writeCol.style.flex = '1';
        writeCol.innerHTML = '<h3>Write</h3>';

        if (data.writes && data.writes.length > 0) {
            data.writes.forEach(write => {
                const card = document.createElement('div');
                card.className = 'flow-card';
                card.style.border = '1px solid #007bff';
                card.style.borderRadius = '8px';
                card.style.padding = '15px';
                card.style.marginBottom = '15px';
                card.style.backgroundColor = '#f0f8ff';

                let html = `<strong>Format: ${write['Format'] || 'Unknown'}</strong>`;
                if (write['Mode']) html += ` <span style="font-size:0.8em; background:#eee; padding:2px 4px; borderRadius:4px;">${write['Mode']}</span>`;
                html += `<div style="font-size:0.9em; color:#555; word-break:break-all;">${write['File Path']}</div>`;
                html += `<hr style="margin:8px 0; border:0; border-top:1px dashed #ccc;">`;
                html += `<div>Rows: ${write['Rows'] ? write['Rows'].toLocaleString() : '0'}</div>`;
                html += `<div>Bytes: ${formatBytes(write['Bytes Written'])}</div>`;
                html += `<div>Files: ${write['Files Written'] || 0}</div>`;
                html += `<div>Avg Size: ${formatBytes(write['Average File Size'])}</div>`;

                card.innerHTML = html;
                writeCol.appendChild(card);
            });
        } else {
            writeCol.innerHTML += '<div class="no-data">No Writes Detected</div>';
        }

        flowContainer.appendChild(readCol);
        flowContainer.appendChild(arrowCol);
        flowContainer.appendChild(writeCol);

        sqlContent.appendChild(flowContainer);

        // Render Side Header Info
        const appName = data.appInfo ? data.appInfo.name : (data.appName || "Unknown");
        const appId = data.appInfo ? data.appInfo.id : (data.appId || "Unknown");
        const infoDiv = document.getElementById('app-detail-info');
        infoDiv.innerHTML = `<strong>App Name:</strong> ${appName} | <strong>App ID:</strong> ${appId}`;

        // Render Stage Table
        const tbody = document.querySelector('#detail-table tbody');
        tbody.innerHTML = '';

        if (data.stages) {
            data.stages.forEach(stage => {
                const tr = document.createElement('tr');

                // Format formatBytes helpers
                const input = formatBytes(stage["Input"], 'GB');
                const output = formatBytes(stage["Output"], 'GB');
                const sr = formatBytes(stage["Shuffle Read"], 'GB');
                const sw = formatBytes(stage["Shuffle Write"], 'GB');
                const sm = formatBytes(stage["Spill Memory"], 'GB');

                const avgInput = formatBytes(stage["Avg Input"], 'MB') + ' MB';

                // Description (truncate if too long?)
                let desc = stage["Description"] || stage["Stage Name"] || "";
                if (desc.length > 50) desc = desc.substring(0, 50) + "...";

                tr.innerHTML = `
                        <td>${stage["Stage ID"]}</td>
                        <td style="text-align:left;" title="${stage["Stage Name"]}">${stage["Stage Name"]}</td>
                        <td style="text-align:left;" title="${stage["Description"]}">${desc}</td>
                        <td>${stage["Status"]}</td>
                        <td>${stage["Duration"] ? stage["Duration"].toFixed(1) : '0'}</td>
                        <td>${stage["Tasks"]}</td>
                        <td>${avgInput}</td>
                        <td>${input} GB</td>
                        <td>${output} GB</td>
                        <td>${sr} GB</td>
                        <td>${sw} GB</td>
                        <td>${sm} GB</td>
                    `;
                tbody.appendChild(tr);
            });
        }
    }

    function showDetail() {
        panelList.classList.add('hidden');
        panelResult.classList.add('hidden');
        panelDetail.classList.remove('hidden');
        document.querySelector('main').classList.add('wide-layout');
    }

    function showResult() {
        panelList.classList.add('hidden');
        panelDetail.classList.add('hidden');
        panelResult.classList.remove('hidden');
        document.querySelector('main').classList.add('wide-layout');
    }

    function showList() {
        panelList.classList.remove('hidden');
        panelResult.classList.add('hidden');
        panelDetail.classList.add('hidden');
        document.querySelector('main').classList.remove('wide-layout');
    }
    const selectAllCb = document.getElementById('select-all');
    const logoBtn = document.getElementById('logo-btn');
    const unitBtns = document.querySelectorAll('.unit-btn');
    const sortHeaders = document.querySelectorAll('.sortable');

    // New Buttons
    // const btnUpload = document.getElementById('btn-upload-log'); // Changed to class
    const btnUploadTriggers = document.querySelectorAll('.btn-trigger-upload');
    const btnDelete = document.getElementById('btn-delete-log');
    const uploadInput = document.getElementById('log-upload-input');

    // State
    let logFiles = [];
    let currentResults = []; // Store raw data
    let currentUnit = 'GB';   // Default to GB
    let metricDefinitions = {}; // Store tooltips
    let shsUrl = "";

    // Sort State
    let sortState = {
        column: 'date',
        direction: 'desc'
    };

    // Columns identified as Bytes to be converted
    const BYTE_COLUMNS = [
        "Total Input", "Total Output",
        "Driver Memory", "Executor Memory", "Executor Overhead Memory", "Total Memory Capacity",
        "Peak Heap Usage", "Total Shuffle Read", "Total Shuffle Write",
        "Max Shuffle Read (Stage)", "Max Shuffle Write (Stage)",
        "Max Shuffle Read (Job)", "Max Shuffle Write (Job)",
        "Total Spill (Memory)", "Total Spill (Disk)",
        "Max Spill Memory (Stage)"
    ];

    // Init
    fetchConfig();
    const definitionsPromise = fetchDefinitions(); // Store promise
    fetchLogs();

    async function fetchConfig() {
        try {
            const res = await fetch('/api/config');
            if (res.ok) {
                const data = await res.json();
                shsUrl = data.shs_url;
                // Re-render if we have data (fixes race condition)
                if (currentResults.length > 0) {
                    renderResults(currentResults);
                }
            }
        } catch (e) {
            console.error("Failed to load config", e);
        }
    }

    async function fetchRecentResults() {
        try {
            // Ensure definitions are loaded first
            await definitionsPromise;

            const res = await fetch('/api/results/recent');
            if (res.ok) {
                const data = await res.json();
                currentResults = data; // Store the raw data
                renderResults(currentResults);
                showResult();
            } else {
                alert('최근 분석 결과를 불러오지 못했습니다.');
            }
        } catch (e) {
            console.error("Failed to load recent results", e);
            alert('최근 분석 결과를 불러오는 중 오류가 발생했습니다.');
        }
    }

    async function fetchDefinitions() {
        try {
            const res = await fetch('/api/definitions');
            if (res.ok) {
                metricDefinitions = await res.json();
                // FIX: Re-render if we already have results to ensure tooltips appear
                if (currentResults.length > 0) {
                    renderResults(currentResults);
                }
            }
        } catch (e) {
            console.error("Failed to load definitions", e);
        }
    }

    // Event Listeners
    btnStart.addEventListener('click', startAnalysis);
    btnViewRecent.addEventListener('click', fetchRecentResults);
    btnBack.addEventListener('click', showList);
    btnDownloadCsv.addEventListener('click', () => {
        downloadCsvWithCurrentUnit();
    });

    function downloadCsvWithCurrentUnit() {
        if (!currentResults || currentResults.length === 0) {
            alert('다운로드할 데이터가 없습니다.');
            return;
        }

        // 1. Determine columns (respecting current column order if possible, or default keys)
        // If columnOrder is empty, init it
        let cols = columnOrder.length > 0 ? columnOrder : Object.keys(currentResults[0]);

        // 2. Build Header Row
        // For header text, we want to append unit like "Driver Memory (MB)" if it's a byte column
        const headerRow = cols.map(key => {
            if (BYTE_COLUMNS.includes(key) && currentUnit !== 'B') {
                return `"${key} (${currentUnit})"`;
            }
            return `"${key}"`;
        });

        const csvRows = [headerRow.join(',')];

        // 3. Build Data Rows
        currentResults.forEach(row => {
            const values = cols.map(key => {
                let val = row[key];

                if (BYTE_COLUMNS.includes(key)) {
                    // Convert unit
                    val = formatBytes(val, currentUnit);
                    // Note: formatBytes returns number or string. 
                    // If number, it might need formatting?
                    // formatBytes inside already returns number or string "0".
                    // Let's just use it as is.
                }

                // Handle null/undefined
                if (val === null || val === undefined) {
                    val = "";
                }

                // Escape quotes
                const stringVal = String(val).replace(/"/g, '""');
                return `"${stringVal}"`;
            });
            csvRows.push(values.join(','));
        });

        // 4. Create Blob and Download
        const csvString = csvRows.join('\n');
        // Add BOM for Excel compatibility with UTF-8
        const bom = '\uFEFF';
        const blob = new Blob([bom + csvString], { type: 'text/csv;charset=utf-8;' });

        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

        link.setAttribute('href', url);
        link.setAttribute('download', `spark_analysis_result_${currentUnit}_${timestamp}.csv`);
        link.style.visibility = 'hidden';

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    // logoBtn.addEventListener('click', showList); // Removed: handled by <a> tag in HTML

    // Upload & Delete Listeners
    // btnUpload.addEventListener('click', () => uploadInput.click());
    btnUploadTriggers.forEach(btn => {
        btn.addEventListener('click', () => uploadInput.click());
    });
    uploadInput.addEventListener('change', handleUpload);
    btnDelete.addEventListener('click', handleDelete);

    selectAllCb.addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.querySelectorAll('.file-checkbox').forEach(cb => cb.checked = checked);
    });

    // Sort Event Listeners
    sortHeaders.forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.sort;
            if (sortState.column === col) {
                // Toggle direction
                sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
            } else {
                // New column, default to asc for text, desc for date? let's stick to asc default
                sortState.direction = 'asc';
                sortState.column = col;
            }
            updateSortIcons();
            renderLogList(logFiles);
        });
    });

    // Re-render chart if unit changes
    unitBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            unitBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentUnit = btn.dataset.unit;

            // Re-render table
            renderResults(currentResults);

            // Re-render chart if visible
            if (summaryChart && !summaryChartContainer.classList.contains('hidden')) {
                handleCreateGraph();
            }
        });
    });

    async function fetchLogs() {
        try {
            const res = await fetch('/api/logs');
            logFiles = await res.json();

            // 1. Load cache
            const nameCache = JSON.parse(localStorage.getItem('spark_app_names_cache') || '{}');

            // 2. Identify missing names
            const missingFiles = [];
            logFiles.forEach(f => {
                // If cached and valid (not Unknown), use it
                if (nameCache[f.filename] && nameCache[f.filename] !== "Unknown") {
                    f.appName = nameCache[f.filename];
                } else {
                    missingFiles.push(f.filename);
                }
            });

            updateSortIcons(); // Init icons based on default state
            // 3. Render what we have first (optional, but let's render names as loading)
            renderLogList(logFiles);

            // 4. Fetch missing if any
            if (missingFiles.length > 0) {
                // Optimization: fetch in chunks if needed, but for now all at once 
                const nameRes = await fetch('/api/extract-names', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ files: missingFiles })
                });

                if (nameRes.ok) {
                    const newNames = await nameRes.json();

                    // Update cache
                    const updatedCache = { ...nameCache, ...newNames };
                    localStorage.setItem('spark_app_names_cache', JSON.stringify(updatedCache));

                    // Update current list object
                    logFiles.forEach(f => {
                        if (newNames[f.filename]) {
                            f.appName = newNames[f.filename];
                        }
                    });

                    // Re-render
                    renderLogList(logFiles);
                }
            }

        } catch (err) {
            console.error(err);
            alert('로그 목록을 불러오지 못했습니다.');
        }
    }

    function renderLogList(files) {
        const grid = document.getElementById('log-list-grid');
        grid.innerHTML = '';

        // Sorting Logic
        if (files) {
            files.sort((a, b) => {
                let valA = a[sortState.column] || '';
                let valB = b[sortState.column] || '';

                if (sortState.column === 'appName') {
                    if (!valA) valA = '';
                    if (!valB) valB = '';
                }

                if (valA < valB) return sortState.direction === 'asc' ? -1 : 1;
                if (valA > valB) return sortState.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // Empty State Logic
        if (!files || files.length === 0) {
            emptyStateContainer.classList.remove('hidden');
            logListPanel.classList.add('hidden');
            return;
        } else {
            emptyStateContainer.classList.add('hidden');
            logListPanel.classList.remove('hidden');
        }

        files.forEach(file => {
            const card = document.createElement('div');
            card.className = 'card log-item-card';

            const appName = file.appName || 'Unknown App';
            const dateStr = file.date || 'Unknown Date';

            // Generate a random gradient or color based on app name hash for visual variety? 
            // For now, stick to the design.

            card.innerHTML = `
                <input type="checkbox" class="file-checkbox log-item-checkbox" value="${file.filename}">
                <div class="card-header">
                    <span class="card-tag">Application Log</span>
                    <h3 class="card-title" style="font-size:1.2rem;">${appName}</h3>
                </div>
                <div class="card-desc">
                    <div style="font-family:monospace; font-size:0.85rem; color:#8b949e; margin-bottom:8px;">${file.filename}</div>
                    <div style="font-size:0.9rem;">${dateStr}</div>
                </div>
                <div class="card-meta">
                     <span style="color:#58a6ff;">Ready to analyze</span>
                </div>
            `;

            // Make the whole card clickable to toggle checkbox (optional, but nice)
            card.addEventListener('click', (e) => {
                if (e.target.type !== 'checkbox') {
                    const cb = card.querySelector('.file-checkbox');
                    cb.checked = !cb.checked;
                }
            });

            grid.appendChild(card);
        });
    }

    function updateSortIcons() {
        sortHeaders.forEach(th => {
            th.classList.remove('asc', 'desc');
            if (th.dataset.sort === sortState.column) {
                th.classList.add(sortState.direction);
            }
        });
    }

    async function startAnalysis() {
        const selected = Array.from(document.querySelectorAll('.file-checkbox:checked')).map(cb => cb.value);
        if (selected.length === 0) {
            alert('분석할 애플리케이션 로그를 선택하세요.');
            return;
        }

        showLoading(true, "Analyzing Log Applications...");
        try {
            const res = await fetch('/api/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ files: selected })
            });

            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.detail || 'Analysis failed');
            }

            // Analysis success, wait for definitions then fetch results
            await definitionsPromise;
            await fetchRecentResults();

        } catch (err) {
            alert('분석 중 오류가 발생했습니다: ' + err.message);
            showLoading(false);
        }
    }

    async function handleUpload(e) {
        const files = e.target.files;
        if (!files || files.length === 0) return;

        const formData = new FormData();
        Array.from(files).forEach(file => {
            formData.append('files', file);
        });

        showLoading(true, "Uploading Files...");
        try {
            const res = await fetch('/api/upload', {
                method: 'POST',
                body: formData
            });

            if (res.ok) {
                const result = await res.json();
                alert(`${result.saved}개의 파일이 업로드되었습니다.`);
                fetchLogs(); // Refresh list
            } else {
                throw new Error('Upload failed');
            }
        } catch (err) {
            alert('업로드 중 오류 발생: ' + err.message);
        } finally {
            showLoading(false);
            uploadInput.value = ''; // Reset input
        }
    }

    async function handleDelete(e) {
        if (e) e.preventDefault();
        const selected = Array.from(document.querySelectorAll('.file-checkbox:checked')).map(cb => cb.value);
        if (selected.length === 0) {
            alert('삭제할 로그를 선택하세요.');
            return;
        }

        if (!confirm(`선택한 ${selected.length}개의 파일을 삭제하시겠습니까?`)) return;

        showLoading(true, "Deleting Selected Logs...");
        try {
            const res = await fetch('/api/logs', {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ files: selected })
            });

            if (res.ok) {
                const result = await res.json();
                // Clear deleted from cache
                const nameCache = JSON.parse(localStorage.getItem('spark_app_names_cache') || '{}');
                selected.forEach(fname => delete nameCache[fname]);
                localStorage.setItem('spark_app_names_cache', JSON.stringify(nameCache));

                alert(`${result.deleted}개의 파일이 삭제되었습니다.`);
                fetchLogs(); // Refresh list
            } else {
                const err = await res.json();
                throw new Error(err.detail || 'Delete failed');
            }
        } catch (err) {
            alert('삭제 중 오류 발생: ' + err.message);
        } finally {
            showLoading(false);
        }
    }

    async function fetchRecentResults() {
        showLoading(true, "Fetching Results...");
        try {
            const res = await fetch('/api/results/recent');
            if (res.status === 404) {
                alert('최근 분석된 메트릭 결과가 없습니다.');
                showLoading(false);
                return;
            }
            if (!res.ok) throw new Error('Failed to fetch results');

            const data = await res.json();
            currentResults = data; // Store raw data
            renderResults(data);
            showResult();
        } catch (err) {
            alert('결과를 불러오는 중 오류가 발생했습니다: ' + err.message);
        } finally {
            showLoading(false);
        }
    }

    function formatBytes(bytes, unit) {
        if (bytes === 0 || bytes === "" || bytes === null || isNaN(bytes)) return bytes;

        let num = Number(bytes);
        if (isNaN(num)) return bytes;

        const units = { 'B': 1, 'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4 };
        const factor = units[unit] || 1;

        const val = num / factor;

        // Show 6 decimal places as requested
        if (num === 0) return "0";

        return parseFloat(val.toFixed(6));
    }

    // Result Sort and Order State
    let resultSortState = {
        column: null,
        direction: 'desc'
    };
    let columnOrder = [];

    function renderResults(data) {
        resultTableHead.innerHTML = '';
        resultTableBody.innerHTML = '';

        if (!data || data.length === 0) return;

        // Initialize column order if needed
        if (columnOrder.length === 0) {
            columnOrder = Object.keys(data[0]);
        }

        // Apply Sort if active (on the data itself)
        let displayData = [...data];
        if (resultSortState.column) {
            displayData.sort((a, b) => {
                let valA = a[resultSortState.column];
                let valB = b[resultSortState.column];

                if (valA === undefined || valA === null) valA = -Infinity;
                if (valB === undefined || valB === null) valB = -Infinity;

                if (typeof valA === 'number' && typeof valB === 'number') {
                    return resultSortState.direction === 'asc' ? valA - valB : valB - valA;
                }

                valA = String(valA).toLowerCase();
                valB = String(valB).toLowerCase();

                if (valA < valB) return resultSortState.direction === 'asc' ? -1 : 1;
                if (valA > valB) return resultSortState.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // Create Headers
        const trHead = document.createElement('tr');

        // Add Checkbox Column Header
        const thCheck = document.createElement('th');
        const checkAll = document.createElement('input');
        checkAll.type = 'checkbox';
        checkAll.addEventListener('change', (e) => {
            const checks = resultTableBody.querySelectorAll('.row-checkbox');
            checks.forEach(c => c.checked = e.target.checked);
        });
        thCheck.appendChild(checkAll);
        trHead.appendChild(thCheck);

        columnOrder.forEach((key, index) => {
            const th = document.createElement('th');
            let text = key;
            if (BYTE_COLUMNS.includes(key) && currentUnit !== 'B') {
                text += ` (${currentUnit})`;
            }

            th.draggable = true; // Enable Drag
            th.classList.add('sortable');
            th.dataset.column = key; // For identifying column

            // Tooltip Check
            let innerContent = text;
            // Try strict match, then trimmed match
            // Remove any potential invisible characters from key for lookup
            const cleanKey = key.trim();
            let def = metricDefinitions[cleanKey] || metricDefinitions[key];

            // Debug check for specific keys if missing
            if (!def && (cleanKey === 'Application ID' || cleanKey === 'Start Date')) {
                console.warn(`Missing definition for key: "${key}" (clean: "${cleanKey}")`);
                console.log('Available keys:', Object.keys(metricDefinitions));
            }

            if (def) {
                innerContent = `<span class="tooltip-trigger" data-tooltip="${def}">${text}</span>`;
            }

            th.innerHTML = `${innerContent} <span class="sort-icon"></span>`;

            // Sort Interaction
            th.addEventListener('click', (e) => {
                // Prevent sort when actually dragging (simple check)
                if (th.classList.contains('dragging')) return;

                if (resultSortState.column === key) {
                    resultSortState.direction = resultSortState.direction === 'asc' ? 'desc' : 'asc';
                } else {
                    resultSortState.column = key;
                    resultSortState.direction = 'desc';
                }
                renderResults(data);
            });

            // Drag and Drop Events
            th.addEventListener('dragstart', handleDragStart);
            th.addEventListener('dragover', handleDragOver);
            th.addEventListener('dragleave', handleDragLeave);
            th.addEventListener('drop', (e) => handleDrop(e, key, data));

            if (resultSortState.column === key) {
                th.classList.add(resultSortState.direction);
            }

            trHead.appendChild(th);
        });
        resultTableHead.appendChild(trHead);

        // Create Rows based on columnOrder
        displayData.forEach((row, index) => {
            const tr = document.createElement('tr');

            // Add Checkbox Column Cell
            const tdCheck = document.createElement('td');
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'row-checkbox';
            checkbox.value = index; // Store index to retrieve data
            tdCheck.appendChild(checkbox);
            tr.appendChild(tdCheck);

            columnOrder.forEach(key => {
                const td = document.createElement('td');
                let val = row[key];

                if (BYTE_COLUMNS.includes(key)) {
                    val = formatBytes(val, currentUnit);
                    if (currentUnit === 'B' && typeof val === 'number') {
                        val = val.toLocaleString();
                    }
                } else if (typeof val === 'number') {
                    val = val.toLocaleString();
                }

                if (key === "Application ID" && shsUrl && val) {
                    const link = document.createElement('a');
                    const baseUrl = shsUrl.endsWith('/') ? shsUrl.slice(0, -1) : shsUrl;
                    link.href = `${baseUrl}/history/${val}/`;
                    link.target = "_blank";
                    link.textContent = val;
                    link.style.color = "#007bff";
                    link.style.textDecoration = "underline";
                    td.appendChild(link);
                } else {
                    td.textContent = val;
                }
                tr.appendChild(td);
            });
            resultTableBody.appendChild(tr);
        });

        // Attach tooltip event listeners after rendering
        attachTooltipListeners();
    }



    function handleCreateGraph() {
        console.log("Create Graph Button Clicked");
        // 1. Get Selected Data
        const checks = Array.from(resultTableBody.querySelectorAll('.row-checkbox:checked'));
        if (checks.length === 0) {
            alert('그래프를 그릴 애플리케이션을 선택해주세요.');
            return;
        }

        const selectedIndices = checks.map(c => parseInt(c.value));

        let displayData = [...currentResults];
        // Apply current sort state to match table order/indices
        if (resultSortState.column) {
            displayData.sort((a, b) => {
                let valA = a[resultSortState.column];
                let valB = b[resultSortState.column];
                if (valA === undefined || valA === null) valA = -Infinity;
                if (valB === undefined || valB === null) valB = -Infinity;
                if (typeof valA === 'number' && typeof valB === 'number') {
                    return resultSortState.direction === 'asc' ? valA - valB : valB - valA;
                }
                valA = String(valA).toLowerCase();
                valB = String(valB).toLowerCase();
                if (valA < valB) return resultSortState.direction === 'asc' ? -1 : 1;
                if (valA > valB) return resultSortState.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

        const selectedData = selectedIndices.map(i => displayData[i]);

        // 2. Prepare Data for Chart
        // Sort by Start Date
        selectedData.sort((a, b) => {
            const dateA = new Date(a['Start Date'] || 0);
            const dateB = new Date(b['Start Date'] || 0);
            return dateA - dateB;
        });

        // X-Axis Labels (yyyy-MM-dd)
        const labels = selectedData.map(d => {
            if (d['Start Date']) {
                return d['Start Date'].split(' ')[0]; // Take yyyy-MM-dd
            }
            return d['Application ID'] || 'Unknown';
        });

        const nonNumeric = ['Start Date', 'Application ID', 'Application Name', 'Max Shuffle Read Stage ID', 'Max Shuffle Write Stage ID', 'Max Spill Stage ID', 'Max Shuffle Read Job ID', 'Max Shuffle Write Job ID'];
        const metricKeys = Object.keys(selectedData[0]).filter(k => !nonNumeric.includes(k) && typeof selectedData[0][k] === 'number');

        const datasets = metricKeys.map((key, idx) => {
            // Colors
            const hue = (idx * 137.508) % 360; // golden angle approx
            const color = `hsl(${hue}, 70%, 50%)`;

            const isDuration = key.toLowerCase().includes('duration') || key.includes('(Sec)');
            const isBytes = BYTE_COLUMNS.includes(key);

            const dataPoints = selectedData.map(d => {
                let val = d[key];
                if (val === null || val === undefined) return null;

                if (isBytes) {
                    // Return number for chart
                    return formatBytes(val, currentUnit);
                }
                return val;
            });

            let label = key;
            if (isBytes && currentUnit !== 'B') {
                label += ` (${currentUnit})`;
            }

            return {
                label: label,
                data: dataPoints,
                borderColor: color,
                backgroundColor: color,
                fill: false,
                tension: 0.1,
                hidden: !isDuration
            };
        });

        // 3. Render Chart
        if (summaryChart) {
            summaryChart.destroy();
        }

        summaryChartContainer.classList.remove('hidden');

        const ctx = document.getElementById('summaryChart').getContext('2d');
        summaryChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Start Date'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Value'
                        },
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            boxWidth: 8
                        },
                        onClick: (e, legendItem, legend) => {
                            Chart.defaults.plugins.legend.onClick.call(this, e, legendItem, legend);
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += context.parsed.y.toLocaleString();
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        });

        // 4. Render Statistics Cards
        renderStatsCards(selectedData);

        summaryChartContainer.scrollIntoView({ behavior: 'smooth' });
    }

    // Statistics Cards Rendering
    function renderStatsCards(data) {
        statsCardsContainer.innerHTML = '';
        statsCardsContainer.classList.remove('hidden');

        if (!data || data.length === 0) {
            statsCardsContainer.classList.add('hidden');
            return;
        }

        // Metrics to display with their config
        const metricsConfig = [
            { key: 'Duration (Sec)', label: 'Duration', unit: 'sec', icon: '⏱️', category: 'duration' },
            { key: 'Max Shuffle Read (Stage)', label: 'Max Shuffle Read', unit: currentUnit, isByte: true, icon: '📥', category: 'shuffle' },
            { key: 'Max Shuffle Write (Stage)', label: 'Max Shuffle Write', unit: currentUnit, isByte: true, icon: '📤', category: 'shuffle' },
            { key: 'Total Input', label: 'Total Input', unit: currentUnit, isByte: true, icon: '📂', category: 'io' },
            { key: 'Total Output', label: 'Total Output', unit: currentUnit, isByte: true, icon: '💾', category: 'io' },
            { key: 'Total Memory Capacity', label: 'Total Memory', unit: currentUnit, isByte: true, icon: '🧠', category: 'memory' },
            { key: 'Avg Idle Cores (%)', label: 'Avg Idle Cores', unit: '%', icon: '💤', category: 'efficiency' },
            { key: 'Peak Memory Usage (%)', label: 'Peak Memory Usage', unit: '%', icon: '📈', category: 'memory' },
            { key: 'Preempted Executors', label: 'Preempted Executors', unit: '', icon: '⚠️', category: 'preempt' }
        ];

        // Helper functions
        function getValues(key, isByte) {
            return data.map(d => {
                let val = d[key];
                if (val === null || val === undefined || isNaN(val)) return null;
                if (isByte) {
                    return formatBytes(val, currentUnit);
                }
                return Number(val);
            }).filter(v => v !== null && !isNaN(v));
        }

        function calcMin(arr) {
            return arr.length > 0 ? Math.min(...arr) : 0;
        }

        function calcMax(arr) {
            return arr.length > 0 ? Math.max(...arr) : 0;
        }

        function calcAvg(arr) {
            return arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
        }

        function calcMedian(arr) {
            if (arr.length === 0) return 0;
            const sorted = [...arr].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
        }

        function formatValue(val, unit) {
            const intVal = Math.round(val);
            if (unit === '%') {
                return intVal.toLocaleString() + '%';
            } else if (unit === 'sec') {
                return intVal.toLocaleString() + 's';
            } else if (unit && unit !== '') {
                return intVal.toLocaleString() + ' ' + unit;
            }
            return intVal.toLocaleString();
        }

        // Create header
        const header = document.createElement('h3');
        header.textContent = `📊 Statistics Summary (${data.length} Applications)`;
        statsCardsContainer.appendChild(header);

        // Create cards
        metricsConfig.forEach(metric => {
            const values = getValues(metric.key, metric.isByte);

            if (values.length === 0) return; // Skip if no data

            const min = calcMin(values);
            const max = calcMax(values);
            const avg = calcAvg(values);
            const median = calcMedian(values);

            const card = document.createElement('div');
            card.className = `stat-card ${metric.category}`;

            card.innerHTML = `
                <div class="stat-card-header">
                    <div class="stat-card-icon">${metric.icon}</div>
                    <div class="stat-card-title">${metric.label}</div>
                </div>
                <div class="stat-card-values">
                    <div class="stat-value-item">
                        <div class="stat-value-label">MIN</div>
                        <div class="stat-value-number">${formatValue(min, metric.unit)}</div>
                    </div>
                    <div class="stat-value-item">
                        <div class="stat-value-label">MAX</div>
                        <div class="stat-value-number">${formatValue(max, metric.unit)}</div>
                    </div>
                    <div class="stat-value-item">
                        <div class="stat-value-label">MEDIAN</div>
                        <div class="stat-value-number">${formatValue(median, metric.unit)}</div>
                    </div>
                    <div class="stat-value-item">
                        <div class="stat-value-label">AVG</div>
                        <div class="stat-value-number">${formatValue(avg, metric.unit)}</div>
                    </div>
                </div>
            `;

            statsCardsContainer.appendChild(card);
        });
    }

    // Drag and Drop Handlers
    let draggedColumn = null;

    function handleDragStart(e) {
        draggedColumn = e.target.closest('th').dataset.column;
        e.target.closest('th').classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
    }

    function handleDragOver(e) {
        e.preventDefault(); // Necessary to allow dropping
        const th = e.target.closest('th');
        if (th && th.dataset.column !== draggedColumn) {
            th.classList.add('drag-over');
        }
        e.dataTransfer.dropEffect = 'move';
        return false;
    }

    function handleDragLeave(e) {
        const th = e.target.closest('th');
        if (th) th.classList.remove('drag-over');
    }

    function handleDrop(e, targetColumn, data) {
        e.stopPropagation();
        e.preventDefault();

        const th = e.target.closest('th');
        if (th) th.classList.remove('drag-over', 'dragging');

        // Remove dragging class from all (cleanup)
        document.querySelectorAll('.sortable').forEach(el => el.classList.remove('dragging', 'drag-over'));

        if (draggedColumn !== targetColumn) {
            const fromIndex = columnOrder.indexOf(draggedColumn);
            const toIndex = columnOrder.indexOf(targetColumn);

            if (fromIndex > -1 && toIndex > -1) {
                // Move element
                columnOrder.splice(fromIndex, 1);
                columnOrder.splice(toIndex, 0, draggedColumn);

                // Re-render
                renderResults(data);
            }
        }
        draggedColumn = null;
    }

    function showLoading(show, message = "Analyzing...") {
        const msgEl = document.getElementById('loading-message');
        if (show) {
            if (msgEl) msgEl.textContent = message;
            loadingOverlay.classList.remove('hidden');
        } else {
            loadingOverlay.classList.add('hidden');
        }
    }


    // ===== Tooltip Helper Functions =====
    function showTooltip(element, text) {
        const tooltip = document.getElementById('global-tooltip');
        if (!tooltip || !text) return;

        tooltip.textContent = text;
        tooltip.classList.remove('hidden');

        // Get element position
        const rect = element.getBoundingClientRect();

        // Position tooltip above the element, centered
        const tooltipRect = tooltip.getBoundingClientRect();
        let left = rect.left + (rect.width / 2);
        let top = rect.top;

        // Adjust if tooltip goes off-screen horizontally
        if (left - tooltipRect.width / 2 < 10) {
            left = tooltipRect.width / 2 + 10;
        } else if (left + tooltipRect.width / 2 > window.innerWidth - 10) {
            left = window.innerWidth - tooltipRect.width / 2 - 10;
        }

        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    }

    function hideTooltip() {
        const tooltip = document.getElementById('global-tooltip');
        if (tooltip) {
            tooltip.classList.add('hidden');
        }
    }

    function attachTooltipListeners() {
        // Remove old listeners by cloning (prevents duplicate listeners)
        const triggers = document.querySelectorAll('.tooltip-trigger');

        triggers.forEach(trigger => {
            trigger.addEventListener('mouseenter', function (e) {
                const tooltipText = this.getAttribute('data-tooltip');
                if (tooltipText) {
                    showTooltip(this, tooltipText);
                }
            });

            trigger.addEventListener('mouseleave', function (e) {
                hideTooltip();
            });
        });
    }

});
