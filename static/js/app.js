document.addEventListener('DOMContentLoaded', () => {
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
    // ...

    const panelList = document.getElementById('log-selection-view');
    const panelResult = document.getElementById('analysis-result-view');
    const panelDetail = document.getElementById('detail-result-view');
    const loadingOverlay = document.getElementById('loading-overlay');

    // ...

    // Event Listeners
    btnStart.addEventListener('click', startAnalysis);
    btnStartDetail.addEventListener('click', startDetailAnalysis);
    btnViewRecent.addEventListener('click', fetchRecentResults);
    btnBack.addEventListener('click', showList);
    btnBackFromDetail.addEventListener('click', showList);
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

        showLoading(true);
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

    function renderDetailTable(data) {
        // data = { appInfo: {...}, stages: [...] }
        const stages = data.stages || [];
        const appInfo = data.appInfo || {};

        // Render Header Info
        const infoDiv = document.getElementById('app-detail-info');
        infoDiv.innerHTML = `App Name: <strong>${appInfo.name}</strong> | App ID: <strong>${appInfo.id}</strong>`;

        const tbody = document.querySelector('#detail-table tbody');
        tbody.innerHTML = '';

        stages.forEach(stage => {
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

    function showDetail() {
        panelList.classList.add('hidden');
        panelResult.classList.add('hidden');
        panelDetail.classList.remove('hidden');
    }

    function showResult() {
        panelList.classList.add('hidden');
        panelDetail.classList.add('hidden');
        panelResult.classList.remove('hidden');
    }

    function showList() {
        panelList.classList.remove('hidden');
        panelResult.classList.add('hidden');
        panelDetail.classList.add('hidden');
    }
    const selectAllCb = document.getElementById('select-all');
    const logoBtn = document.getElementById('logo-btn');
    const unitBtns = document.querySelectorAll('.unit-btn');
    const sortHeaders = document.querySelectorAll('.sortable');

    // New Buttons
    const btnUpload = document.getElementById('btn-upload-log');
    const btnDelete = document.getElementById('btn-delete-log');
    const uploadInput = document.getElementById('log-upload-input');

    // State
    let logFiles = [];
    let currentResults = []; // Store raw data
    let currentUnit = 'GB';   // Default to GB
    let metricDefinitions = {}; // Store tooltips

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
    fetchDefinitions();
    fetchLogs();

    async function fetchDefinitions() {
        try {
            const res = await fetch('/api/definitions');
            if (res.ok) {
                metricDefinitions = await res.json();
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
        window.location.href = '/api/results/download';
    });
    logoBtn.addEventListener('click', showList);

    // Upload & Delete Listeners
    btnUpload.addEventListener('click', () => uploadInput.click());
    uploadInput.addEventListener('change', handleUpload);
    btnDelete.addEventListener('click', handleDelete);

    selectAllCb.addEventListener('change', (e) => {
        const checked = e.target.checked;
        document.querySelectorAll('.file-checkbox').forEach(cb => cb.checked = checked);
    });

    unitBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            unitBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentUnit = btn.dataset.unit;
            renderResults(currentResults);
        });
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

    async function fetchLogs() {
        try {
            const res = await fetch('/api/logs');
            logFiles = await res.json();

            // 1. Load cache
            const nameCache = JSON.parse(localStorage.getItem('spark_app_names_cache') || '{}');

            // 2. Identify missing names
            const missingFiles = [];
            logFiles.forEach(f => {
                if (nameCache[f.filename]) {
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
        listTableBody.innerHTML = '';

        // Sorting Logic
        if (files) {
            files.sort((a, b) => {
                let valA = a[sortState.column] || '';
                let valB = b[sortState.column] || '';

                // Handle Loading... text for appName creates bad sort
                // We'll treat missing appname as empty string or keep it as matches
                if (sortState.column === 'appName') {
                    // If it's undefined/null, treat as empty
                    if (!valA) valA = '';
                    if (!valB) valB = '';
                }

                if (valA < valB) return sortState.direction === 'asc' ? -1 : 1;
                if (valA > valB) return sortState.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }

        files.forEach(file => {
            const tr = document.createElement('tr');
            const appName = file.appName || '<span style="color:#aaa;">Loading...</span>';

            tr.innerHTML = `
                <td><input type="checkbox" class="file-checkbox" value="${file.filename}"></td>
                <td>${file.filename}</td>
                <td>${appName}</td>
                <td>${file.date}</td>
            `;
            listTableBody.appendChild(tr);
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

        showLoading(true);
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

            // Analysis success, fetch results
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

        showLoading(true);
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

    async function handleDelete() {
        const selected = Array.from(document.querySelectorAll('.file-checkbox:checked')).map(cb => cb.value);
        if (selected.length === 0) {
            alert('삭제할 로그를 선택하세요.');
            return;
        }

        if (!confirm(`선택한 ${selected.length}개의 파일을 삭제하시겠습니까?`)) return;

        showLoading(true);
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
        showLoading(true);
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

        // If it's effectively an integer (e.g. 10 GB exactly), we could strip zeros,
        // but user requested "소숫점 6자리까지 표기". Usually implies fixed or max.
        // Let's use toFixed(6) but maybe strip trailing zeros if it's cleaner?
        // "6자리까지 표기되록" -> usually means "up to 6" or "fixed 6".
        // Let's do up to 6 significant decimals to look cleaner,
        // e.g. 10.5 not 10.500000.
        // But for consistency let's stick to parseFloat(val.toFixed(6)) which strips trailing zeros.

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

        // Create Headers based on columnOrder
        const trHead = document.createElement('tr');
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
            if (metricDefinitions[key]) {
                innerContent = `<span class="tooltip-trigger">${text}<span class="tooltip-content">${metricDefinitions[key]}</span></span>`;
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
        displayData.forEach(row => {
            const tr = document.createElement('tr');
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

                td.textContent = val;
                tr.appendChild(td);
            });
            resultTableBody.appendChild(tr);
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

    function showLoading(show) {
        if (show) loadingOverlay.classList.remove('hidden');
        else loadingOverlay.classList.add('hidden');
    }


});
