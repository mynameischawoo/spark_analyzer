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

    let executorChart = null;

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

    // --- SQL Plan Helpers ---

    // Optimization: Deduplicate Scans AND Consolidate Writes
    function optimizePlan(root) {
        if (!root) return null;

        // Helper: Collect metrics deeply from all descendants
        function getDeepMetrics(startNode, targetMetrics) {
            const acc = {}; // map name -> [ids]

            function scanDeep(n) {
                if (!n) return;

                // If this node has metrics, add them
                if (n.metrics) {
                    n.metrics.forEach(m => {
                        if (!m || !m.name) return; // Safety check

                        // Filter? Or just take all?
                        // We strictly want "written" stuff or generic rows
                        const mName = m.name.toLowerCase();
                        // Write semantic metrics
                        const relevant = mName.includes("files") || mName.includes("bytes") || mName.includes("size") || mName.includes("rows") || mName.includes("partitions");

                        if (relevant) {
                            if (!acc[m.name]) acc[m.name] = [];
                            // Avoid duplicates? Accumulator ID is unique.
                            if (!acc[m.name].includes(m.accumulatorId)) {
                                acc[m.name].push(m.accumulatorId);
                            }
                        }
                    });
                }

                if (n.children) n.children.forEach(scanDeep);
            }

            // Don't scan the node itself again if we are calling this on children
            if (startNode.children) startNode.children.forEach(scanDeep);

            return acc;
        }

        // 1. Identify Scans & Writes
        const scans = [];
        const writes = [];

        function traverse(node) {
            if (!node) return;
            const name = node.nodeName.toLowerCase();

            if (name.includes("scan") && !name.includes("onerowrelation")) {
                scans.push(node);
            } else if (name.includes("command") || name.includes("write") || name.includes("insert")) {
                writes.push(node);
            }

            if (node.children) {
                node.children.forEach(traverse);
            }
        }

        // Traverse full tree (or what's left of it)
        traverse(root);
        // Note: traverse() above is just collecting references.
        // If we want to safely modify the tree structure, we should do it carefully.
        // But for METRIC CONSOLIDATION, we can update the objects in place.

        // 2. Consolidate Writes (DEEP MERGE)
        writes.forEach(writeNode => {
            // Find all relevant metrics in the subtree
            const deepMetrics = getDeepMetrics(writeNode);

            // Merge into writeNode._mergedAccums
            if (!writeNode._mergedAccums) writeNode._mergedAccums = {};

            Object.keys(deepMetrics).forEach(k => {
                if (!writeNode._mergedAccums[k]) writeNode._mergedAccums[k] = [];
                deepMetrics[k].forEach(id => {
                    // Check if already present
                    if (!writeNode._mergedAccums[k].includes(id)) {
                        writeNode._mergedAccums[k].push(id);
                    }
                });
            });

            // Also try to capture child strings for parsing (naive, just first child?)
            if (writeNode.children && writeNode.children.length > 0) {
                // Concatenate simple strings of direct children just in case
                writeNode._childString = writeNode.children.map(c => c.simpleString).join(" ");
            }
        });

        // 3. Group Scans by Table/Path
        const uniqueScans = {};

        scans.forEach(scan => {
            const simple = scan.simpleString || '';
            let ident = simple;
            const match = simple.match(/([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*)/);
            if (match) ident = match[0];
            else {
                const pathMatch = simple.match(/Location: \[(.*?)\]/);
                if (pathMatch) ident = pathMatch[1];
            }

            if (!uniqueScans[ident]) {
                uniqueScans[ident] = { ...scan, children: [] };
                uniqueScans[ident]._mergedAccums = {};
            }

            if (scan.metrics) {
                scan.metrics.forEach(m => {
                    if (!m || !m.name) return; // Safety check
                    if (!uniqueScans[ident]._mergedAccums[m.name]) uniqueScans[ident]._mergedAccums[m.name] = [];
                    uniqueScans[ident]._mergedAccums[m.name].push(m.accumulatorId);
                });
            }
        });

        // 4. Reconstruct Children (Scans + Writes)
        // Since strict mode removes intermediates, we return a root that points to the NEW Unique Scans + Original Writes.
        // But 'root' is likely the Write command itself?
        // If root is a Write/Command, we keep it as is (it has consolidated metrics).
        // But we want to replace its children with the unique scans.

        const name = root.nodeName.toLowerCase();
        if (name.includes("command") || name.includes("write") || name.includes("insert")) {
            // Root is the Write. It should have the scans as children.
            const newScanChildren = Object.values(uniqueScans);

            // Do we strictly want to replace ALL children?
            // Users wants "Read Parquet" nodes.
            return {
                ...root,
                children: newScanChildren,
                _mergedAccums: root._mergedAccums // Preservation
            };
        }

        // If root is not the write (e.g. wrapper), return structure?
        // Fallback: Just return root with existing structure but deduplicated scans?
        // Since simplifyPlan skips to Scans/Output, the children array likely has the raw Scans.
        // We replace them.
        return {
            ...root,
            children: Object.values(uniqueScans)
        };
    }

    function simplifyPlan(node) {
        // ... (Keep existing simple logic)
        // We rely on simplify to stripe out the middle
        if (!node) return null;

        let newChildren = [];
        if (node.children) {
            node.children.forEach(child => {
                const simplerChild = simplifyPlan(child);
                if (simplerChild) newChildren.push(simplerChild);
            });
        }

        const name = node.nodeName.toLowerCase();
        const isOneRow = name.includes("onerowrelation");
        const isScan = (name.includes("scan") || name.includes("hadoopfsrelation")) && !isOneRow;
        const isOutput = name.includes("command") || name.includes("write") || name.includes("insert");

        // Keep Write (Output) nodes and Scan nodes.
        // Intermediate nodes are skipped.
        const shouldKeep = isScan || isOutput;

        if (!shouldKeep) {
            if (newChildren.length === 1) return newChildren[0];
            if (newChildren.length === 0) return null;
            if (newChildren.length > 1) return { ...node, children: newChildren, nodeName: "Flow" };
            return newChildren[0];
        }

        return { ...node, children: newChildren };
    }

    // --- DataFlint Ported Parsers ---

    function dataFlintSpecialSplit(input) {
        const result = [];
        let buffer = "";
        let bracketCount = 0;
        let inQuotes = false;

        for (let i = 0; i < input.length; i++) {
            const char = input[i];
            if (char === "[") bracketCount++;
            if (char === "]") bracketCount--;
            if (char === '"') inQuotes = !inQuotes;

            if (char === "," && bracketCount === 0 && !inQuotes) {
                result.push(buffer.trim());
                buffer = "";
            } else {
                buffer += char;
            }
        }
        if (buffer) result.push(buffer.trim());
        return result;
    }

    function parseDataFlintWrite(input) {
        // Ported from WriteToHDFSParser.ts
        let raw = input.replace("Execute InsertIntoHadoopFsRelationCommand", "").trim();
        if (raw.startsWith("InsertIntoHadoopFsRelationCommand")) {
            raw = raw.replace("InsertIntoHadoopFsRelationCommand", "").trim();
        }

        if (raw.startsWith("(") && raw.endsWith(")")) {
            raw = raw.substring(1, raw.length - 1);
        }

        const parts = dataFlintSpecialSplit(raw);
        if (parts.length < 2) return { location: raw };

        let parsed = {
            location: parts[0],
            format: "unknown",
            mode: "unknown",
            tableName: null,
            partitionKeys: null
        };

        if (parts[2] && parts[2].includes("[")) {
            parsed.partitionKeys = parts[2].slice(1, -1);
            parsed.format = parts[3];
            parsed.mode = parts[5];
        } else {
            parsed.format = parts[2];
            parsed.mode = parts[4];
        }

        if (parts[4] && parts[4].includes("`")) parsed.tableName = parts[4];
        else if (parts[5] && parts[5].includes("`")) parsed.tableName = parts[5];
        else if (parts.length > 6 && parts[6] && parts[6].includes("`")) parsed.tableName = parts[6];

        return parsed;
    }

    function parseDataFlintScan(input, nodeName) {
        // Ported from ScanFileParser.ts
        const result = {};

        const formatMatch = /Format: (\w+),/.exec(input);
        if (formatMatch) result.format = formatMatch[1];
        if (!result.format && nodeName.includes("Scan")) {
            const parts = nodeName.split(" ");
            if (parts.length >= 2) result.format = parts[1];
        }

        const locMatch = /Location: \w+\(.*\)(?:\[(.*?)\])/.exec(input) || /Location: \[(.*?)\]/.exec(input);

        if (locMatch) {
            let path = locMatch[1];
            if (path.includes("...")) {
                result.location = path.split(",")[0];
            } else {
                result.location = path;
            }
        }

        const partMatch = /PartitionFilters: \[(.*?)\]/.exec(input);
        if (partMatch) result.partitionFilters = partMatch[1];

        const pushMatch = /PushedFilters: \[(.*?)\]/.exec(input);
        if (pushMatch) result.pushedFilters = pushMatch[1];

        const nameParts = nodeName.split(" ");
        if (nameParts.length >= 3) {
            result.tableName = nameParts[2];
        }

        return result;
    }

    // Helper to sum stage metrics (Global Fallback)
    function getStageMetrics(stages) {
        let bytes = 0;
        let rows = 0;
        if (stages) {
            stages.forEach(s => {
                // Sum ALL output-bearing stages regardless of ID
                // This is robust for AQE split scenarios where ID filtering fails
                if ((s["Output"] || 0) > 0 || (s["Output Records"] || 0) > 0) {
                    bytes += (s["Output"] || 0);
                    rows += (s["Output Records"] || 0);
                }
            });
        }
        return { bytes, rows };
    }

    function renderRecursivePlan(node, accumulators, container, stages, allExecutions) {
        if (!node) return '';

        if (node.nodeName === "Flow") {
            let html = `
                <div class="node-wrapper">
                     <div class="sql-children" style="margin-top:0; padding-top:10px; border-top: 1px dashed #ddd;">`;
            if (node.children) {
                node.children.forEach(child => {
                    html += renderRecursivePlan(child, accumulators, container, stages, allExecutions);
                });
            }
            html += `   </div>
                </div>`;
            return html;
        }

        const name = node.nodeName.toLowerCase();
        // Combined string for extraction (include consolidated child string)
        const simple = (node.simpleString || '') + (node._childString || '');

        let title = node.nodeName;
        let icon = '📄';

        let details = [];
        let parsedMeta = {};

        // --- Determine Type and Parse ---
        if (name.includes("insertinto") || name.includes("command") || name.includes("write")) {
            title = "Write To Hdfs";
            icon = '💾';
            // Use DataFlint Write Parser
            const writeInfo = parseDataFlintWrite(simple);

            // Map parsed info to details order
            if (writeInfo.tableName) parsedMeta.table = writeInfo.tableName;
            if (writeInfo.location) parsedMeta.path = writeInfo.location;
            if (writeInfo.format && writeInfo.format !== "unknown") parsedMeta.format = writeInfo.format;
            if (writeInfo.partitionKeys) parsedMeta.partitionBy = writeInfo.partitionKeys;

        } else if (name.includes("scan")) {
            // Use DataFlint Scan Parser
            const scanInfo = parseDataFlintScan(simple, node.nodeName);

            if (scanInfo.format) {
                title = "Read " + scanInfo.format;
                title = title.replace(/\b\w/g, c => c.toUpperCase()); // proper case
            } else {
                title = "Read Data";
            }

            if (scanInfo.tableName) parsedMeta.table = scanInfo.tableName;
            if (scanInfo.location) parsedMeta.path = scanInfo.location;
            if (scanInfo.partitionFilters) parsedMeta.partitionFilters = scanInfo.partitionFilters;
            if (scanInfo.pushedFilters) parsedMeta.pushedFilters = scanInfo.pushedFilters;
            if (scanInfo.format) parsedMeta.format = scanInfo.format;
        }

        // --- Metrics Processing ---
        let metricsSource = node._mergedAccums ?
            Object.keys(node._mergedAccums).map(k => ({ name: k, isMerged: true, ids: node._mergedAccums[k] })) :
            (node.metrics || []);

        const getVal = (mItem) => {
            if (mItem.isMerged) {
                let t = 0; mItem.ids.forEach(id => { t += Number(accumulators[id] || 0); }); return t;
            } else {
                return Number(accumulators[mItem.accumulatorId] || 0);
            }
        };

        let collectedMetrics = {
            rows: 0,
            files: 0,
            bytes: 0,
            partitions: 0,

            // Explicit separate parsing
            filesRead: 0,
            filesWritten: 0,
            bytesRead: 0,
            bytesWritten: 0,
            rowsRead: 0,
            rowsWritten: 0,
            partitionsWritten: 0
        };

        // Populate collectedMetrics
        metricsSource.forEach(m => {
            if (!m) return;
            const val = getVal(m);
            const mName = m.name ? m.name.toLowerCase() : "";
            if (!mName) return;

            // Generic fallback
            if (mName.includes("files")) collectedMetrics.files = val;
            if (mName.includes("bytes") || mName.includes("size")) collectedMetrics.bytes = val;
            if (mName.includes("rows")) collectedMetrics.rows = val;

            // Specific parsing
            if (mName.includes("files") && mName.includes("read")) collectedMetrics.filesRead = val;
            if (mName.includes("files") && (mName.includes("written") || mName.includes("output"))) collectedMetrics.filesWritten = val;

            if (mName.includes("bytes") && mName.includes("read")) collectedMetrics.bytesRead = val;
            if (mName.includes("bytes") && (mName.includes("written") || mName.includes("output"))) collectedMetrics.bytesWritten = val;

            if (mName.includes("rows") && (mName.includes("written") || mName.includes("output"))) collectedMetrics.rowsWritten = val;
            // Rows Read often just "rows" in Scan, but "number of output rows" can be ambiguous. 
            // Usually "number of output rows" in Scan == Rows Read.
            if (mName.includes("rows") && !mName.includes("written")) collectedMetrics.rowsRead = val;

            if (mName.includes("partitions")) collectedMetrics.partitionsWritten = val;
        });

        // --- Construct Details Array (Strict Order per Type) ---

        // --- DataFlint Logic Port: Metric Processing ---
        function humanFileSize(bytes) {
            if (!bytes || bytes === 0) return "0 B";
            const k = 1024;
            const dm = 2;
            const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }

        // Ported from MetricProcessors.tsx + Enhanced with Cross-Execution Scavenging
        function processOutputNodeMetrics(node, accumulators, stages, allExecutions) {
            const metrics = [];

            // 1. Local Node Metrics
            let sources = node._mergedAccums ?
                Object.keys(node._mergedAccums).map(k => ({ name: k, isMerged: true, ids: node._mergedAccums[k] })) :
                (node.metrics || []);

            const getVal = (mItem) => {
                if (mItem.isMerged) {
                    let t = 0; mItem.ids.forEach(id => { t += Number(accumulators[id] || 0); }); return t;
                } else {
                    return Number(accumulators[mItem.accumulatorId] || 0);
                }
            };

            let bytesWritten = 0;
            let rows = 0;

            // Helper for loose matching
            const isBytes = (n) => {
                const s = n.toLowerCase().replace(/\s/g, '');
                return s.includes("byteswritten") || s.includes("outputbytes") || s === "writtenoutput" || s === "bytes";
            };
            const isRows = (n) => {
                const s = n.toLowerCase().replace(/\s/g, '');
                return s.includes("rowswritten") || s.includes("outputrows") || s === "numberofoutputrows" || s === "rows";
            };

            sources.forEach(m => {
                if (!m) return; // Safety check
                const val = getVal(m);
                const name = m.name;
                if (!name) return; // Safety check

                if (isBytes(name)) bytesWritten = val;
                if (isRows(name)) rows = val;
            });

            // 2. Cross-Execution Scavenging (The "Seeker" Logic)
            if (allExecutions && allExecutions.length > 0) {
                let scavengedBytes = 0;
                let scavengedRows = 0;

                allExecutions.forEach(exec => {
                    // Find ANY node that has the metrics we look for.
                    function scanAllNodes(n) {
                        if (!n) return;

                        if (n.metrics) {
                            n.metrics.forEach(m => {
                                if (!m || !m.name) return; // Safety check
                                const mName = m.name;
                                const mVal = Number(accumulators[m.accumulatorId] || 0);

                                if (isBytes(mName)) scavengedBytes = Math.max(scavengedBytes, mVal);
                                if (isRows(mName)) scavengedRows = Math.max(scavengedRows, mVal);
                            });
                        }
                        if (n.children) n.children.forEach(scanAllNodes);
                    }
                    scanAllNodes(exec.plan);
                });

                bytesWritten = Math.max(bytesWritten, scavengedBytes);
                rows = Math.max(rows, scavengedRows);
            }

            // 3. Stage Global Fallback (The "Safety Net")
            // Finally, check against the raw Stage Output metrics (Task Metrics)
            const stageM = getStageMetrics(stages);

            rows = Math.max(rows, stageM.rows);
            bytesWritten = Math.max(bytesWritten, stageM.bytes);


            // Construct Final Metric List
            if (rows > 0) metrics.push({ name: "Rows", value: rows.toLocaleString() });
            if (bytesWritten > 0) metrics.push({ name: "Bytes Written", value: humanFileSize(bytesWritten) });

            return metrics;
        }

        if (name.includes("insertinto") || name.includes("command") || name.includes("write")) {
            // Write To Hdfs Order: Rows, Bytes Written, Partition By, File Path, Format

            // 1. Metrics First
            const outputMetrics = processOutputNodeMetrics(node, accumulators, stages, allExecutions);
            outputMetrics.forEach(m => {
                if (m && m.name) details.push({ label: m.name, value: m.value });
            });

            // 2. Metadata Next
            if (parsedMeta.partitionBy) {
                // Remove #ID suffixes (e.g. platform#34 -> platform)
                const cleanPartition = parsedMeta.partitionBy.replace(/#\d+/g, '');
                details.push({ label: "Partition By", value: cleanPartition });
            }
            if (parsedMeta.path) {
                let p = parsedMeta.path;
                if (p.length > 35) p = p.substring(0, 12) + "..." + p.substring(p.length - 18);
                details.push({ label: "File Path", value: p });
            }
            if (parsedMeta.format) details.push({ label: "Format", value: parsedMeta.format });

        } else if (name.includes("scan")) {
            // Read Parquet Order: File Read, Bytes Read, Rows, Average File Size, File Path, Partition Filters, Table

            let finalFilesRead = collectedMetrics.filesRead > 0 ? collectedMetrics.filesRead : collectedMetrics.files;
            let finalBytesRead = collectedMetrics.bytesRead > 0 ? collectedMetrics.bytesRead : collectedMetrics.bytes;
            let finalRowsRead = collectedMetrics.rowsRead > 0 ? collectedMetrics.rowsRead : collectedMetrics.rows;

            let avgSize = 0;
            if (finalFilesRead > 0 && finalBytesRead > 0) {
                avgSize = finalBytesRead / finalFilesRead;
            }

            if (finalFilesRead > 0) details.push({ label: "Files Read", value: finalFilesRead.toLocaleString() });
            if (finalBytesRead > 0) details.push({ label: "Bytes Read", value: formatBytes(finalBytesRead) });
            if (finalRowsRead > 0) details.push({ label: "Rows", value: finalRowsRead.toLocaleString() });
            if (avgSize > 0) details.push({ label: "Average File Size", value: formatBytes(avgSize) });

            if (parsedMeta.path) {
                let p = parsedMeta.path;
                if (p.length > 35) p = p.substring(0, 12) + "..." + p.substring(p.length - 18);
                details.push({ label: "File Path", value: p });
            }
            // Ensure partition filters show "Full Scan" if empty or missing, but ONLY if explicit partition filters field was expected
            // DataFlint shows "Partition Filters: Full Scan" when empty.
            // In my port, parseDataFlintScan returns empty string or "Full Scan" logic?
            // In my port, parseDataFlintScan returns `partitionFilters: "Full Scan"` if empty regex.
            // So if parsedMeta.partitionFilters is set, use it.
            if (parsedMeta.partitionFilters) {
                details.push({ label: "Partition Filters", value: parsedMeta.partitionFilters });
            } else {
                // If not found, check if it had a chance to be found
                // Actually DataFlint Scan parser returns empty string or "Full Scan" logic?
                // In my port, parseDataFlintScan returns `partitionFilters: "Full Scan"` if empty regex.
                // So if parsedMeta.partitionFilters is set, use it.
            }

            if (parsedMeta.table) details.push({ label: "Table", value: parsedMeta.table });

            if (parsedMeta.pushedFilters) details.push({ label: "Push Down Filters", value: parsedMeta.pushedFilters });
        } else {
            // Fallback
            Object.keys(collectedMetrics).forEach(k => {
                // Skip internal helper keys
                if (['filesRead', 'filesWritten', 'bytesRead', 'bytesWritten', 'rowsRead', 'rowsWritten'].includes(k)) return;
                if (collectedMetrics[k] > 0) details.push({ label: k, value: collectedMetrics[k].toLocaleString() });
            });
        }

        let rowsHtml = '';
        details.forEach(d => {
            rowsHtml += `
                <div class="rich-row">
                    <span class="rich-label">${d.label}:</span>
                    <span class="rich-value">${d.value}</span>
                </div>`;
        });

        const footerHtml = `<div class="rich-footer"><span style="color:#22c55e; font-size:1.2em;">✔</span></div>`;

        let html = `
            <div class="node-wrapper">
                <div class="sql-node-rich">
                    <div class="rich-header">${title}<span class="rich-header-icon">${icon}</span></div>
                    <div class="rich-body">${rowsHtml}</div>
                    ${footerHtml}
                </div>`;

        if (node.children && node.children.length > 0) {
            html += '<div class="sql-children">';
            node.children.forEach(child => html += renderRecursivePlan(child, accumulators, container, stages, allExecutions));
            html += '</div>';
        }
        html += '</div>';
        return html;
    }

    function renderDetailTable(data) {
        console.log("Detail Analysis Data:", data); // Debug: Check if sqlExecutions exists
        // Render Chart
        if (data.executorTimeSeries) {
            renderExecutorChart(data.executorTimeSeries);
        }

        // Render SQL Plan
        const sqlContainer = document.getElementById('sql-plan-container');
        const sqlContent = document.getElementById('sql-plan-content');

        if (data.sqlExecutions && Object.keys(data.sqlExecutions).length > 0) {
            sqlContainer.classList.remove('hidden');

            // Find the "Best" execution to show (Priority: Match User Criteria -> Then Max nodes)
            const execs = Object.values(data.sqlExecutions);

            let bestExec = null;
            let targetExecId = null;

            // 1. Priority: Find Stage matching "parquet at NativeMethodAccessorImpl"
            // The user specifically asked to filter by this Stage Name/Callsite.
            if (data.stages) {
                const targetStage = data.stages.find(s => {
                    const name = (s["Stage Name"] || "").toLowerCase();
                    const desc = (s["Description"] || "").toLowerCase();
                    const key = "nativemethodaccessorimpl.java:0"; // specific keyword
                    return name.includes(key) || desc.includes(key);
                });

                if (targetStage && targetStage["Execution ID"]) {
                    targetExecId = targetStage["Execution ID"];
                    console.log("Found Target Stage:", targetStage["Stage Name"], "ExecID:", targetExecId);
                }
            }

            if (targetExecId && data.sqlExecutions[targetExecId]) {
                bestExec = data.sqlExecutions[targetExecId];
            } else {
                // 2. Fallback: Max Nodes
                let maxNodes = -1;

                function countNodes(node) {
                    if (!node) return 0;
                    let c = 1;
                    if (node.children) {
                        node.children.forEach(child => c += countNodes(child));
                    }
                    return c;
                }

                execs.forEach(exec => {
                    const c = countNodes(exec.plan);
                    if (c > maxNodes) {
                        maxNodes = c;
                        bestExec = exec;
                    }
                });
            }

            if (bestExec) {
                // 1. First cleanup noise
                const simplifiedPlan = simplifyPlan(bestExec.plan);
                // 2. Then flattened and deduplicate Scans
                const finalPlan = optimizePlan(simplifiedPlan);

                // Pass Object.values(data.sqlExecutions) to enable scavenging
                sqlContent.innerHTML = renderRecursivePlan(finalPlan, data.accumulators || {}, null, data.stages, Object.values(data.sqlExecutions));
            }
        } else {
            sqlContainer.classList.add('hidden');
        }

        const { stages } = data;
        const appName = data.appInfo ? data.appInfo.name : (data.appName || "Unknown");
        const appId = data.appInfo ? data.appInfo.id : (data.appId || "Unknown");

        // Render Header Info
        const infoDiv = document.getElementById('app-detail-info');
        infoDiv.innerHTML = `<strong>App Name:</strong> ${appName} | <strong>App ID:</strong> ${appId}`;

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
