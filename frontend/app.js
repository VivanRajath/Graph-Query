/* ═══════════════════════════════════════════════════════════════
   Graph Query System — Frontend Application
   ═══════════════════════════════════════════════════════════════ */

const API_BASE = "http://localhost:8000";

// ─── Node Color Map ──────────────────────────────────────────
const NODE_COLORS = {
    SalesOrder:      "#6C7BFF",
    Order:           "#6C7BFF",
    OrderItem:       "#818CF8",
    Customer:        "#4ECDC4",
    Delivery:        "#FF9F43",
    Invoice:         "#26DE81",
    BillingDocument: "#26DE81",
    Payment:         "#A855F7",
    Product:         "#FF6B6B",
    Plant:           "#FBBF24",
    Address:         "#64748B",
    Unknown:         "#64748B",
};

// Darker borders for each color (20% darker)
function darkenColor(hex, amount = 0.2) {
    const num = parseInt(hex.slice(1), 16);
    let r = (num >> 16) & 255;
    let g = (num >> 8) & 255;
    let b = num & 255;
    r = Math.max(0, Math.floor(r * (1 - amount)));
    g = Math.max(0, Math.floor(g * (1 - amount)));
    b = Math.max(0, Math.floor(b * (1 - amount)));
    return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, "0")}`;
}

// ─── State ───────────────────────────────────────────────────
let cy = null;
let isLoading = false;
let overlayVisible = true;

// ─── Initialize Cytoscape ────────────────────────────────────
function initGraph() {
    cy = cytoscape({
        container: document.getElementById("cy"),
        style: [
            {
                selector: "node",
                style: {
                    width: 14,
                    height: 14,
                    "background-color": "data(color)",
                    "background-opacity": 0.9,
                    "border-width": 2,
                    "border-color": "data(borderColor)",
                    "overlay-opacity": 0,
                    label: "data(label)",
                    "font-size": 7,
                    "font-family": "'Inter', sans-serif",
                    "font-weight": 500,
                    color: document.body.classList.contains('dark-mode') ? '#f8fafc' : '#374151',
                    "text-valign": "bottom",
                    "text-halign": "center",
                    "text-margin-y": 6,
                    "text-opacity": 0,
                    "text-outline-width": 2,
                    "text-outline-color": document.body.classList.contains('dark-mode') ? '#0f172a' : '#ffffff',
                    "min-zoomed-font-size": 8,
                    "transition-property": "width, height, background-color, border-width",
                    "transition-duration": "0.2s",
                },
            },
            {
                // Show labels at higher zoom
                selector: "node",
                style: {
                    "text-opacity": 0,
                },
            },
            {
                selector: "node:active, node:selected",
                style: {
                    width: 22,
                    height: 22,
                    "background-opacity": 1,
                    "border-width": 3,
                    "border-color": "#1e40af",
                    "text-opacity": 1,
                    "shadow-blur": 12,
                    "shadow-color": "#3b82f6",
                    "shadow-opacity": 0.4,
                    "shadow-offset-x": 0,
                    "shadow-offset-y": 0,
                },
            },
            {
                selector: "node.highlighted",
                style: {
                    width: 20,
                    height: 20,
                    "border-width": 3,
                    "border-color": "#f59e0b",
                    "text-opacity": 1,
                    "shadow-blur": 10,
                    "shadow-color": "#f59e0b",
                    "shadow-opacity": 0.5,
                },
            },
            {
                selector: "node.dimmed",
                style: {
                    "background-opacity": 0.15,
                    "border-opacity": 0.2,
                    "text-opacity": 0,
                },
            },
            {
                selector: "edge",
                style: {
                    width: 1.2,
                    "line-color": "#93c5fd",
                    "curve-style": "bezier",
                    "overlay-opacity": 0,
                    "target-arrow-shape": "triangle",
                    "target-arrow-color": "#93c5fd",
                    "arrow-scale": 0.6,
                    opacity: 0.7,
                    "transition-property": "width, line-color, opacity",
                    "transition-duration": "0.2s",
                },
            },
            {
                selector: "edge:active, edge:selected",
                style: {
                    width: 2.5,
                    "line-color": "#3b82f6",
                    "target-arrow-color": "#3b82f6",
                    opacity: 1,
                },
            },
            {
                selector: "edge.dimmed",
                style: {
                    opacity: 0.08,
                },
            },
        ],
        layout: { name: "preset" },
        minZoom: 0.1,
        maxZoom: 5,
    });

    // Show labels on zoom
    window.labelsForcedHidden = false;
    cy.on("zoom", () => {
        if (window.labelsForcedHidden) return;
        const zoom = cy.zoom();
        cy.nodes().style("text-opacity", zoom > 1.2 ? 1 : 0);
    });

    // Node click → show details & expand
    cy.on("tap", "node", async function (evt) {
        const node = evt.target;
        showNodeDetails(node.data());
        await expandNode(node.data().id);
    });

    // Background click → hide details
    cy.on("tap", function (evt) {
        if (evt.target === cy) {
            hideNodeDetails();
            clearSearch();
        }
    });
}

// ─── Load Graph from API ─────────────────────────────────────
async function loadGraph() {
    showGraphLoading(true);
    try {
        const res = await fetch(`${API_BASE}/api/graph/nodes?limit=150`);
        const data = await res.json();
        updateStatus(true);
        populateGraph(data);
        updateGraphStats();
    } catch (err) {
        console.warn("Graph load failed:", err);
        updateStatus(false);
    } finally {
        showGraphLoading(false);
    }
}

function populateGraph(data) {
    if (!data.nodes || data.nodes.length === 0) return;

    const elements = [];

    data.nodes.forEach((node) => {
        const color = NODE_COLORS[node.type] || NODE_COLORS.Unknown;
        if (!cy.getElementById(node.id).length) {
            elements.push({
                group: "nodes",
                data: {
                    id: node.id,
                    label: truncate(node.label, 16),
                    type: node.type,
                    color: color,
                    borderColor: darkenColor(color, 0.25),
                    properties: node.properties || {},
                },
            });
        }
    });

    data.edges.forEach((edge) => {
        const edgeId = `${edge.source}-${edge.type}-${edge.target}`;
        if (!cy.getElementById(edgeId).length) {
            elements.push({
                group: "edges",
                data: {
                    id: edgeId,
                    source: edge.source,
                    target: edge.target,
                    label: edge.type.replace(/_/g, " "),
                },
            });
        }
    });

    if (elements.length > 0) {
        cy.add(elements);
        runLayout();
    }
}

function runLayout() {
    try {
        cy.layout({
            name: "cola",
            animate: true,
            animationDuration: 600,
            nodeSpacing: 30,
            edgeLength: 120,
            fit: true,
            padding: 40,
            randomize: false,
            maxSimulationTime: 3000,
        }).run();
    } catch (e) {
        // Fallback to grid if cola fails
        cy.layout({ name: "grid", fit: true, padding: 40, animate: true }).run();
    }
}

// ─── Expand Node ─────────────────────────────────────────────
async function expandNode(nodeId) {
    try {
        const res = await fetch(`${API_BASE}/api/graph/expand/${encodeURIComponent(nodeId)}`);
        const data = await res.json();
        if (data.nodes && data.nodes.length > 0) {
            populateGraph(data);
            updateGraphStats();
        }
    } catch (err) {
        console.warn("Node expansion failed:", err);
    }
}

// ─── Node Details ────────────────────────────────────────────
function showNodeDetails(data) {
    const panel = document.getElementById("nodeDetails");
    const labelEl = document.getElementById("nodeLabel");
    const propsEl = document.getElementById("nodeProps");

    labelEl.textContent = data.label || "Unknown";

    propsEl.innerHTML = "";
    const props = data.properties || {};
    Object.entries(props).forEach(([key, value]) => {
        if (value !== null && value !== "" && value !== undefined) {
            const row = document.createElement("div");
            row.className = "prop-row";
            row.innerHTML = `
                <span class="prop-key">${formatKey(key)}:</span>
                <span class="prop-value">${escapeHtml(String(value))}</span>
            `;
            propsEl.appendChild(row);
        }
    });

    panel.classList.add("visible");
}

function hideNodeDetails() {
    document.getElementById("nodeDetails").classList.remove("visible");
}

// ─── Graph Legend ────────────────────────────────────────────
function buildLegend() {
    const container = document.getElementById("graphLegend");
    if (!container) return;

    // Deduplicate: show only unique colors with meaningful labels
    const seen = new Set();
    const items = [];
    Object.entries(NODE_COLORS).forEach(([type, color]) => {
        if (type === "Unknown" || type === "Order" || type === "Invoice") return; // skip aliases
        if (!seen.has(color)) {
            seen.add(color);
            items.push({ type, color });
        }
    });

    container.innerHTML = items
        .map(
            (item) => `
        <div class="legend-item">
            <span class="legend-dot" style="background:${item.color}; box-shadow: 0 0 6px ${item.color}55;"></span>
            <span class="legend-label">${item.type}</span>
        </div>
    `
        )
        .join("");
}

// ─── Graph Stats ─────────────────────────────────────────────
function updateGraphStats() {
    const statsEl = document.getElementById("graphStats");
    if (!statsEl || !cy) return;
    const nodeCount = cy.nodes().length;
    const edgeCount = cy.edges().length;
    statsEl.textContent = `${nodeCount} nodes · ${edgeCount} edges`;
}

// ─── Graph Search / Filter ───────────────────────────────────
function setupGraphSearch() {
    const input = document.getElementById("graphSearch");
    if (!input) return;

    input.addEventListener("input", () => {
        const query = input.value.trim().toLowerCase();
        if (!query) {
            clearSearch();
            return;
        }
        cy.nodes().forEach((node) => {
            const label = (node.data("label") || "").toLowerCase();
            const type = (node.data("type") || "").toLowerCase();
            if (label.includes(query) || type.includes(query)) {
                node.removeClass("dimmed").addClass("highlighted");
            } else {
                node.removeClass("highlighted").addClass("dimmed");
            }
        });
        cy.edges().forEach((edge) => {
            const src = edge.source();
            const tgt = edge.target();
            if (src.hasClass("highlighted") || tgt.hasClass("highlighted")) {
                edge.removeClass("dimmed");
            } else {
                edge.addClass("dimmed");
            }
        });
    });
}

function clearSearch() {
    const input = document.getElementById("graphSearch");
    if (input) input.value = "";
    if (cy) {
        cy.nodes().removeClass("highlighted dimmed");
        cy.edges().removeClass("dimmed");
    }
}

// ─── Graph Loading Skeleton ──────────────────────────────────
function showGraphLoading(show) {
    const skeleton = document.getElementById("graphSkeleton");
    if (skeleton) {
        skeleton.style.display = show ? "flex" : "none";
    }
}

// ─── Zoom Controls ───────────────────────────────────────────
function setupZoomControls() {
    const btnZoomIn = document.getElementById("btnZoomIn");
    const btnZoomOut = document.getElementById("btnZoomOut");
    const btnZoomReset = document.getElementById("btnZoomReset");

    if (btnZoomIn) {
        btnZoomIn.addEventListener("click", () => {
            cy.zoom({ level: cy.zoom() * 1.3, renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 } });
        });
    }
    if (btnZoomOut) {
        btnZoomOut.addEventListener("click", () => {
            cy.zoom({ level: cy.zoom() / 1.3, renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 } });
        });
    }
    if (btnZoomReset) {
        btnZoomReset.addEventListener("click", () => {
            cy.fit(undefined, 40);
        });
    }
}

// ─── Quick Query Chips ───────────────────────────────────────
function setupQuickChips() {
    const chips = document.querySelectorAll(".quick-chip");
    chips.forEach((chip) => {
        chip.addEventListener("click", () => {
            const question = chip.dataset.query;
            if (question) {
                const input = document.getElementById("chatInput");
                input.value = question;
                // Trigger form submit
                document.getElementById("chatForm").dispatchEvent(new Event("submit"));
            }
        });
    });
}

// ─── Chat ────────────────────────────────────────────────────
function setupChat() {
    const form = document.getElementById("chatForm");
    const input = document.getElementById("chatInput");

    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const question = input.value.trim();
        if (!question || isLoading) return;

        addMessage(question, "user");
        input.value = "";
        input.style.height = "auto";
        input.dispatchEvent(new Event("input")); // reset send button
        showLoading();

        try {
            const res = await fetch(`${API_BASE}/api/query`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ question }),
            });

            hideLoading();

            if (!res.ok) {
                const err = await res.json();
                addMessage(err.detail || "An error occurred.", "error");
                return;
            }

            const data = await res.json();
            addMessage(data.answer, "system", data);
        } catch (err) {
            hideLoading();
            addMessage("Failed to connect to the server. Please check that the backend is running.", "error");
        }
    });

    // Escape to clear
    input.addEventListener("keydown", (e) => {
        if (e.key === "Escape") {
            input.value = "";
            input.style.height = "auto";
            input.dispatchEvent(new Event("input"));
        }
    });

    // Auto-resize textarea
    input.addEventListener("input", function() {
        this.style.height = "auto";
        this.style.height = (this.scrollHeight) + "px";
    });
    
    // Enter to submit, Shift+Enter for newline
    input.addEventListener("keydown", function(e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            form.dispatchEvent(new Event("submit"));
        }
    });

    // UI Buttons
    const btnToggleChat = document.getElementById("btnToggleChat");
    if (btnToggleChat) {
        btnToggleChat.addEventListener("click", () => {
            const chatPanel = document.querySelector('.chat-panel');
            chatPanel.classList.toggle('chat-hidden');
            setTimeout(() => { if(cy) cy.resize(); }, 100);
        });
    }

    const btnDarkMode = document.getElementById("btnDarkMode");
    if (btnDarkMode) {
        btnDarkMode.addEventListener("click", toggleDarkMode);
    }

    // Keyboard shortcut: Ctrl+D for dark mode
    document.addEventListener("keydown", (e) => {
        if (e.ctrlKey && e.key === 'd') {
            e.preventDefault();
            toggleDarkMode();
        }
    });

    const btnMinimize = document.getElementById("btnMinimize");
    let controlsMinimized = false;
    if (btnMinimize) {
        btnMinimize.addEventListener("click", () => {
            controlsMinimized = !controlsMinimized;
            document.querySelectorAll(".graph-legend, .graph-search-wrapper, .zoom-controls, .graph-stats-pill").forEach(el => {
                if(el) el.style.display = controlsMinimized ? 'none' : '';
            });
            btnMinimize.innerHTML = controlsMinimized ? 
                `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg> Maximize` :
                `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg> Minimize`;
        });
    }

    const btnOverlay = document.getElementById("btnOverlay");
    if (btnOverlay) {
        btnOverlay.addEventListener("click", () => {
            window.labelsForcedHidden = !window.labelsForcedHidden;
            if(cy) {
                if(window.labelsForcedHidden) {
                    cy.nodes().style("text-opacity", 0);
                    btnOverlay.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg> Show Granular Overlay`;
                } else {
                    const zoom = cy.zoom();
                    cy.nodes().style("text-opacity", zoom > 1.2 ? 1 : 0);
                    btnOverlay.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg> Hide Granular Overlay`;
                }
            }
        });
    }

    const btnNodeIds = document.getElementById("btnNodeIds");
    let showNodeIds = false;
    if (btnNodeIds) {
        btnNodeIds.addEventListener("click", () => {
            showNodeIds = !showNodeIds;
            if(cy) {
                const zoom = cy.zoom();
                cy.nodes().style({
                    "label": showNodeIds ? "data(id)" : "data(label)",
                    "text-opacity": showNodeIds ? 1 : (zoom > 1.2 && !window.labelsForcedHidden ? 1 : 0)
                });
                btnNodeIds.innerHTML = showNodeIds ? 
                    `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg> Show Type Labels` :
                    `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg> Toggle Node IDs`;
            }
        });
    }
}

function addMessage(text, type, extraData = null) {
    const container = document.getElementById("chatMessages");
    const div = document.createElement("div");
    div.className = `message ${type}-message`;

    const now = new Date();
    const timeStr = now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

    let headerHtml = "";
    if (type === "system" || type === "error") {
        headerHtml = `
            <div class="system-header">
                <div class="avatar-ai">D</div>
                <div class="ai-name">Dodge AI <span>Graph Agent</span></div>
                <span class="msg-time">${timeStr}</span>
            </div>
        `;
    } else {
        headerHtml = `
            <div class="user-header">
                <div class="avatar-user"><svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg></div>
                <div class="user-name">You</div>
                <span class="msg-time">${timeStr}</span>
            </div>
        `;
    }

    let contentHtml = "";
    if (extraData && extraData.data && extraData.data.length > 0 && type === "system") {
        contentHtml = renderTabs(extraData);
    } else {
        contentHtml = formatMessageContent(text);
    }

    div.innerHTML = `
        ${headerHtml}
        <div class="message-content">${contentHtml}</div>
    `;

    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

// ─── Chat Tabs ───────────────────────────────────────────────
function renderTabs(data) {
    const tabSuffix = Math.random().toString(36).substr(2, 6);
    
    // Data tree
    let dataHtml = '<div class="data-tree">';
    data.data.forEach((row, i) => {
        dataHtml += `<details class="data-tree-node"><summary>Record ${i + 1}</summary><div class="details-content">`;
        for (const [key, val] of Object.entries(row)) {
            let displayVal = escapeHtml(String(val));
            const kLower = key.toLowerCase();
            if (["order", "delivery", "billing", "document", "customer", "party", "product"].some(x => kLower.includes(x)) && String(val).match(/^\\d+$/)) {
                displayVal = `<a href="#" class="graph-link" data-node-id="${displayVal}">${displayVal}</a>`;
            }
            dataHtml += `<div class="data-row"><span class="data-key">${formatKey(key)}:</span> <span class="data-val">${displayVal}</span></div>`;
        }
        dataHtml += `</div></details>`;
    });
    dataHtml += '</div>';

    const sqlHtml = `<pre class="sql-code"><code>${escapeHtml(data.sql || 'No SQL available.')}</code></pre>`;

    return `
        <div class="chat-tabs-container">
            <div class="chat-tabs-header">
                <button class="chat-tab-btn active" data-target="tab-summary-${tabSuffix}">Summary</button>
                <button class="chat-tab-btn" data-target="tab-data-${tabSuffix}">Data Tree</button>
                <button class="chat-tab-btn" data-target="tab-sql-${tabSuffix}">SQL View</button>
            </div>
            <div class="chat-tabs-body">
                <div class="chat-tab-content active" id="tab-summary-${tabSuffix}">
                    ${formatMessageContent(data.answer)}
                </div>
                <div class="chat-tab-content" id="tab-data-${tabSuffix}">
                    ${dataHtml}
                </div>
                <div class="chat-tab-content" id="tab-sql-${tabSuffix}">
                    ${sqlHtml}
                </div>
            </div>
        </div>
    `;
}

// Global listener for tab switching
document.addEventListener("click", (e) => {
    if (e.target.classList.contains("chat-tab-btn")) {
        const container = e.target.closest(".chat-tabs-container");
        const targetId = e.target.getAttribute("data-target");
        
        // Update buttons
        container.querySelectorAll(".chat-tab-btn").forEach(btn => btn.classList.remove("active"));
        e.target.classList.add("active");
        
        // Update content
        container.querySelectorAll(".chat-tab-content").forEach(content => content.classList.remove("active"));
        const targetContent = document.getElementById(targetId);
        if (targetContent) targetContent.classList.add("active");
    }
});

function showLoading() {
    isLoading = true;
    document.getElementById("btnSend").disabled = true;
    const statusText = document.querySelector(".status-text");
    if (statusText) statusText.textContent = "Dodge AI is thinking...";
    const statusDot = document.querySelector(".status-dot");
    if (statusDot) statusDot.style.background = "#f59e0b";

    const container = document.getElementById("chatMessages");
    const div = document.createElement("div");
    div.className = "message system-message";
    div.id = "loadingMessage";
    div.innerHTML = `
        <div class="system-header">
            <div class="avatar-ai pulse-avatar">D</div>
            <div class="ai-name">Dodge AI <span>Graph Agent</span></div>
        </div>
        <div class="message-content">
            <div class="typing-indicator"><span></span><span></span><span></span></div>
        </div>
    `;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function hideLoading() {
    isLoading = false;
    document.getElementById("btnSend").disabled = false;
    const statusText = document.querySelector(".status-text");
    if (statusText) statusText.textContent = "Dodge AI is awaiting instructions";
    const statusDot = document.querySelector(".status-dot");
    if (statusDot) statusDot.style.background = "#22c55e";
    const loader = document.getElementById("loadingMessage");
    if (loader) loader.remove();
}

// ─── Status ──────────────────────────────────────────────────
async function checkHealth() {
    try {
        const res = await fetch(`${API_BASE}/api/health`);
        if (res.ok) {
            updateStatus(true);
            return true;
        }
    } catch (e) {}
    updateStatus(false);
    return false;
}

function updateStatus(connected) {
    const dot = document.querySelector(".status-dot");
    const text = document.querySelector(".status-text");
    if (!dot || !text) return;
    if (connected) {
        dot.style.background = "#22c55e";
        text.textContent = "Dodge AI is awaiting instructions";
    } else {
        dot.style.background = "#ef4444";
        text.textContent = "Dodge AI is offline";
    }
}

// ─── Graph Status Badge ──────────────────────────────────────
async function fetchGraphStatus() {
    try {
        const res = await fetch(`${API_BASE}/api/graph/status`);
        const data = await res.json();
        const badge = document.getElementById("dbModeBadge");
        if (badge) {
            badge.textContent = data.mode === "neo4j" ? "Neo4j" : "Local DB";
            badge.className = `db-badge ${data.mode === "neo4j" ? "badge-neo4j" : "badge-sqlite"}`;
        }
    } catch (e) {
        const badge = document.getElementById("dbModeBadge");
        if (badge) {
            badge.textContent = "Offline";
            badge.className = "db-badge badge-offline";
        }
    }
}

// ─── Overlay Toggle ──────────────────────────────────────────
function setupOverlayToggle() {
    const btn = document.getElementById("btnOverlay");
    if (!btn) return;

    btn.addEventListener("click", () => {
        overlayVisible = !overlayVisible;
        if (overlayVisible) {
            cy.edges().style("display", "element");
            btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg> Hide Granular Overlay`;
        } else {
            cy.edges().style("display", "none");
            btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg> Show Granular Overlay`;
        }
    });
}

// ─── Utilities ───────────────────────────────────────────────
function truncate(str, maxLen) {
    if (!str) return "";
    str = String(str);
    return str.length > maxLen ? str.slice(0, maxLen) + "…" : str;
}

function formatKey(key) {
    return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function formatMessageContent(text) {
    // Convert markdown-like formatting to HTML
    let html = escapeHtml(text);

    // Bold: **text**
    html = html.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");

    // Bullet points: lines starting with - or *
    html = html.replace(/^[\-\*]\s+(.+)$/gm, "<li>$1</li>");
    html = html.replace(/(<li>.*<\/li>)/gs, "<ul>$1</ul>");
    // Clean up nested <ul> tags
    html = html.replace(/<\/ul>\s*<ul>/g, "");

    // Line breaks
    html = html.replace(/\n/g, "<br>");

    // Clean up excessive <br> before/after lists
    html = html.replace(/<br><ul>/g, "<ul>");
    html = html.replace(/<\/ul><br>/g, "</ul>");

    // special graph id links: [text](id:xxx)
    html = html.replace(/\[([^\]]+)\]\(id:([^\)]+)\)/g, '<a href="#" class="graph-link" data-node-id="$2">$1</a>');

    return html;
}

// ─── Interactive Chat Zoom ─────────────────────────────────────
function zoomToNode(nodeId) {
    if (!cy) return;
    
    // Search visible nodes for matching generic business ID (nodeId)
    // We check `label`, or `properties.salesOrder`, `properties.deliveryDocument`, etc.
    let targetNode = null;
    const query = String(nodeId).toLowerCase();

    cy.nodes().forEach((node) => {
        // Option A: exact match on id string (e.g. if it matches neo4j or local graph ids directly)
        if (node.id().toLowerCase() === query || node.id().toLowerCase().includes("_" + query)) {
            targetNode = node;
            return;
        }
        
        // Option B: match label
        if ((node.data("label") || "").toLowerCase() === query) {
            targetNode = node;
            return;
        }

        // Option C: match properties values
        const props = node.data("properties") || {};
        for (const [key, val] of Object.entries(props)) {
            if (String(val).toLowerCase() === query) {
                targetNode = node;
                return;
            }
        }
    });

    if (targetNode) {
        // Clear existing highlights
        clearSearch(); // Clears any search input filter highlights

        // Apply focus
        cy.nodes().removeClass("highlighted dimmed");
        cy.edges().removeClass("dimmed");
        
        cy.nodes().addClass("dimmed");
        targetNode.removeClass("dimmed").addClass("highlighted");

        // Edge highlighting
        cy.edges().forEach((edge) => {
            const src = edge.source();
            const tgt = edge.target();
            if (src.id() === targetNode.id() || tgt.id() === targetNode.id()) {
                edge.removeClass("dimmed");
                src.removeClass("dimmed");
                tgt.removeClass("dimmed");
            } else {
                edge.addClass("dimmed");
            }
        });

        // Show details
        showNodeDetails(targetNode.data());

        // Animate camera to node
        cy.animate({
            center: { eles: targetNode },
            zoom: 2.2,
            duration: 600
        });

    } else {
        showToast("Node not found in current graph view. Expand nodes or try searching.");
    }
}

function setupChatLinks() {
    const chatContainer = document.getElementById("chatMessages");
    if (!chatContainer) return;

    chatContainer.addEventListener("click", (e) => {
        const link = e.target.closest(".graph-link");
        if (link) {
            e.preventDefault();
            const nodeId = link.getAttribute("data-node-id");
            if (nodeId) {
                zoomToNode(nodeId);
            }
        }
    });
}

// ─── Toast System ────────────────────────────────────────────
function showToast(message) {
    let container = document.getElementById("toastContainer");
    if (!container) {
        container = document.createElement("div");
        container.id = "toastContainer";
        container.className = "toast-container";
        document.body.appendChild(container);
    }

    const toast = document.createElement("div");
    toast.className = "toast";
    toast.textContent = message;
    container.appendChild(toast);

    // Trigger animation
    requestAnimationFrame(() => requestAnimationFrame(() => toast.classList.add("show")));

    setTimeout(() => {
        toast.classList.remove("show");
        toast.addEventListener("transitionend", () => toast.remove());
    }, 3000);
}


// ─── Button Handlers ─────────────────────────────────────────
function setupButtons() {
    const input = document.getElementById("chatInput");
    const sendBtn = document.getElementById("btnSend");
    if (input && sendBtn) {
        input.addEventListener("input", () => {
            if (input.value.trim().length > 0) {
                sendBtn.classList.add("active");
            } else {
                sendBtn.classList.remove("active");
            }
        });
    }
}

// ─── Dark Mode Toggle ────────────────────────────────────────
function toggleDarkMode() {
    document.body.classList.toggle('dark-mode');
    const isDark = document.body.classList.contains('dark-mode');
    
    // Persist preference
    localStorage.setItem('darkMode', isDark ? 'true' : 'false');
    
    // Update icon (moon ↔ sun)
    const btnDarkMode = document.getElementById('btnDarkMode');
    if (btnDarkMode) {
        btnDarkMode.innerHTML = isDark
            ? `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`
            : `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`;
    }
    
    // Update cytoscape node/edge colors
    if (window.cy) {
        window.cy.nodes().style({
            'color': isDark ? '#f8fafc' : '#374151',
            'text-outline-color': isDark ? '#0f172a' : '#ffffff'
        });
        window.cy.edges().style({
            'line-color': isDark ? '#94a3b8' : '#93c5fd',
            'target-arrow-color': isDark ? '#94a3b8' : '#93c5fd'
        });
    }
}

function restoreDarkMode() {
    const saved = localStorage.getItem('darkMode');
    if (saved === 'true') {
        document.body.classList.add('dark-mode');
        // Update icon to sun
        const btnDarkMode = document.getElementById('btnDarkMode');
        if (btnDarkMode) {
            btnDarkMode.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
        }
    }
}

// ─── Initialize ──────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    restoreDarkMode();
    initGraph();
    setupChat();
    setupButtons();
    setupZoomControls();
    setupQuickChips();
    setupGraphSearch();
    setupOverlayToggle();
    buildLegend();
    setupChatLinks();

    const healthy = await checkHealth();
    if (healthy) {
        await loadGraph();
        fetchGraphStatus();
    }

    // Periodically check health
    setInterval(checkHealth, 30000);
});
