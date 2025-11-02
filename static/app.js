const API_BASE = '/api';

let currentPage = 1;
let currentBucketId = 11;
let selectedBucketIds = [11]; // For multi-season selection
let totalPages = 1;
let pageSize = 50;
let comparisonChart = null;
let chartData = null; // Store all season data
let currentStatTab = 'pts_per_rnd'; // Current stat being displayed
let currentSortBy = 'rank';
let currentSortOrder = 'asc';
let isMultiSeasonMode = false;

// Fields that should default to descending (higher is better)
const descDefaultFields = ['pts_per_rnd', 'dpr', 'player_cpi', 'win_pct', 'overall_total', 'total_games', 'rounds_total'];

function getDefaultSortOrder(field) {
    return descDefaultFields.includes(field) ? 'desc' : 'asc';
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Immediately populate seasons dropdown with defaults so it doesn't show "Loading..."
    const defaultSeasons = [11, 10, 9, 8, 7];
    populateSeasonDropdown(defaultSeasons);
    populateSeasonCheckboxes(defaultSeasons);
    
    initializeFilters(); // This will update with actual data from API if successful
    setupEventListeners();
    // Set initial sort to rank (ascending by default)
    currentSortBy = 'rank';
    currentSortOrder = 'asc';
    updateSortIndicators(); // Set initial sort indicator
    loadPlayers();
});

function setupEventListeners() {
    // Remove fetch button - data is fetched automatically weekly
    
    // Add click handlers for sortable column headers
    document.querySelectorAll('.sortable').forEach(header => {
        header.addEventListener('click', () => {
            const sortField = header.getAttribute('data-sort');
            if (currentSortBy === sortField) {
                // Toggle sort order if clicking same column
                currentSortOrder = currentSortOrder === 'asc' ? 'desc' : 'asc';
            } else {
                // New column, use default sort order for that field
                currentSortBy = sortField;
                currentSortOrder = getDefaultSortOrder(sortField);
            }
            currentPage = 1;
            updateSortIndicators();
            loadPlayers();
        });
    });
    
    document.getElementById('bucketSelect').addEventListener('change', (e) => {
        if (!isMultiSeasonMode) {
            currentBucketId = parseInt(e.target.value);
            selectedBucketIds = [currentBucketId];
            currentPage = 1;
            loadPlayers();
            loadFilterOptions();
        }
    });
    
    document.getElementById('multiSeasonToggle').addEventListener('change', (e) => {
        isMultiSeasonMode = e.target.checked;
        const bucketSelect = document.getElementById('bucketSelect');
        const checkboxes = document.getElementById('seasonCheckboxes');
        
        if (isMultiSeasonMode) {
            bucketSelect.style.display = 'none';
            checkboxes.style.display = 'block';
            updateSelectedSeasons();
        } else {
            bucketSelect.style.display = 'block';
            checkboxes.style.display = 'none';
            currentBucketId = parseInt(bucketSelect.value);
            selectedBucketIds = [currentBucketId];
            currentPage = 1;
            loadPlayers();
        }
    });
    document.getElementById('applyFilters').addEventListener('click', () => {
        currentPage = 1;
        loadPlayers();
    });
    document.getElementById('prevPage').addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            loadPlayers();
        }
    });
    document.getElementById('nextPage').addEventListener('click', () => {
        if (currentPage < totalPages) {
            currentPage++;
            loadPlayers();
        }
    });
    
    // Modal close
    document.querySelector('.close').addEventListener('click', () => {
        document.getElementById('chartModal').style.display = 'none';
    });
    
    window.addEventListener('click', (e) => {
        const modal = document.getElementById('chartModal');
        if (e.target === modal) {
            modal.style.display = 'none';
        }
    });
}

async function initializeFilters() {
    await loadFilterOptions();
}

async function loadFilterOptions() {
    try {
        // Add timeout to prevent hanging
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
        
        const response = await fetch(`${API_BASE}/stats/filters?bucket_id=${currentBucketId}`, {
            signal: controller.signal
        }).finally(() => clearTimeout(timeoutId));
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        const stateSelect = document.getElementById('stateFilter');
        const skillSelect = document.getElementById('skillFilter');
        
        // Clear and populate states
        stateSelect.innerHTML = '<option value="">All States</option>';
        data.states.forEach(state => {
            const option = document.createElement('option');
            option.value = state;
            option.textContent = state;
            stateSelect.appendChild(option);
        });
        
        // Clear and populate skill levels
        skillSelect.innerHTML = '<option value="">All Levels</option>';
        data.skill_levels.forEach(skill => {
            const option = document.createElement('option');
            option.value = skill;
            option.textContent = skill;
            skillSelect.appendChild(option);
        });
        
        // Populate season dropdown and checkboxes
        if (data.available_seasons && data.available_seasons.length > 0) {
            populateSeasonDropdown(data.available_seasons);
            populateSeasonCheckboxes(data.available_seasons);
        } else {
            // Fallback: show error or use default seasons if API doesn't return available_seasons
            console.warn('No available_seasons in API response, using defaults');
            const defaultSeasons = [11, 10, 9, 8, 7];
            populateSeasonDropdown(defaultSeasons);
            populateSeasonCheckboxes(defaultSeasons);
        }
    } catch (error) {
        console.error('Error loading filter options:', error);
        // If filter options fail to load, still try to populate seasons from defaults
        if (error.name === 'AbortError') {
            console.warn('Filter options request timed out');
        }
        // Fallback to default seasons if API call fails
        const defaultSeasons = [11, 10, 9, 8, 7];
        populateSeasonDropdown(defaultSeasons);
        populateSeasonCheckboxes(defaultSeasons);
    }
}

function populateSeasonDropdown(seasons) {
    const bucketSelect = document.getElementById('bucketSelect');
    if (!bucketSelect) {
        console.error('bucketSelect element not found');
        return;
    }
    
    bucketSelect.innerHTML = '';
    
    const bucketYearMap = {
        11: "2025-2026",
        10: "2024-2025",
        9: "2023-2024",
        8: "2022-2023",
        7: "2021-2022"
    };
    
    seasons.forEach(bucketId => {
        const yearRange = bucketYearMap[bucketId] || `Season ${bucketId}`;
        const option = document.createElement('option');
        option.value = bucketId;
        option.textContent = `${yearRange} (Season ${bucketId})`;
        bucketSelect.appendChild(option);
    });
    
    // Set current selection
    if (currentBucketId && !isMultiSeasonMode) {
        bucketSelect.value = currentBucketId;
    }
    
    // Show multi-season toggle if we have multiple seasons
    const multiSeasonContainer = document.getElementById('multiSeasonContainer');
    if (seasons.length > 1 && multiSeasonContainer) {
        multiSeasonContainer.style.display = 'block';
    }
}

function populateSeasonCheckboxes(seasons) {
    const container = document.getElementById('seasonCheckboxes');
    if (!container) {
        console.error('seasonCheckboxes container not found');
        return;
    }
    
    container.innerHTML = '';
    
    const bucketYearMap = {
        11: "2025-2026",
        10: "2024-2025",
        9: "2023-2024",
        8: "2022-2023",
        7: "2021-2022"
    };
    
    seasons.forEach(bucketId => {
        const yearRange = bucketYearMap[bucketId] || `Season ${bucketId}`;
        const label = document.createElement('label');
        label.style.display = 'flex';
        label.style.alignItems = 'center';
        label.style.marginBottom = '8px';
        label.style.cursor = 'pointer';
        label.style.color = '#333';
        label.style.fontSize = '0.95rem';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = bucketId;
        checkbox.checked = selectedBucketIds.includes(bucketId);
        checkbox.style.marginRight = '8px';
        checkbox.style.width = '16px';
        checkbox.style.height = '16px';
        checkbox.style.cursor = 'pointer';
        checkbox.addEventListener('change', () => {
            updateSelectedSeasons();
        });
        
        const span = document.createElement('span');
        span.textContent = `${yearRange} (Season ${bucketId})`;
        span.style.userSelect = 'none';
        
        label.appendChild(checkbox);
        label.appendChild(span);
        container.appendChild(label);
    });
}

function updateSelectedSeasons() {
    const checkboxes = document.querySelectorAll('#seasonCheckboxes input[type="checkbox"]');
    selectedBucketIds = Array.from(checkboxes)
        .filter(cb => cb.checked)
        .map(cb => parseInt(cb.value));
    
    if (selectedBucketIds.length > 0) {
        currentPage = 1;
        loadPlayers();
    }
}

function updateSortIndicators() {
    // Clear all indicators
    document.querySelectorAll('.sort-indicator').forEach(ind => {
        ind.textContent = '';
        ind.className = 'sort-indicator';
    });
    
    // Set indicator for current sort column
    const activeHeader = document.querySelector(`[data-sort="${currentSortBy}"]`);
    if (activeHeader) {
        const indicator = activeHeader.querySelector('.sort-indicator');
        if (indicator) {
            indicator.textContent = currentSortOrder === 'asc' ? '↑' : '↓';
            indicator.className = `sort-indicator ${currentSortOrder}`;
        }
    }
}

async function loadPlayers() {
    showLoading(true);
    hideError();
    
    try {
        let url, params;
        
        if (isMultiSeasonMode && selectedBucketIds.length > 1) {
            // Multi-season mode
            params = new URLSearchParams({
                page: currentPage,
                page_size: pageSize,
                sort_by: currentSortBy,
                sort_order: currentSortOrder
            });
            
            // Add all selected bucket_ids
            selectedBucketIds.forEach(id => {
                params.append('bucket_ids', id);
            });
            
            url = `${API_BASE}/players/multi-season?${params}`;
        } else {
            // Single season mode
            params = new URLSearchParams({
                bucket_id: currentBucketId,
                page: currentPage,
                page_size: pageSize,
                sort_by: currentSortBy,
                sort_order: currentSortOrder
            });
            
            url = `${API_BASE}/players?${params}`;
        }
        
        const search = document.getElementById('searchInput').value.trim();
        if (search) params.append('search', search);
        
        const state = document.getElementById('stateFilter').value;
        if (state) params.append('state', state);
        
        const skill = document.getElementById('skillFilter').value;
        if (skill) params.append('skill_level', skill);
        
        // Rebuild URL with updated params
        if (isMultiSeasonMode && selectedBucketIds.length > 1) {
            params = new URLSearchParams({
                page: currentPage,
                page_size: pageSize,
                sort_by: currentSortBy,
                sort_order: currentSortOrder
            });
            selectedBucketIds.forEach(id => params.append('bucket_ids', id));
            if (search) params.append('search', search);
            if (state) params.append('state', state);
            if (skill) params.append('skill_level', skill);
            url = `${API_BASE}/players/multi-season?${params}`;
        } else {
            params = new URLSearchParams({
                bucket_id: currentBucketId,
                page: currentPage,
                page_size: pageSize,
                sort_by: currentSortBy,
                sort_order: currentSortOrder
            });
            if (search) params.append('search', search);
            if (state) params.append('state', state);
            if (skill) params.append('skill_level', skill);
            url = `${API_BASE}/players?${params}`;
        }
        
        // Add timeout to prevent hanging - longer timeout for multi-season queries
        const controller = new AbortController();
        const timeoutDuration = (isMultiSeasonMode && selectedBucketIds.length > 1) ? 60000 : 10000; // 60s for multi-season, 10s for single
        const timeoutId = setTimeout(() => controller.abort(), timeoutDuration);
        
        const response = await fetch(url, {
            signal: controller.signal
        }).finally(() => clearTimeout(timeoutId));
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        displayPlayers(data.players, isMultiSeasonMode);
        
        totalPages = Math.ceil(data.total / pageSize);
        updatePagination(data.total, data.page, data.page_size);
        
        showLoading(false);
        
    } catch (error) {
        console.error('Error loading players:', error);
        if (error.name === 'AbortError') {
            showError('Request timed out. The database may be locked. Please try again in a moment.');
        } else {
            showError('Error loading players. Make sure data has been fetched first.');
        }
        showLoading(false);
    }
}

function displayPlayers(players, isMultiSeason = false) {
    const tbody = document.getElementById('playersBody');
    tbody.innerHTML = '';
    
    if (players.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" style="text-align: center; padding: 40px;">No players found. Try fetching data first.</td></tr>';
        return;
    }
    
    players.forEach(player => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${player.rank || 'N/A'}</td>
            <td><strong>${player.first_name} ${player.last_name}</strong></td>
            <td>${player.state || 'N/A'}</td>
            <td>${player.skill_level || 'N/A'}</td>
            <td>${formatNumber(player.pts_per_rnd)}</td>
            <td>${formatNumber(player.dpr)}</td>
            <td>${formatNumber(player.player_cpi)}</td>
            <td>${formatPercent(player.win_pct)}</td>
            <td>${player.total_games || 0}</td>
            <td>${player.rounds_total || 0}</td>
            <td>${formatNumber(player.overall_total)}</td>
            <td>
                <button class="btn btn-small" onclick="showComparison(${player.player_id})">
                    Compare Seasons
                </button>
            </td>
        `;
        tbody.appendChild(row);
    });
}

function formatNumber(value) {
    if (value === null || value === undefined) return 'N/A';
    return typeof value === 'number' ? value.toFixed(2) : value;
}

function formatPercent(value) {
    if (value === null || value === undefined) return 'N/A';
    return typeof value === 'number' ? `${value.toFixed(2)}%` : value;
}

function updatePagination(total, page, pageSize) {
    currentPage = page;
    document.getElementById('pageInfo').textContent = 
        `Page ${page} of ${totalPages} (${total} total players)`;
    document.getElementById('prevPage').disabled = page <= 1;
    document.getElementById('nextPage').disabled = page >= totalPages;
}

function showLoading(show) {
    document.getElementById('loading').style.display = show ? 'block' : 'none';
}

function showError(message) {
    const errorDiv = document.getElementById('error');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}

function hideError() {
    document.getElementById('error').style.display = 'none';
}

// Stat configuration for charts
const statConfigs = {
    'pts_per_rnd': {
        label: 'PPR (Points Per Round)',
        color: 'rgb(75, 192, 192)',
        yAxisTitle: 'Points Per Round',
        format: (v) => v !== null ? v.toFixed(2) : 'No data'
    },
    'dpr': {
        label: 'DPR (Differential Per Round)',
        color: 'rgb(255, 99, 132)',
        yAxisTitle: 'Differential Per Round',
        format: (v) => v !== null ? v.toFixed(2) : 'No data'
    },
    'player_cpi': {
        label: 'CPI',
        color: 'rgb(54, 162, 235)',
        yAxisTitle: 'CPI',
        format: (v) => v !== null ? v.toFixed(2) : 'No data'
    },
    'win_pct': {
        label: 'Win Percentage',
        color: 'rgb(255, 206, 86)',
        yAxisTitle: 'Win Percentage (%)',
        format: (v) => v !== null ? `${v.toFixed(2)}%` : 'No data',
        max: 100,
        min: 0
    },
    'rank': {
        label: 'Rank',
        color: 'rgb(153, 102, 255)',
        yAxisTitle: 'Rank (Lower = Better)',
        format: (v) => v !== null ? `Rank: ${v}` : 'No data',
        reverse: true
    }
};

function renderChart(statKey) {
    if (!chartData || chartData.seasons.length === 0) return;
    
    const config = statConfigs[statKey];
    if (!config) return;
    
    // Destroy existing chart
    if (comparisonChart) {
        comparisonChart.destroy();
    }
    
    const ctx = document.getElementById('comparisonChart').getContext('2d');
    const labels = chartData.seasons.map(s => `Season ${s.bucket_id}`);
    const data = chartData.seasons.map(s => {
        if (statKey === 'pts_per_rnd') return s.pts_per_rnd;
        if (statKey === 'dpr') return s.dpr;
        if (statKey === 'rank') return s.rank;
        if (statKey === 'win_pct') return s.win_pct;
        if (statKey === 'player_cpi') return s.player_cpi;
        return null;
    });
    
    const chartType = statKey === 'win_pct' ? 'bar' : 'line';
    
    comparisonChart = new Chart(ctx, {
        type: chartType,
        data: {
            labels: labels,
            datasets: [{
                label: config.label,
                data: data,
                borderColor: config.color,
                backgroundColor: statKey === 'win_pct' 
                    ? config.color.replace('rgb', 'rgba').replace(')', ', 0.7)')
                    : config.color.replace('rgb', 'rgba').replace(')', ', 0.1)'),
                borderWidth: 3,
                tension: chartType === 'line' ? 0.3 : 0,
                pointRadius: chartType === 'line' ? 8 : 0,
                pointHoverRadius: chartType === 'line' ? 10 : 0,
                fill: statKey === 'rank'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${config.label}: ${config.format(context.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    reverse: config.reverse || false,
                    beginAtZero: statKey === 'win_pct' || statKey === 'rank',
                    min: config.min,
                    max: config.max,
                    title: {
                        display: true,
                        text: config.yAxisTitle
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        precision: statKey === 'win_pct' ? 0 : 2
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

async function showComparison(playerId) {
    // Get all available seasons (8, 9, 10, 11)
    const bucketIds = [8, 9, 10, 11];
    
    try {
        const params = new URLSearchParams();
        bucketIds.forEach(id => params.append('bucket_ids', id));
        
        // Add timeout
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000);
        
        const response = await fetch(`${API_BASE}/players/${playerId}/comparison?${params}`, {
            signal: controller.signal
        }).finally(() => clearTimeout(timeoutId));
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.seasons.length === 0) {
            alert('No data available for comparison across seasons.');
            return;
        }
        
        document.getElementById('chartTitle').textContent = 
            `${data.first_name} ${data.last_name} - Season Comparison (${data.seasons.length} seasons)`;
        
        const modal = document.getElementById('chartModal');
        modal.style.display = 'block';
        
        // Sort seasons by bucket_id and store data
        data.seasons.sort((a, b) => a.bucket_id - b.bucket_id);
        chartData = data;
        currentStatTab = 'pts_per_rnd'; // Start with PPR
        
        // Setup tab click handlers (remove old listeners first to avoid duplicates)
        document.querySelectorAll('.tab-button').forEach(btn => {
            // Clone and replace to remove old listeners
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);
            
            newBtn.addEventListener('click', () => {
                // Update active tab
                document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
                newBtn.classList.add('active');
                
                // Switch chart
                currentStatTab = newBtn.getAttribute('data-stat');
                renderChart(currentStatTab);
            });
        });
        
        // Set initial active tab (PPR)
        document.querySelectorAll('.tab-button').forEach(btn => {
            btn.classList.remove('active');
            if (btn.getAttribute('data-stat') === 'pts_per_rnd') {
                btn.classList.add('active');
            }
        });
        
        // Render initial chart (PPR)
        renderChart(currentStatTab);
        
    } catch (error) {
        console.error('Error loading comparison:', error);
        if (error.name === 'AbortError') {
            alert('Request timed out. Please try again.');
        } else {
            alert('Error loading comparison data. Player may not have data for multiple seasons.');
        }
    }
}

// Make showComparison available globally
window.showComparison = showComparison;

