const API_BASE = '/api';

let currentPage = 1;
let pageSize = 50;
let totalPages = 1;
let totalCount = 0;
let player1Id = null;
let player2Id = null;
let eventId = null;
let sortBy = 'date';

let player1SearchTimeout = null;
let player2SearchTimeout = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    setupPlayerSearch('player1Search', 'player1Id');
    setupPlayerSearch('player2Search', 'player2Id');
    
    document.getElementById('pageSize').addEventListener('change', (e) => {
        pageSize = parseInt(e.target.value);
        currentPage = 1;
        loadGames();
    });
    
    document.getElementById('sortBy').addEventListener('change', (e) => {
        sortBy = e.target.value;
        currentPage = 1;
        loadGames();
    });
    
    loadGames();
});

function setupPlayerSearch(inputId, hiddenId) {
    const input = document.getElementById(inputId);
    const hidden = document.getElementById(hiddenId);
    
    // Create autocomplete dropdown
    const autocomplete = document.createElement('div');
    autocomplete.className = 'player-autocomplete';
    autocomplete.id = `${inputId}Autocomplete`;
    input.parentElement.style.position = 'relative';
    input.parentElement.appendChild(autocomplete);
    
    input.addEventListener('input', (e) => {
        const query = e.target.value.trim();
        
        if (query.length < 2) {
            autocomplete.style.display = 'none';
            hidden.value = '';
            return;
        }
        
        clearTimeout(inputId === 'player1Search' ? player1SearchTimeout : player2SearchTimeout);
        
        const timeout = setTimeout(async () => {
            try {
                const response = await fetch(`${API_BASE}/players/search?q=${encodeURIComponent(query)}&limit=10`);
                const data = await response.json();
                
                if (data.players && data.players.length > 0) {
                    autocomplete.innerHTML = '';
                    data.players.forEach(player => {
                        const item = document.createElement('div');
                        item.className = 'player-autocomplete-item';
                        item.textContent = player.name;
                        item.onclick = () => {
                            input.value = player.name;
                            hidden.value = player.player_id.toString();
                            autocomplete.style.display = 'none';
                            console.log(`Selected player: ${player.name} (ID: ${player.player_id})`);
                            // Auto-filter when player is selected
                            currentPage = 1;
                            loadGames();
                        };
                        autocomplete.appendChild(item);
                    });
                    autocomplete.style.display = 'block';
                } else {
                    autocomplete.style.display = 'none';
                }
            } catch (error) {
                console.error('Error searching players:', error);
            }
        }, 300);
        
        if (inputId === 'player1Search') {
            player1SearchTimeout = timeout;
        } else {
            player2SearchTimeout = timeout;
        }
    });
    
    // Hide autocomplete when clicking outside
    document.addEventListener('click', (e) => {
        if (!input.parentElement.contains(e.target)) {
            autocomplete.style.display = 'none';
        }
    });
}

async function loadGames() {
    const loading = document.getElementById('loading');
    const error = document.getElementById('error');
    const gamesList = document.getElementById('gamesList');
    const pagination = document.getElementById('pagination');
    
    loading.style.display = 'block';
    error.style.display = 'none';
    gamesList.innerHTML = '';
    pagination.style.display = 'none';
    
    // Get filter values
    const player1IdInput = document.getElementById('player1Id').value;
    const player2IdInput = document.getElementById('player2Id').value;
    player1Id = player1IdInput && player1IdInput.trim() ? parseInt(player1IdInput) : null;
    player2Id = player2IdInput && player2IdInput.trim() ? parseInt(player2IdInput) : null;
    const eventIdInput = document.getElementById('eventIdFilter').value;
    eventId = eventIdInput && eventIdInput.trim() ? parseInt(eventIdInput) : null;
    
    // Debug logging
    console.log('Filter values:', { player1Id, player2Id, eventId });
    
    try {
        const params = new URLSearchParams({
            page: currentPage,
            page_size: pageSize,
            sort_by: sortBy
        });
        
        if (player1Id) {
            params.append('player1_id', player1Id);
            console.log('Adding player1_id filter:', player1Id);
        }
        if (player2Id) {
            params.append('player2_id', player2Id);
            console.log('Adding player2_id filter:', player2Id);
        }
        if (eventId) params.append('event_id', eventId);
        
        const response = await fetch(`${API_BASE}/games?${params}`);
        const data = await response.json();
        
        if (data.games && data.games.length > 0) {
            data.games.forEach(game => {
                gamesList.appendChild(createGameCard(game));
            });
            
            totalPages = data.pagination.total_pages;
            totalCount = data.pagination.total_count;
            updatePagination();
            pagination.style.display = 'flex';
        } else {
            gamesList.innerHTML = '<div class="loading">No games found</div>';
        }
    } catch (err) {
        error.textContent = `Error loading games: ${err.message}`;
        error.style.display = 'block';
    } finally {
        loading.style.display = 'none';
    }
}

function createGameCard(game) {
    const card = document.createElement('div');
    card.className = 'game-card';
    card.onclick = () => {
        window.location.href = `/games/${game.id}`;
    };
    
    const isPlayer1Winner = game.winner_id === game.player1_id;
    const isPlayer2Winner = game.winner_id === game.player2_id;
    
    card.innerHTML = `
        <div class="game-header">
            <div class="game-players">
                <div>
                    <div class="player-name ${isPlayer1Winner ? 'winner' : (isPlayer2Winner ? 'loser' : '')}">
                        ${game.player1_name || `Player ${game.player1_id}`}
                    </div>
                    <div class="stat-value" style="font-size: 0.9rem; color: #666;">
                        ${game.player1_ppr ? game.player1_ppr.toFixed(2) : 'N/A'} PPR
                    </div>
                </div>
                <div class="player-vs">vs</div>
                <div>
                    <div class="player-name ${isPlayer2Winner ? 'winner' : (isPlayer1Winner ? 'loser' : '')}">
                        ${game.player2_name || `Player ${game.player2_id}`}
                    </div>
                    <div class="stat-value" style="font-size: 0.9rem; color: #666;">
                        ${game.player2_ppr ? game.player2_ppr.toFixed(2) : 'N/A'} PPR
                    </div>
                </div>
            </div>
            <div class="game-score">
                ${game.player1_score !== null && game.player1_score !== undefined ? game.player1_score : (game.player1_points || 0)} - ${game.player2_score !== null && game.player2_score !== undefined ? game.player2_score : (game.player2_points || 0)}
            </div>
        </div>
        <div class="game-stats">
            <div class="stat-item">
                <div class="stat-label">Total Rounds</div>
                <div class="stat-value">${game.total_rounds || ((game.player1_rounds || 0) + (game.player2_rounds || 0))}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Player 1 Rounds</div>
                <div class="stat-value">${game.player1_rounds || 0}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Player 2 Rounds</div>
                <div class="stat-value">${game.player2_rounds || 0}</div>
            </div>
            ${game.combined_cpi ? `
            <div class="stat-item">
                <div class="stat-label">Avg CPI</div>
                <div class="stat-value">${game.combined_cpi.toFixed(1)}</div>
            </div>
            ` : ''}
            <div class="stat-item">
                <div class="stat-label">Player 1 4-Baggers</div>
                <div class="stat-value">${game.player1_four_baggers || 0}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Player 2 4-Baggers</div>
                <div class="stat-value">${game.player2_four_baggers || 0}</div>
            </div>
        </div>
        <div class="game-event">
            ${game.event_name || `Event ${game.event_id}`}
            ${game.bracket_name ? ` - ${game.bracket_name}` : ''}
        </div>
    `;
    
    return card;
}

function updatePagination() {
    const pageInfo = document.getElementById('pageInfo');
    pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${totalCount} total games)`;
    
    document.getElementById('prevPage').disabled = currentPage === 1;
    document.getElementById('nextPage').disabled = currentPage === totalPages;
}

function previousPage() {
    if (currentPage > 1) {
        currentPage--;
        loadGames();
    }
}

function nextPage() {
    if (currentPage < totalPages) {
        currentPage++;
        loadGames();
    }
}

function clearFilters() {
    document.getElementById('player1Search').value = '';
    document.getElementById('player1Id').value = '';
    document.getElementById('player2Search').value = '';
    document.getElementById('player2Id').value = '';
    document.getElementById('eventIdFilter').value = '';
    currentPage = 1;
    loadGames();
}

