const API_BASE = '/api';

let player1SearchTimeout = null;
let player2SearchTimeout = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    setupPlayerSearch('player1Search', 'player1Id', 'player1Autocomplete');
    setupPlayerSearch('player2Search', 'player2Id', 'player2Autocomplete');
});

function setupPlayerSearch(inputId, hiddenId, autocompleteId) {
    const input = document.getElementById(inputId);
    const hidden = document.getElementById(hiddenId);
    const autocomplete = document.getElementById(autocompleteId);
    
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
                            hidden.value = player.player_id;
                            autocomplete.style.display = 'none';
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

async function searchHeadToHead(event) {
    event.preventDefault();
    
    const player1Id = document.getElementById('player1Id').value;
    const player2Id = document.getElementById('player2Id').value;
    
    if (!player1Id || !player2Id) {
        alert('Please select both players');
        return;
    }
    
    if (player1Id === player2Id) {
        alert('Please select two different players');
        return;
    }
    
    const loading = document.getElementById('loading');
    const error = document.getElementById('error');
    const noResults = document.getElementById('noResults');
    const content = document.getElementById('h2hContent');
    const summary = document.getElementById('h2hSummary');
    const gamesList = document.getElementById('gamesList');
    
    loading.style.display = 'block';
    error.style.display = 'none';
    noResults.style.display = 'none';
    content.style.display = 'none';
    summary.innerHTML = '';
    gamesList.innerHTML = '';
    
    try {
        const response = await fetch(`${API_BASE}/head-to-head/${player1Id}/${player2Id}`);
        const data = await response.json();
        
        if (data.total_games === 0) {
            loading.style.display = 'none';
            noResults.style.display = 'block';
            return;
        }
        
        // Render summary
        const player1WinPct = data.player1_win_pct.toFixed(1);
        const player2WinPct = data.player2_win_pct.toFixed(1);
        
        summary.innerHTML = `
            <div class="h2h-players">
                <div class="h2h-player">
                    <div class="h2h-player-name">${data.player1_name}</div>
                    <div style="color: #666; font-size: 0.9rem;">Player ID: ${data.player1_id}</div>
                </div>
                <div class="vs-divider">VS</div>
                <div class="h2h-player">
                    <div class="h2h-player-name">${data.player2_name}</div>
                    <div style="color: #666; font-size: 0.9rem;">Player ID: ${data.player2_id}</div>
                </div>
            </div>
            <div class="h2h-stats">
                <div class="h2h-stat">
                    <div class="h2h-stat-label">Total Games</div>
                    <div class="h2h-stat-value">${data.total_games}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player1_name} Wins</div>
                    <div class="h2h-stat-value ${data.player1_wins > data.player2_wins ? 'winner' : (data.player1_wins < data.player2_wins ? 'loser' : '')}">${data.player1_wins}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player2_name} Wins</div>
                    <div class="h2h-stat-value ${data.player2_wins > data.player1_wins ? 'winner' : (data.player2_wins < data.player1_wins ? 'loser' : '')}">${data.player2_wins}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player1_name} Win %</div>
                    <div class="h2h-stat-value ${data.player1_win_pct > data.player2_win_pct ? 'winner' : (data.player1_win_pct < data.player2_win_pct ? 'loser' : '')}">${player1WinPct}%</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player2_name} Win %</div>
                    <div class="h2h-stat-value ${data.player2_win_pct > data.player1_win_pct ? 'winner' : (data.player2_win_pct < data.player1_win_pct ? 'loser' : '')}">${player2WinPct}%</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player1_name} Total Points</div>
                    <div class="h2h-stat-value">${data.player1_stats.total_points}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player2_name} Total Points</div>
                    <div class="h2h-stat-value">${data.player2_stats.total_points}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player1_name} PPR</div>
                    <div class="h2h-stat-value">${data.player1_stats.ppr}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player2_name} PPR</div>
                    <div class="h2h-stat-value">${data.player2_stats.ppr}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player1_name} 4-Baggers</div>
                    <div class="h2h-stat-value">${data.player1_stats.total_four_baggers}</div>
                </div>
                <div class="h2h-stat">
                    <div class="h2h-stat-label">${data.player2_name} 4-Baggers</div>
                    <div class="h2h-stat-value">${data.player2_stats.total_four_baggers}</div>
                </div>
            </div>
        `;
        
        // Render games list
        if (data.games && data.games.length > 0) {
            const gamesHeader = document.createElement('h2');
            gamesHeader.textContent = 'Game History';
            gamesHeader.style.marginBottom = '20px';
            gamesList.appendChild(gamesHeader);
            
            data.games.forEach(game => {
                const gameItem = document.createElement('div');
                gameItem.className = 'game-item';
                gameItem.onclick = () => {
                    window.location.href = `/games/${game.game_id}`;
                };
                
                const isPlayer1Winner = game.winner_id === data.player1_id;
                const isPlayer2Winner = game.winner_id === data.player2_id;
                
                gameItem.innerHTML = `
                    <div class="game-item-header">
                        <div class="game-item-players">
                            <div>
                                <div style="font-weight: 600; ${isPlayer1Winner ? 'color: #28a745;' : ''}">
                                    ${data.player1_name}
                                </div>
                                <div style="font-size: 0.85rem; color: #666;">
                                    ${game.player1_ppr ? game.player1_ppr.toFixed(2) : 'N/A'} PPR, ${game.player1_rounds} rounds
                                </div>
                            </div>
                            <div style="font-size: 1.2rem; font-weight: 600; color: #666;">vs</div>
                            <div>
                                <div style="font-weight: 600; ${isPlayer2Winner ? 'color: #28a745;' : ''}">
                                    ${data.player2_name}
                                </div>
                                <div style="font-size: 0.85rem; color: #666;">
                                    ${game.player2_ppr ? game.player2_ppr.toFixed(2) : 'N/A'} PPR, ${game.player2_rounds} rounds
                                </div>
                            </div>
                        </div>
                        <div class="game-item-score">
                            ${game.player1_score !== null && game.player1_score !== undefined ? game.player1_score : (game.player1_points || 0)} - ${game.player2_score !== null && game.player2_score !== undefined ? game.player2_score : (game.player2_points || 0)}
                        </div>
                    </div>
                    <div class="game-item-event">
                        ${game.event_name}
                        ${game.created_at ? ` • ${new Date(game.created_at).toLocaleDateString()}` : ''}
                    </div>
                `;
                
                gamesList.appendChild(gameItem);
            });
        }
        
        loading.style.display = 'none';
        content.style.display = 'block';
    } catch (err) {
        loading.style.display = 'none';
        error.textContent = `Error loading head-to-head stats: ${err.message}`;
        error.style.display = 'block';
    }
}

