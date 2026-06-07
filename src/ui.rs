use std::time::{Duration, Instant};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use tokio_util::sync::CancellationToken;

/// Snapshot of download state for the UI to read without holding a tokio lock.
pub struct UiState {
    pub downloaded_bytes: u64,
    pub needed_count: usize,
    pub total_pieces: u32,
    pub complete: bool,
    pub active_peers: usize,
}

/// Runs the TUI on a dedicated blocking thread so it never starves the tokio
/// runtime.  State is shared via a lightweight `std::sync::Mutex` that the
/// download tasks update after every piece.
pub async fn run_ui(
    ui_state: Arc<StdMutex<UiState>>,
    torrent_name: String,
    total_length: u64,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let token_clone = token.clone();

    // Run ALL terminal I/O on a blocking thread so we never block tokio workers.
    let result = tokio::task::spawn_blocking(move || {
        run_ui_blocking(ui_state, torrent_name, total_length, token_clone)
    }).await?;

    result
}

fn run_ui_blocking(
    ui_state: Arc<StdMutex<UiState>>,
    torrent_name: String,
    total_length: u64,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let start_time = Instant::now();
    let mut last_tick = Instant::now();
    let mut last_downloaded = 0u64;
    let mut speed = 0f64;
    let mut confirm_exit = false;

    loop {
        if token.is_cancelled() {
            break;
        }

        // Read shared state — this is a std::sync::Mutex, so it's instant.
        let (downloaded_bytes, needed_count, total_pieces, complete, active_peers) = {
            let state = ui_state.lock().unwrap();
            (state.downloaded_bytes, state.needed_count, state.total_pieces, state.complete, state.active_peers)
        };

        if complete {
            break;
        }

        let now = Instant::now();
        let dt = now.duration_since(last_tick).as_secs_f64();
        if dt >= 1.0 {
            let downloaded_since_last_tick = downloaded_bytes.saturating_sub(last_downloaded);
            speed = downloaded_since_last_tick as f64 / dt;
            last_downloaded = downloaded_bytes;
            last_tick = now;
        }

        let progress = if total_length > 0 {
            (downloaded_bytes as f64 / total_length as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let elapsed = start_time.elapsed().as_secs();

        let eta_secs = if speed > 0.0 {
            ((total_length.saturating_sub(downloaded_bytes)) as f64 / speed) as u64
        } else {
            0
        };

        terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(9), // ASCII Art
                    Constraint::Length(3), // Progress Bar
                    Constraint::Min(0),    // Stats
                ])
                .split(size);

            // ASCII Art
            let ascii_art = r#" 
 ███████████    ███████    ███████████   █████ █████
░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███ ░░███ ░░███
░   ░███  ░  ███     ░░███ ░███    ░███  ░░███ ███
    ░███    ░███      ░███ ░██████████    ░░█████
    ░███    ░███      ░███ ░███░░░░░███    ███░███
    ░███    ░░███     ███  ░███    ░███   ███ ░░███
    █████    ░░░███████░   █████   █████ █████ █████
   ░░░░░       ░░░░░░░    ░░░░░   ░░░░░ ░░░░░ ░░░░░"#;
            // println!("{:?}", ascii_art.lines().count());
            let title = Paragraph::new(ascii_art)
                .style(Style::default().fg(Color::Cyan))
                .alignment(Alignment::Left);                // lines are getting centered seperately
            f.render_widget(title, chunks[0]);

            // Progress Bar
            let label = format!("{:.2}%", progress * 100.0);
            let gauge = Gauge::default()
                .block(Block::default().title(torrent_name.clone()).borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Green).bg(Color::Black))
                .ratio(progress)
                .label(label);
            f.render_widget(gauge, chunks[1]);

            // Stats
            let speed_mbps = speed / 1_048_576.0;
            let downloaded_mb = downloaded_bytes as f64 / 1_048_576.0;
            let total_mb = total_length as f64 / 1_048_576.0;

            let mut stats_text = vec![
                Line::from(vec![
                    Span::styled("Total Size: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{:.2} MB", total_mb)),
                ]),
                Line::from(vec![
                    Span::styled("Downloaded: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{:.2} MB", downloaded_mb)),
                ]),
                Line::from(vec![
                    Span::styled("Speed: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{:.2} MB/s", speed_mbps)),
                ]),
                Line::from(vec![
                    Span::styled("Active Peers: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{}", active_peers)),
                ]),
                Line::from(vec![
                    Span::styled("Time Elapsed: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{}s", elapsed)),
                ]),
                Line::from(vec![
                    Span::styled("ETA: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{}s", eta_secs)),
                ]),
                Line::from(vec![
                    Span::styled("Pieces Remaining: ", Style::default().fg(Color::Yellow)),
                    Span::raw(format!("{}/{}", needed_count, total_pieces)),
                ]),
            ];

            if confirm_exit {
                stats_text.push(Line::from(vec![
                    Span::styled("Are you sure you want to exit? Press '1' to confirm, or any other key to cancel.", Style::default().fg(Color::LightRed)),
                ]));
            } else {
                stats_text.push(Line::from(vec![
                    Span::styled("Press 'q' to quit", Style::default().fg(Color::DarkGray)),
                ]));
            }

            let stats_paragraph = Paragraph::new(stats_text)
                .block(Block::default().title("Statistics").borders(Borders::ALL))
                .alignment(Alignment::Left);
            f.render_widget(stats_paragraph, chunks[2]);
        })?;

        // Poll for keyboard input — blocking for up to 200ms is fine here
        // because we're on a dedicated OS thread, NOT on a tokio worker.
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if confirm_exit {
                    if key.code == KeyCode::Char('1') {
                        token.cancel();
                        break;
                    } else {
                        confirm_exit = false;
                    }
                } else if key.code == KeyCode::Char('q') {
                    confirm_exit = true;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
