use std::time::{Duration, Instant};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, BorderType, Gauge, Paragraph, Tabs, Table, Row, Cell, List, ListItem, Wrap},
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

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum AppTab {
    Setup,
    Trackers,
    Overview,
    Peers,
    Files,
    Logs,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum PieceStatus {
    Missing,
    Downloading,
    Complete,
}

pub struct PeerInfo {
    pub ip: String,
    pub down_speed: f64,
    pub up_speed: f64,
    pub progress: f64,
    pub total_downloaded: u64,
    pub total_uploaded: u64,
}

use tokio::sync::mpsc;

#[derive(Debug)]
pub enum UiUpdate {
    DownloadedBytes(u64),
    UploadedBytes(u64),
    PieceStatus(u32, PieceStatus),
    ActivePeers(isize), // +1 or -1
    PeerStats {
        ip: String,
        downloaded_delta: u64,
        uploaded_delta: u64,
        progress: f64,
    },
    Log(String),
    Init {
        torrent_name: String,
        total_length: u64,
        num_pieces: u32,
        file_names: Vec<String>,
    },
    TrackersQueried(Vec<String>),
    SetupError(String),
    StartTimer,
}

/// Snapshot of download state for the UI to read without holding a tokio lock.
pub struct UiState {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub needed_count: usize,
    pub total_pieces: u32,
    pub complete: bool,
    pub active_peers: usize,
    pub active_tab: AppTab,
    pub pieces: Vec<PieceStatus>,
    pub logs: Vec<String>,
    pub peers: Vec<PeerInfo>,
    pub file_names: Vec<String>,
    pub trackers: Vec<String>,
    pub torrent_name: String,
    pub total_length: u64,
    pub setup_error: Option<String>,
}

pub async fn run_ui(
    ui_state: Arc<StdMutex<UiState>>,
    rx: mpsc::Receiver<UiUpdate>,
    setup_tx: mpsc::Sender<(String, String)>,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let token_clone = token.clone();

    let result = tokio::task::spawn_blocking(move || {
        run_ui_blocking(ui_state, rx, setup_tx, token_clone)
    }).await?;

    result
}

fn run_ui_blocking(
    ui_state: Arc<StdMutex<UiState>>,
    mut rx: mpsc::Receiver<UiUpdate>,
    setup_tx: mpsc::Sender<(String, String)>,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut start_time: Option<Instant> = None;
    let mut last_tick = Instant::now();
    let mut last_downloaded = 0u64;
    let mut last_uploaded = 0u64;
    let mut last_peer_downloaded: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut last_peer_uploaded: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut down_speed = 0f64;
    let mut up_speed = 0f64;
    let mut confirm_exit = false;

    let setup_tx = setup_tx.clone();
    let mut input_torrent = String::new();
    let mut input_download = String::new();
    let mut setup_step = 0;

    loop {
        if token.is_cancelled() {
            break;
        }

        // 1. Drain the channel and batch updates
        while let Ok(update) = rx.try_recv() {
            let mut state = ui_state.lock().unwrap();
            match update {
                UiUpdate::DownloadedBytes(b) => state.downloaded_bytes = b,
                UiUpdate::UploadedBytes(b) => state.uploaded_bytes = b,
                UiUpdate::PieceStatus(idx, status) => {
                    if (idx as usize) < state.pieces.len() {
                        state.pieces[idx as usize] = status;
                    }
                    if status == PieceStatus::Complete {
                        state.needed_count = state.pieces.iter().filter(|&&p| p != PieceStatus::Complete).count();
                        state.complete = state.needed_count == 0;
                    }
                }
                UiUpdate::ActivePeers(delta) => {
                    state.active_peers = (state.active_peers as isize + delta).max(0) as usize;
                }
                UiUpdate::PeerStats { ip, downloaded_delta, uploaded_delta, progress } => {
                    if let Some(p) = state.peers.iter_mut().find(|p| p.ip == ip) {
                        p.total_downloaded += downloaded_delta;
                        p.total_uploaded += uploaded_delta;
                        p.progress = progress;
                    } else {
                        state.peers.push(PeerInfo {
                            ip,
                            down_speed: 0.0,
                            up_speed: 0.0,
                            progress,
                            total_downloaded: downloaded_delta,
                            total_uploaded: uploaded_delta,
                        });
                    }
                }
                UiUpdate::Log(msg) => {
                    state.logs.push(msg);
                }
                UiUpdate::Init { torrent_name, total_length, num_pieces, file_names } => {
                    state.torrent_name = torrent_name;
                    state.total_length = total_length;
                    state.total_pieces = num_pieces;
                    state.needed_count = num_pieces as usize;
                    state.pieces = vec![PieceStatus::Missing; num_pieces as usize];
                    state.file_names = file_names;
                }
                UiUpdate::TrackersQueried(trackers) => {
                    state.trackers = trackers;
                }
                UiUpdate::SetupError(err) => {
                    state.setup_error = Some(err);
                    state.active_tab = AppTab::Setup;
                    setup_step = 0;
                }
                UiUpdate::StartTimer => {
                    start_time = Some(Instant::now());
                }
            }
        }

        let (downloaded, uploaded, needed, total_p, complete, peers_count, active_tab, total_length) = {
            let state = ui_state.lock().unwrap();
            (
                state.downloaded_bytes,
                state.uploaded_bytes,
                state.needed_count,
                state.total_pieces,
                state.complete,
                state.active_peers,
                state.active_tab,
                state.total_length,
            )
        };

        if complete && active_tab == AppTab::Overview {
             // We don't break immediately anymore, we want to see the 100%
        }

        let now = Instant::now();
        let dt = now.duration_since(last_tick).as_secs_f64();
        if dt >= 1.0 {
            down_speed = downloaded.saturating_sub(last_downloaded) as f64 / dt;
            up_speed = uploaded.saturating_sub(last_uploaded) as f64 / dt;
            last_downloaded = downloaded;
            last_uploaded = uploaded;

            // Calculate per-peer speeds
            {
                let mut state = ui_state.lock().unwrap();
                for p in state.peers.iter_mut() {
                    let last_down = last_peer_downloaded.get(&p.ip).cloned().unwrap_or(0);
                    let last_up = last_peer_uploaded.get(&p.ip).cloned().unwrap_or(0);
                    
                    p.down_speed = (p.total_downloaded.saturating_sub(last_down)) as f64 / dt;
                    p.up_speed = (p.total_uploaded.saturating_sub(last_up)) as f64 / dt;
                    
                    last_peer_downloaded.insert(p.ip.clone(), p.total_downloaded);
                    last_peer_uploaded.insert(p.ip.clone(), p.total_uploaded);
                }
            }

            last_tick = now;
        }

        let progress = if total_length > 0 {
            (downloaded as f64 / total_length as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let elapsed = if let Some(st) = start_time {
            st.elapsed().as_secs()
        } else {
            0
        };

        terminal.draw(|f| {
            let area = f.area();
            if active_tab == AppTab::Setup {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(12), // Header
                        Constraint::Min(0),     // Content
                        Constraint::Length(1),  // Footer
                    ])
                    .split(area);
                render_header(f, chunks[0]);
                render_setup(f, chunks[1], setup_step, &input_torrent, &input_download, &ui_state);
                render_footer(f, chunks[2], confirm_exit);
            } else {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(12), // Header
                        Constraint::Length(3),  // Tabs
                        Constraint::Min(0),     // Content
                        Constraint::Length(1),  // Footer
                    ])
                    .split(area);

                render_header(f, chunks[0]);
                render_tabs(f, chunks[1], active_tab);

                match active_tab {
                    AppTab::Overview => {
                        let content_chunks = Layout::default()
                            .direction(Direction::Horizontal)
                            .constraints([
                                Constraint::Length(35),
                                Constraint::Min(0),
                            ])
                            .split(chunks[2]);
                        
                        let t_name = ui_state.lock().unwrap().torrent_name.clone();
                        render_stats(f, content_chunks[0], downloaded, uploaded, down_speed, up_speed, peers_count, elapsed, total_length, needed, total_p, progress, &t_name);
                        render_piece_map(f, content_chunks[1], &ui_state);
                    }
                    AppTab::Trackers => render_trackers(f, chunks[2], &ui_state),
                    AppTab::Peers => render_peers(f, chunks[2], &ui_state),
                    AppTab::Files => render_files(f, chunks[2], &ui_state, total_length),
                    AppTab::Logs => render_logs(f, chunks[2], &ui_state),
                    AppTab::Setup => {}
                }

                render_footer(f, chunks[3], confirm_exit);
            }
        })?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if active_tab == AppTab::Setup {
                    match key.code {
                        KeyCode::Char(c) => {
                            if setup_step == 0 {
                                input_torrent.push(c);
                            } else if setup_step == 1 {
                                input_download.push(c);
                            }
                        }
                        KeyCode::Backspace => {
                            if setup_step == 0 {
                                input_torrent.pop();
                            } else if setup_step == 1 {
                                input_download.pop();
                            }
                        }
                        KeyCode::Enter => {
                            if setup_step == 0 {
                                setup_step = 1;
                            } else if setup_step == 1 {
                                let _ = setup_tx.blocking_send((input_torrent.clone(), input_download.clone()));
                                ui_state.lock().unwrap().active_tab = AppTab::Trackers;
                            }
                        }
                        _ => {}
                    }
                } else if confirm_exit {
                    if key.code == KeyCode::Char('1') {
                        token.cancel();
                        break;
                    } else {
                        confirm_exit = false;
                    }
                } else {
                    match key.code {
                        KeyCode::Char('q') => confirm_exit = true,
                        KeyCode::Tab => {
                            let mut state = ui_state.lock().unwrap();
                            state.active_tab = match state.active_tab {
                                AppTab::Setup => AppTab::Setup,
                                AppTab::Trackers => AppTab::Overview,
                                AppTab::Overview => AppTab::Peers,
                                AppTab::Peers => AppTab::Files,
                                AppTab::Files => AppTab::Logs,
                                AppTab::Logs => AppTab::Trackers,
                            };
                        }
                        KeyCode::Char('1') => ui_state.lock().unwrap().active_tab = AppTab::Trackers,
                        KeyCode::Char('2') => ui_state.lock().unwrap().active_tab = AppTab::Overview,
                        KeyCode::Char('3') => ui_state.lock().unwrap().active_tab = AppTab::Peers,
                        KeyCode::Char('4') => ui_state.lock().unwrap().active_tab = AppTab::Files,
                        KeyCode::Char('5') => ui_state.lock().unwrap().active_tab = AppTab::Logs,
                        _ => {}
                    }
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

fn render_header(f: &mut ratatui::Frame, area: Rect) {
    let ascii_art = r#"
 ███████████    ███████    ███████████   █████ █████
░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███ ░░███ ░░███
░   ░███  ░  ███     ░░███ ░███    ░███  ░░███ ███
    ░███    ░███      ░███ ░██████████    ░░█████
    ░███    ░███      ░███ ░███░░░░░███    ███░███
    ░███    ░░███     ███  ░███    ░███   ███ ░░███
    █████    ░░░███████░   █████   █████ █████ █████
   ░░░░░       ░░░░░░░    ░░░░░   ░░░░░ ░░░░░ ░░░░░
"#;
    let header = Paragraph::new(ascii_art)
        .style(Style::default().fg(Color::Cyan))
        .alignment(Alignment::Left);
    f.render_widget(header, area);
}

fn render_tabs(f: &mut ratatui::Frame, area: Rect, active_tab: AppTab) {
    let titles = vec!["[1] Trackers", "[2] Overview", "[3] Peers", "[4] Files", "[5] Logs"];
    let index = match active_tab {
        AppTab::Trackers => 0,
        AppTab::Overview => 1,
        AppTab::Peers => 2,
        AppTab::Files => 3,
        AppTab::Logs => 4,
        AppTab::Setup => 0,
    };

    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::BOTTOM).border_type(BorderType::Plain))
        .select(index)
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    f.render_widget(tabs, area);
}

fn render_stats(
    f: &mut ratatui::Frame,
    area: Rect,
    downloaded: u64,
    uploaded: u64,
    down_speed: f64,
    up_speed: f64,
    peers: usize,
    elapsed: u64,
    total_length: u64,
    needed: usize,
    total_pieces: u32,
    progress: f64,
    torrent_name: &str,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Gauge
            Constraint::Min(0),    // Metrics
        ])
        .split(area);

    let gauge = Gauge::default()
        .block(Block::default().title(torrent_name).borders(Borders::ALL).border_type(BorderType::Rounded))
        .gauge_style(Style::default().fg(Color::Green))
        .use_unicode(true)
        .ratio(progress)
        .label(format!("{:.1}%", progress * 100.0));
    f.render_widget(gauge, chunks[0]);

    let eta_secs = if down_speed > 100.0 {
        (total_length.saturating_sub(downloaded) as f64 / down_speed) as u64
    } else {
        0
    };

    let stats = vec![
        Line::from(vec![Span::styled("Status:      ", Style::default().fg(Color::DarkGray)), Span::styled(if progress >= 1.0 { "Seeding" } else { "Downloading" }, Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))]),
        Line::from(vec![Span::styled("Size:        ", Style::default().fg(Color::DarkGray)), Span::styled(format_size(total_length), Style::default().fg(Color::White))]),
        Line::from(vec![Span::styled("Downloaded:  ", Style::default().fg(Color::DarkGray)), Span::styled(format_size(downloaded), Style::default().fg(Color::Yellow))]),
        Line::from(vec![Span::styled("Uploaded:    ", Style::default().fg(Color::DarkGray)), Span::styled(format_size(uploaded), Style::default().fg(Color::Blue))]),
        Line::from(""),
        Line::from(vec![Span::styled("Down Speed:  ", Style::default().fg(Color::DarkGray)), Span::styled(format!("{}/s", format_size(down_speed as u64)), Style::default().fg(Color::Green))]),
        Line::from(vec![Span::styled("Up Speed:    ", Style::default().fg(Color::DarkGray)), Span::styled(format!("{}/s", format_size(up_speed as u64)), Style::default().fg(Color::Blue))]),
        Line::from(""),
        Line::from(vec![Span::styled("Peers:       ", Style::default().fg(Color::DarkGray)), Span::styled(peers.to_string(), Style::default().fg(Color::White))]),
        Line::from(vec![Span::styled("Pieces:      ", Style::default().fg(Color::DarkGray)), Span::styled(format!("{}/{}", total_pieces - needed as u32, total_pieces), Style::default().fg(Color::White))]),
        Line::from(""),
        Line::from(vec![Span::styled("Elapsed:     ", Style::default().fg(Color::DarkGray)), Span::styled(format_duration(elapsed), Style::default().fg(Color::White))]),
        Line::from(vec![Span::styled("ETA:         ", Style::default().fg(Color::DarkGray)), Span::styled(if eta_secs > 0 { format_duration(eta_secs) } else { "--:--:--".to_string() }, Style::default().fg(Color::Yellow))]),
    ];

    let paragraph = Paragraph::new(stats)
        .block(Block::default().title("Statistics").borders(Borders::ALL).border_type(BorderType::Rounded));
    f.render_widget(paragraph, chunks[1]);
}

fn render_piece_map(f: &mut ratatui::Frame, area: Rect, ui_state: &Arc<StdMutex<UiState>>) {
    let pieces = {
        let state = ui_state.lock().unwrap();
        state.pieces.clone()
    };

    let mut spans = Vec::new();
    for p in pieces {
        let span = match p {
            PieceStatus::Complete => Span::styled("█", Style::default().fg(Color::Green)),
            PieceStatus::Downloading => Span::styled("▒", Style::default().fg(Color::Yellow)),
            PieceStatus::Missing => Span::styled("░", Style::default().fg(Color::DarkGray)),
        };
        spans.push(span);
    }

    let paragraph = Paragraph::new(Line::from(spans))
        .block(Block::default().title("Piece Map").borders(Borders::ALL).border_type(BorderType::Rounded))
        .wrap(Wrap { trim: false });
    f.render_widget(paragraph, area);
}

fn render_peers(f: &mut ratatui::Frame, area: Rect, ui_state: &Arc<StdMutex<UiState>>) {
    let peers = {
        let state = ui_state.lock().unwrap();
        // Since we don't have a real peer list populated yet, we'll show a placeholder
        // or the actual list if it's there.
        state.peers.iter().map(|p| {
            Row::new(vec![
                Cell::from(p.ip.clone()),
                Cell::from(format!("{}/s", format_size(p.down_speed as u64))),
                Cell::from(format!("{}/s", format_size(p.up_speed as u64))),
                Cell::from(format!("{:.1}%", p.progress * 100.0)),
            ])
        }).collect::<Vec<_>>()
    };

    let header = Row::new(vec!["IP / Client", "Down Speed", "Up Speed", "Progress"])
        .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .bottom_margin(1);

    let table = Table::new(peers, [
        Constraint::Percentage(40),
        Constraint::Percentage(20),
        Constraint::Percentage(20),
        Constraint::Percentage(20),
    ])
    .header(header)
    .block(Block::default().title("Active Connections").borders(Borders::ALL).border_type(BorderType::Rounded));
    
    f.render_widget(table, area);
}

fn render_files(f: &mut ratatui::Frame, area: Rect, ui_state: &Arc<StdMutex<UiState>>, _total_size: u64) {
    let files = {
        let state = ui_state.lock().unwrap();
        state.file_names.clone()
    };

    let list_items: Vec<ListItem> = files.iter().map(|name| {
        ListItem::new(Line::from(vec![
            Span::styled("📄 ", Style::default().fg(Color::White)),
            Span::raw(name),
        ]))
    }).collect();

    let list = List::new(list_items)
        .block(Block::default().title("Files").borders(Borders::ALL).border_type(BorderType::Rounded));
    f.render_widget(list, area);
}

fn render_logs(f: &mut ratatui::Frame, area: Rect, ui_state: &Arc<StdMutex<UiState>>) {
    let logs = {
        let state = ui_state.lock().unwrap();
        state.logs.clone()
    };

    let list_items: Vec<ListItem> = logs.iter().rev().take(100).map(|log| {
        ListItem::new(Text::raw(log))
    }).collect();

    let list = List::new(list_items)
        .block(Block::default().title("Event Logs").borders(Borders::ALL).border_type(BorderType::Rounded));
    f.render_widget(list, area);
}

fn render_footer(f: &mut ratatui::Frame, area: Rect, confirm_exit: bool) {
    let text = if confirm_exit {
        Span::styled("Are you sure? [1] Confirm Exit  [Any] Cancel", Style::default().fg(Color::LightRed).add_modifier(Modifier::BOLD))
    } else {
        Span::styled(" [Tab] Cycle Tabs  [1-4] Switch Tab  [q] Quit ", Style::default().fg(Color::DarkGray))
    };
    let p = Paragraph::new(Line::from(text)).alignment(Alignment::Center);
    f.render_widget(p, area);
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_duration(seconds: u64) -> String {
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    let s = seconds % 60;
    if h > 0 {
        format!("{:02}h {:02}m {:02}s", h, m, s)
    } else {
        format!("{:02}m {:02}s", m, s)
    }
}

fn render_setup(f: &mut ratatui::Frame, area: Rect, step: u8, torrent_path: &str, download_path: &str, ui_state: &Arc<StdMutex<UiState>>) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(" Setup ");

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .margin(2)
        .split(inner_area);

    let t_style = if step == 0 { Style::default().fg(Color::Yellow) } else { Style::default().fg(Color::DarkGray) };
    let d_style = if step == 1 { Style::default().fg(Color::Yellow) } else { Style::default().fg(Color::DarkGray) };

    let torrent_input = Paragraph::new(if step == 0 { format!("{}█", torrent_path) } else { torrent_path.to_string() })
        .block(Block::default().title(" Torrent File Path ").borders(Borders::ALL))
        .style(t_style);

    let download_input = Paragraph::new(if step == 1 { format!("{}█", download_path) } else { download_path.to_string() })
        .block(Block::default().title(" Download Directory ").borders(Borders::ALL))
        .style(d_style);

    f.render_widget(torrent_input, chunks[0]);
    f.render_widget(download_input, chunks[1]);
    
    let state = ui_state.lock().unwrap();
    if let Some(err) = &state.setup_error {
        let err_msg = Paragraph::new(format!("Error: {}", err))
            .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center);
        f.render_widget(err_msg, chunks[2]);
    } else {
        let instructions = Paragraph::new("Press Enter to continue, Backspace to delete")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        f.render_widget(instructions, chunks[2]);
    }
}

fn render_trackers(f: &mut ratatui::Frame, area: Rect, ui_state: &Arc<StdMutex<UiState>>) {
    let state = ui_state.lock().unwrap();
    
    let block = Block::default()
        .title(" Trackers ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded);
        
    if state.trackers.is_empty() {
        let msg = Paragraph::new("\n\nQuerying trackers... Please wait a few moments.")
            .style(Style::default().fg(Color::Yellow))
            .alignment(Alignment::Center)
            .block(block);
        f.render_widget(msg, area);
    } else {
        let items: Vec<ListItem> = state.trackers.iter()
            .map(|t| ListItem::new(Line::from(vec![Span::raw(t.clone())])))
            .collect();
            
        let list = List::new(items)
            .block(block)
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
            
        f.render_widget(list, area);
    }
}
