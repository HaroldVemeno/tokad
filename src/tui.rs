use crate::tokad::{Data, Node, StateRef, StoreOrNodes};
use std::{io, net::ToSocketAddrs, time::SystemTime};

use futures::{FutureExt, StreamExt};
use tokio::select;
use tokio::sync::mpsc;

use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    widgets::{Clear, Paragraph},
};
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;
use tui_tracing::FormatOptions;

use circular_queue::CircularQueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiMode {
    FullLogs,
    Split,
}

#[derive(Debug)]
pub struct Tui {
    server: StateRef,
    input: Input,
    mode: TuiMode,
    console_history: CircularQueue<String>,
    console_sender: mpsc::Sender<String>,
    console_receiver: mpsc::Receiver<String>,
    traces: tui_tracing::TraceViewer,
}

pub async fn run_tui(
    server: StateRef,
    traces: tui_tracing::TraceViewer,
) -> Result<(), io::Error> {
    let (tx, rx) = mpsc::channel(100);
    let tui = Tui {
        server,
        input: Input::default(),
        mode: TuiMode::Split,
        console_history: CircularQueue::with_capacity(1000),
        console_sender: tx,
        console_receiver: rx,
        traces,
    };

    let mut term = ratatui::init();
    let result = tui.run(&mut term).await;
    ratatui::restore();
    result
}

impl Tui {
    async fn run(mut self, term: &mut DefaultTerminal) -> io::Result<()> {
        let mut event_stream = EventStream::new();
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(200));

        self.traces.set_format_options(FormatOptions {
            show_span_context: true,
            show_target: false,
            show_location: false,
            ..FormatOptions::default()
        });

        let mut last_captured_events = 0;
        let mut should_draw = true;
        loop {
            if should_draw {
                term.draw(|frame| self.render(frame))?;
                let store_status = self.traces.store().status();
                last_captured_events = store_status.captured_events;
                should_draw = false;
            }

            select! {
                _ = ticker.tick() => {
                    let store_status = self.traces.store().status();
                    let current_captured = store_status.captured_events;
                    if current_captured != last_captured_events {
                        should_draw = true;
                    }
                }
                msg = self.console_receiver.recv() => {
                    if let Some(msg) = msg {
                        self.console_history.push(msg);
                        while let Ok(msg) = self.console_receiver.try_recv() {
                            self.console_history.push(msg);
                        }
                        should_draw = true;
                    } else {
                        break Ok(());
                    }
                }
                event = event_stream.next().fuse() => {
                    match event {
                        Some(Ok(event)) => {
                            match event {
                                Event::Key(key) => {
                                    if key.kind == crossterm::event::KeyEventKind::Release {
                                        continue;
                                    }
                                    if key.modifiers == KeyModifiers::CONTROL
                                        && key.code == KeyCode::Char('c') {
                                        return Ok(());
                                    }
                                    if key.code == KeyCode::Tab {
                                        if key.kind != crossterm::event::KeyEventKind::Press {
                                            continue;
                                        }
                                        self.mode = match self.mode {
                                            TuiMode::FullLogs => TuiMode::Split,
                                            TuiMode::Split => TuiMode::FullLogs,
                                        };
                                        should_draw = true;
                                        continue;
                                    }
                                    if key.code == KeyCode::Enter {
                                        if key.kind != crossterm::event::KeyEventKind::Press {
                                            continue;
                                        }
                                        let should_exit = self.enter();
                                        if should_exit { return Ok(()); }
                                        should_draw = true;
                                    } else {
                                        self.input.handle_event(&event);
                                        should_draw = true;
                                    }
                                }
                                Event::Resize(_, _) => {
                                    term.clear()?;
                                    should_draw = true;
                                }
                                _ => {}
                            }
                        }
                        Some(Err(_)) | None => {
                            break Ok(());
                        }
                    }
                }
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let height = frame.area().height;
        let (log_area, console_area_opt, status_area, input_area) = if height <= 1 {
            // Only input area is visible if height is extremely constrained
            (
                ratatui::layout::Rect::default(),
                None,
                ratatui::layout::Rect::default(),
                frame.area(),
            )
        } else if height == 2 {
            // Only status bar and input area are visible
            let chunks: [ratatui::layout::Rect; 2] = Layout::vertical([
                Constraint::Length(1), // Status
                Constraint::Length(1), // Input
            ])
            .areas(frame.area());
            (
                ratatui::layout::Rect::default(),
                None,
                chunks[0],
                chunks[1],
            )
        } else {
            // Normal layout division for height >= 3
            match self.mode {
                TuiMode::FullLogs => {
                    let chunks: [ratatui::layout::Rect; 3] = Layout::vertical([
                        Constraint::Fill(1),
                        Constraint::Length(1),
                        Constraint::Length(1),
                    ])
                    .areas(frame.area());
                    (chunks[0], None, chunks[1], chunks[2])
                }
                TuiMode::Split => {
                    let chunks: [ratatui::layout::Rect; 4] = Layout::vertical([
                        Constraint::Fill(1),
                        Constraint::Fill(1),
                        Constraint::Length(1),
                        Constraint::Length(1),
                    ])
                    .areas(frame.area());
                    (chunks[0], Some(chunks[1]), chunks[2], chunks[3])
                }
            }
        };

        // Render logs if there is height
        if log_area.height > 0 {
            frame.render_widget(&mut self.traces, log_area);
        }

        // Render console area if in Split mode and has height
        if let Some(console_area) = console_area_opt {
            if console_area.height > 0 {
                frame.render_widget(Clear, console_area);
                let rows = console_area.height as usize;
                let width = console_area.width as usize;

                let mut wrapped_lines = Vec::new();
                for s in self.console_history.iter() {
                    let lines = wrap_text(s, width);
                    wrapped_lines.splice(0..0, lines);
                    if wrapped_lines.len() >= rows {
                        break;
                    }
                }

                let start = wrapped_lines.len().saturating_sub(rows);
                let text = ratatui::text::Text::from_iter(
                    wrapped_lines.iter().skip(start)
                        .map(|s| ratatui::text::Line::raw(s.clone())),
                );
                let console_paragraph = Paragraph::new(text);
                frame.render_widget(console_paragraph, console_area);
            }
        }

        // Render status bar if it has height
        if status_area.height > 0 {
            let mode_str = match self.mode {
                TuiMode::FullLogs => "Full Logs",
                TuiMode::Split => "Split Mode",
            };
            let status_width = status_area.width as usize;
            let mut status_text = format!(
                " Node ID: {} | Port: {} | Layout: {} (Press Tab to toggle)",
                self.server.id, self.server.port, mode_str
            );
            if status_text.len() > status_width {
                status_text.truncate(status_width);
            } else {
                status_text.push_str(&" ".repeat(status_width - status_text.len()));
            }

            let status_style = ratatui::style::Style::default()
                .bg(ratatui::style::Color::DarkGray)
                .fg(ratatui::style::Color::White);

            let status_paragraph = Paragraph::new(status_text).style(status_style);
            frame.render_widget(status_paragraph, status_area);
        }

        // Render input area if it has height
        if input_area.height > 0 {
            let mut input_area = input_area;
            if input_area.width > 1 {
                input_area.width -= 1; // Prevent writing to bottom-right cell to avoid auto-scrolling
            }
            frame.render_widget(Clear, input_area);
            let input_width = input_area.width as usize;
            let scroll = self.input.visual_scroll(input_width);
            let display_value: String = self.input.value().chars().skip(scroll).take(input_width).collect();
            let input_paragraph = Paragraph::new(display_value);
            frame.render_widget(input_paragraph, input_area);

            let x = self.input.visual_cursor().max(scroll) - scroll;
            frame.set_cursor_position((input_area.x + x as u16, input_area.y));
        }
    }

    fn enter(&mut self) -> bool {
        let cmd = self.input.value_and_reset();
        self.console_history.push(format!("> {}", cmd));
        let words: Vec<&str> = cmd.split_whitespace().collect();
        if words.is_empty() {
            return false;
        }
        match words[0] {
            "quit" | "exit" => {
                return true;
            }
            "clear" => {
                self.console_history.clear();
            }
            "log_level" => {
                let usage = "Usage: log_level off|error|warn|info|debug|trace";
                if words.len() != 2 {
                    self.console_history.push(usage.to_string());
                    return false;
                }
                let filter = match words[1].to_lowercase().as_str() {
                    "off" => Some(tui_tracing::TraceFilter::all().with_target("disable_logs_completely_nonexistent_target")),
                    "error" => Some(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::ERROR)),
                    "warn" => Some(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::WARN)),
                    "info" => Some(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::INFO)),
                    "debug" => Some(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::DEBUG)),
                    "trace" => Some(tui_tracing::TraceFilter::all().with_min_level(tracing::Level::TRACE)),
                    _ => None,
                };
                if let Some(f) = filter {
                    self.traces.set_filter(f);
                    self.console_history.push(format!("Log level set to {}", words[1]));
                } else {
                    self.console_history.push(format!("Unknown log level. {}", usage));
                }
            }

            "ping" => {
                if words.len() != 2 {
                    self.console_history
                        .push("Wrong argument count. Usage: ping <location>".to_string());
                    return false;
                }
                let Ok(mut socks) = words[1]
                    .to_socket_addrs()
                    .or_else(|_| (words[1], 50051).to_socket_addrs())
                else {
                    self.console_history
                        .push("Unparseable location".to_string());
                    return false;
                };
                let Some(sock) = socks.next() else {
                    self.console_history.push("Not resolvable".to_string());
                    return false;
                };
                self.console_history
                    .push(format!("Ping destination: {}", sock));
                let node = Node::from_sock(0, sock);
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        let res = node.ping(server).await;
                        let _ = tx.send(format!("Ping result: {:?}", res)).await;
                    });
                }
            }

            "lookup_node" => {
                if words.len() != 2 {
                    self.console_history
                        .push("Wrong argument count. Usage: lookup_node <id>".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse::<u128>() else {
                    self.console_history.push("Unparseable key".to_string());
                    return false;
                };
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        let res = server.lookup_node(key).await;
                        let _ = tx.send(format!("Lookup result: {:?}", res)).await;
                    });
                }
            }

            "lookup" => {
                if words.len() != 2 {
                    self.console_history
                        .push("Wrong argument count. Usage: lookup <key>".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse::<u128>() else {
                    self.console_history.push("Unparseable key".to_string());
                    return false;
                };
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        match server.lookup_value(key).await {
                            Ok(StoreOrNodes::Store(store)) => {
                                let _ = tx.send(format!("{}", store)).await;
                            }
                            Ok(StoreOrNodes::Nodes(nodes)) => {
                                let node_strings: Vec<String> =
                                    nodes.nodes.iter().map(|n| n.to_string()).collect();
                                let _ = tx.send(node_strings.join(" ")).await;
                            }
                            Err(e) => {
                                let _ = tx.send(format!("Error: {}", e)).await;
                            }
                        }
                    });
                }
            }
            "local_store" => {
                if words.len() != 3 {
                    self.console_history
                        .push("Wrong argument count. Usage: local_store <key> <value>".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.console_history.push("Key not parsable".to_string());
                    return false;
                };
                let value = words[2].bytes().collect();
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        server
                            .store
                            .lock()
                            .unwrap()
                            .insert(key, Data::new(value, SystemTime::now()));
                        let _ = tx.send("Stored".to_string()).await;
                    });
                }
            }
            "raw_store" => {
                if words.len() != 3 {
                    self.console_history
                        .push("Wrong argument count. Usage: raw_store <key> <value>".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.console_history.push("Key not parsable".to_string());
                    return false;
                };
                let value: Vec<u8> = words[2].bytes().collect();
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        let res = server.raw_publish(key, &value).await;
                        let _ = tx.send(format!("Store result: {:?}", res)).await;
                    });
                }
            }
            "store" => {
                if words.len() != 2 {
                    self.console_history
                        .push("Wrong argument count. Usage: store <value>".to_string());
                    return false;
                }

                let value: Vec<u8> = words[1].bytes().collect();
                {
                    let tx = self.console_sender.clone();
                    let server = self.server;
                    tokio::spawn(async move {
                        match server.publish(&value).await {
                            Ok(key) => {
                                let _ = tx.send(format!("Key: {}", key)).await;
                            }
                            Err(e) => {
                                let _ = tx.send(format!("Error: {}", e)).await;
                            }
                        }
                    });
                }
            }
            "print" => {
                if words.len() != 2 {
                    self.console_history.push(
                        "Wrong argument count. Usage: print id|port|buckets|store|start_time"
                            .to_string(),
                    );
                    return false;
                }
                {
                    match words[1] {
                        "id" => {
                            self.console_history.push(format!("id: {}", self.server.id));
                        }
                        "port" => {
                            self.console_history.push(format!("port: {}", self.server.port));
                        }
                        "buckets" => {
                            let tx = self.console_sender.clone();
                            let server = self.server;
                            tokio::spawn(async move {
                                let mut lines = Vec::new();
                                {
                                    let buckets = server.buckets.lock().unwrap();
                                    for (i, bt) in buckets.iter().enumerate() {
                                        if !bt.is_empty() {
                                            let nodes_str = bt
                                                .iter()
                                                .map(|n| n.to_string())
                                                .collect::<Vec<_>>()
                                                .join(" ");
                                            lines.push((i, nodes_str));
                                        }
                                    }
                                }
                                for (i, nodes_str) in lines {
                                    let _ = tx.send(format!("{}: {}", i, nodes_str)).await;
                                }
                            });
                        }
                        "store" => {
                            let tx = self.console_sender.clone();
                            let server = self.server;
                            tokio::spawn(async move {
                                let mut lines = Vec::new();
                                {
                                    let store = server.store.lock().unwrap();
                                    for (k, v) in store.iter() {
                                        if let Ok(s) = std::str::from_utf8(&v.data) {
                                            lines.push(format!("{}: {}", k, s));
                                        } else {
                                            lines.push(format!("{}: {:?}", k, v.data));
                                        }
                                    }
                                }
                                for line in lines {
                                    let _ = tx.send(line).await;
                                }
                            });
                        }
                        "start_time" => {
                            self.console_history
                                .push(format!("start_time: {:?}", self.server.start_time));
                        }
                        _ => {
                            self.console_history.push("Unknown thing to print. Usage: print id|port|buckets|store|start_time".to_string());
                        }
                    }
                }
            }
            _ => {
                self.console_history
                    .push(format!("Unknown command: {}", words[0]));
                self.console_history.push("Commands: quit, clear, log_level, ping, print, lookup, store, lookup_node, local_store, raw_store".to_string());
            }
        }

        false
    }
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![];
    }
    if text.len() <= width && !text.contains('\n') {
        return vec![text.to_string()];
    }
    let mut lines = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            lines.push(String::new());
            continue;
        }
        let mut current_line = String::new();
        for word in line.split(' ') {
            if current_line.is_empty() {
                current_line.push_str(word);
            } else if current_line.len() + 1 + word.len() <= width {
                current_line.push(' ');
                current_line.push_str(word);
            } else {
                lines.push(current_line);
                current_line = word.to_string();
            }
        }
        if !current_line.is_empty() {
            lines.push(current_line);
        }
    }
    lines
}
