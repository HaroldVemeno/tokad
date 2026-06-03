use crate::tokad::{Data, Node, StateRef, StoreOrNodes};
use std::{io, net::ToSocketAddrs, time::SystemTime};

use futures::{FutureExt, StreamExt};
use tokio::select;
use tokio::sync::mpsc;

use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    widgets::Paragraph,
};
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;
use tui_tracing::FormatOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiMode {
    FullLogs,
    Split,
}

#[derive(Debug)]
pub struct Tui {
    server: Option<StateRef>,
    input: Input,
    mode: TuiMode,
    console_history: Vec<String>,
    console_sender: mpsc::Sender<String>,
    console_receiver: mpsc::Receiver<String>,
    traces: tui_tracing::TraceViewer,
}

pub async fn run_tui(
    server: Option<StateRef>,
    traces: tui_tracing::TraceViewer,
) -> Result<(), io::Error> {
    let (tx, rx) = mpsc::channel(100);
    let tui = Tui {
        server,
        input: Input::default(),
        mode: TuiMode::Split,
        console_history: Vec::new(),
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
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(66));

        self.traces.set_format_options(FormatOptions {
            show_span_context: true,
            show_target: false,
            show_location: false,
            ..FormatOptions::default()
        });

        loop {
            term.draw(|frame| self.render(frame))?;

            select! {
                _ = ticker.tick() => {}
                Some(msg) = self.console_receiver.recv() => {
                    self.console_history.push(msg);
                }
                Some(Ok(event)) = event_stream.next().fuse() => {
                    if let Event::Key(key) = event {
                        if key.code == KeyCode::Tab {
                            self.mode = match self.mode {
                                TuiMode::FullLogs => TuiMode::Split,
                                TuiMode::Split => TuiMode::FullLogs,
                            };
                            continue;
                        }
                        if key.code == KeyCode::Enter {
                            let should_exit = self.enter();
                            if should_exit { return Ok(()); }
                        }
                        if key.modifiers == KeyModifiers::CONTROL
                            && key.code == KeyCode::Char('c') {
                            return Ok(());
                        }
                        self.input.handle_event(&event);
                    }
                }
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let areas = match self.mode {
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
        };

        let (log_area, console_area_opt, status_area, input_area) = areas;

        // Render logs using tui-tracing
        frame.render_widget(&mut self.traces, log_area);

        // Render console area if in Split mode
        if let Some(console_area) = console_area_opt {
            let rows = console_area.height as usize;
            let start = self.console_history.len().saturating_sub(rows);
            let text = ratatui::text::Text::from_iter(
                self.console_history[start..]
                    .iter()
                    .map(|s| ratatui::text::Line::raw(s.clone())),
            );
            let console_paragraph =
                Paragraph::new(text).wrap(ratatui::widgets::Wrap { trim: true });
            frame.render_widget(console_paragraph, console_area);
        }

        // Render status bar (clean, borderless status bar)
        let mode_str = match self.mode {
            TuiMode::FullLogs => "Full Logs",
            TuiMode::Split => "Split Mode",
        };
        let status_text = if let Some(server) = self.server {
            format!(
                " Node ID: {} | Port: {} | Layout: {} (Press Tab to toggle)",
                server.id, server.port, mode_str
            )
        } else {
            format!(
                " Server is dead | Layout: {} (Press Tab to toggle)",
                mode_str
            )
        };

        let status_style = ratatui::style::Style::default()
            .bg(ratatui::style::Color::DarkGray)
            .fg(ratatui::style::Color::White);

        let status_paragraph = Paragraph::new(status_text).style(status_style);
        frame.render_widget(status_paragraph, status_area);

        // Render input area
        frame.render_widget(self.input.value(), input_area);

        let scroll = self.input.visual_scroll(input_area.width as usize);
        let x = self.input.visual_cursor().max(scroll) - scroll;
        frame.set_cursor_position((input_area.x + x as u16, input_area.y));
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
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
                    tokio::spawn(async move {
                        let res = node.ping(server).await;
                        let _ = tx.send(format!("Ping result: {:?}", res)).await;
                    });
                } else {
                    self.console_history.push("Server is dead".to_string());
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
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
                    tokio::spawn(async move {
                        let res = server.lookup_node(key).await;
                        let _ = tx.send(format!("Lookup result: {:?}", res)).await;
                    });
                } else {
                    self.console_history.push("Server is dead".to_string());
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
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
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
                } else {
                    self.console_history.push("Server is dead".to_string());
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
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
                    tokio::spawn(async move {
                        server
                            .store
                            .lock()
                            .await
                            .insert(key, Data::new(value, SystemTime::now()));
                        let _ = tx.send("Stored".to_string()).await;
                    });
                } else {
                    self.console_history.push("Server is dead".to_string());
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
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
                    tokio::spawn(async move {
                        let res = server.raw_publish(key, &value).await;
                        let _ = tx.send(format!("Store result: {:?}", res)).await;
                    });
                } else {
                    self.console_history.push("Server is dead".to_string());
                }
            }
            "store" => {
                if words.len() != 2 {
                    self.console_history
                        .push("Wrong argument count. Usage: store <value>".to_string());
                    return false;
                }

                let value: Vec<u8> = words[1].bytes().collect();
                if let Some(server) = self.server {
                    let tx = self.console_sender.clone();
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
                } else {
                    self.console_history.push("Server is dead".to_string());
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
                if let Some(server) = self.server {
                    match words[1] {
                        "id" => {
                            self.console_history.push(format!("id: {}", server.id));
                        }
                        "port" => {
                            self.console_history.push(format!("port: {}", server.port));
                        }
                        "buckets" => {
                            let tx = self.console_sender.clone();
                            tokio::spawn(async move {
                                let buckets = server.buckets.lock().await;
                                for (i, bt) in buckets.iter().enumerate() {
                                    if !bt.is_empty() {
                                        let nodes_str = bt
                                            .iter()
                                            .map(|n| n.to_string())
                                            .collect::<Vec<_>>()
                                            .join(" ");
                                        let _ = tx.send(format!("{}: {}", i, nodes_str)).await;
                                    }
                                }
                            });
                        }
                        "store" => {
                            let tx = self.console_sender.clone();
                            tokio::spawn(async move {
                                let store = server.store.lock().await;
                                for (k, v) in store.iter() {
                                    if let Ok(s) = std::str::from_utf8(&v.data) {
                                        let _ = tx.send(format!("{}: {}", k, s)).await;
                                    } else {
                                        let _ = tx.send(format!("{}: {:?}", k, v.data)).await;
                                    }
                                }
                            });
                        }
                        "start_time" => {
                            self.console_history
                                .push(format!("start_time: {:?}", server.start_time));
                        }
                        _ => {
                            self.console_history.push("Unknown thing to print. Usage: print id|port|buckets|store|start_time".to_string());
                        }
                    }
                } else {
                    self.console_history.push("Server is dead".to_string());
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
