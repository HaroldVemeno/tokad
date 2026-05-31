use crate::tokad::{Data, Node, StateRef, StoreOrNodes};
use std::{io, net::ToSocketAddrs};

use futures::{FutureExt, StreamExt, future::OptionFuture};
use tokio::{select, sync::mpsc::Receiver};

use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    text::{Line, Text},
    widgets::{Paragraph, Wrap},
};
use tui_input::Input;
use tui_input::backend::crossterm::EventHandler;

//const FPS: f64 = 60.0;

#[derive(Debug, Default)]
pub struct Console {
    server: Option<StateRef>,
    channel: Option<Receiver<String>>,
    input: Input,
    log: Vec<String>,
}

pub async fn start_console(
    state: Option<StateRef>,
    rcv: Option<Receiver<String>>,
) -> Result<(), io::Error> {
    let con = Console {
        server: state,
        channel: rcv,
        ..Console::default()
    };

    let mut term = ratatui::init();
    let res = con.run(&mut term).await;
    ratatui::restore();
    res
}

impl Console {
    async fn run(mut self, term: &mut DefaultTerminal) -> io::Result<()> {
        let mut event_stream = EventStream::new();
        loop {
            term.draw(|frame| self.render(frame))?;

            let mut log_buf = Vec::with_capacity(64);
            let log_fut: OptionFuture<_> = self
                .channel
                .as_mut()
                .map(|ch| ch.recv_many(&mut log_buf, 64))
                .into();

            select! {
                _ = log_fut => {
                    self.log.append(&mut log_buf);
                }
                Some(Ok(event)) = event_stream.next().fuse() => {
                    if let Event::Key(key) = event {
                        if key.code == KeyCode::Enter && self.enter() {
                            return Ok(());
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
        let [log_area, input_area] =
            Layout::vertical([Constraint::Fill(1), Constraint::Length(1)]).areas(frame.area());

        let rows = log_area.height as usize;
        let start = self.log.len().saturating_sub(rows + 5);
        let text = Text::from_iter(self.log[start..].iter().map(Line::raw));
        let log = Paragraph::new(text).wrap(Wrap { trim: true });
        let log_height = log.line_count(log_area.width);
        frame.render_widget(
            log.scroll((
                log_height.saturating_sub(log_area.height as usize) as u16,
                0,
            )),
            log_area,
        );
        frame.render_widget(self.input.value(), input_area);

        let scroll = self.input.visual_scroll(input_area.width as usize);
        let x = self.input.visual_cursor().max(scroll) - scroll;
        frame.set_cursor_position((input_area.x + x as u16, input_area.y));
    }

    fn enter(&mut self) -> bool {
        let cmd = self.input.value_and_reset();
        self.log.push(format!("> {}", cmd));
        let words: Vec<&str> = cmd.split_whitespace().collect();
        if words.is_empty() {
            return false;
        }
        match words[0] {
            "quit" | "exit" => {
                return true;
            }
            "clear" => {
                self.log.clear();
            }
            "ping" => {
                if words.len() != 2 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }
                let Ok(mut socks) = words[1]
                    .to_socket_addrs()
                    .or_else(|_| (words[1], 50051).to_socket_addrs())
                else {
                    self.log.push("Unparseable location".to_string());
                    return false;
                };
                let Some(sock) = socks.next() else {
                    self.log.push("Not resolvable".to_string());
                    return false;
                };
                self.log.push(format!("{}", sock));
                let node = Node::from_sock(0, sock);
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.log(format!("{:?}", (node.ping(server).await))).await;
                    });
                } else {
                    self.log.push("Server is not available".to_string());
                }
            }

            "lookup_node" => {
                if words.len() != 2 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse::<u32>() else {
                    self.log.push("Unparseable key".to_string());
                    return false;
                };
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server
                            .log(format!(
                                "Lookup result: {:?}",
                                (server.lookup_node(key).await)
                            ))
                            .await;
                    });
                } else {
                    self.log.push("Server is not available".to_string());
                }
            }

            "lookup" => {
                if words.len() != 2 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse::<u32>() else {
                    self.log.push("Unparseable key".to_string());
                    return false;
                };
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        match server.lookup_value(key).await {
                            Ok(StoreOrNodes::Store(store)) => {
                                server.log(format!("{}", store)).await;
                            }
                            Ok(StoreOrNodes::Nodes(nodes)) => {
                                server
                                    .log(
                                        nodes
                                            .nodes
                                            .iter()
                                            .map(|n| n.to_string())
                                            .collect::<Vec<_>>()
                                            .join(" ")
                                            .to_string(),
                                    )
                                    .await;
                            }
                            Err(e) => {
                                server.log(e.to_string()).await;
                            }
                        }
                    });
                } else {
                    self.log.push("Server is not available".to_string());
                }
            }
            "local_store" => {
                if words.len() != 3 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.log.push("Key not parsable".to_string());
                    return false;
                };
                let value = words[2].bytes().collect();
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.store.lock().await.insert(key, Data::new(value));
                        server.log("Stored".to_string()).await;
                    });
                }
            }
            "store" => {
                if words.len() != 3 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.log.push("Key not parsable".to_string());
                    return false;
                };
                let value: Vec<u8> = words[2].bytes().collect();
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server
                            .log(format!("{:?}", server.publish(key, &value).await))
                            .await;
                    });
                }
            }
            "print" => {
                if words.len() != 2 {
                    self.log.push("Wrong argument count".to_string());
                    return false;
                }
                if let Some(server) = self.server {
                    match words[1] {
                        "id" => {
                            self.log.push(format!("id: {}", server.id));
                        }
                        "port" => {
                            self.log.push(format!("port: {}", server.port));
                        }
                        "buckets" => {
                            tokio::spawn(async move { server.log_buckets().await });
                        }
                        "store" => {
                            tokio::spawn(async move { server.log_store().await });
                        }
                        _ => {
                            self.log.push("Unknown thing to print".to_string());
                        }
                    }
                } else {
                    self.log.push("Server is not available".to_string());
                }
            }
            _ => {
                self.log.push("Unknown command".to_string());
            }
        }

        false
    }
}
