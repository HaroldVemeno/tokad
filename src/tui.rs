use std::{io, net::ToSocketAddrs, sync::mpsc::Receiver, time::{Duration, Instant}};
use crate::tokad::{Data, Node, Nodes, StateRef, Store, StoreOrNodes};

use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyModifiers}, layout::{Constraint, Layout}, text::{Line, Text}, widgets::{Paragraph, Wrap}, DefaultTerminal, Frame
};
use tokio::task::{spawn_blocking, JoinHandle};
use tui_input::backend::crossterm::EventHandler;
use tui_input::Input;

pub fn start_console(state: Option<StateRef>, rcv: Option<Receiver<String>>) -> JoinHandle<Result<(), io::Error>> {
    let mut con = Console::default();
    con.server = state;
    con.channel = rcv;
    
    spawn_blocking(move || {
        let mut term = ratatui::init();
        let res = con.run(&mut term);
        ratatui::restore();
        res
    })
}

#[derive(Debug, Default)]
pub struct Console {
    server: Option<StateRef>,
    channel: Option<Receiver<String>>,
    input: Input,
    log: Vec<String>
}

impl Console {
    pub fn push(&mut self, s: impl Into<String>) {
        self.log.push(s.into());
    }

    fn run(mut self, term: &mut DefaultTerminal) -> io::Result<()> {
        let tick_time = Duration::from_millis(100);
        let mut last_tick = Instant::now();
        loop {
            if self.channel.is_some() {
                while let Some(Ok(msg)) = self.channel.as_mut().map(|o| o.try_recv()) {
                    self.push(msg);
                }
            }

            term.draw(|frame| self.render(frame))?;

            let timeout = tick_time.saturating_sub(last_tick.elapsed());
            if event::poll(timeout)? {
                let event = event::read()?;
                if let Event::Key(key) = event {
                    if key.code == KeyCode::Enter
                        && self.enter() {
                            return Ok(());
                        }
                    if key.modifiers == KeyModifiers::CONTROL
                        && key.code == KeyCode::Char('c') {
                            return Ok(());
                        }
                    self.input.handle_event(&event);
                }
            } else {
                last_tick = Instant::now();
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let [log_area, input_area] = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(1),
        ]).areas(frame.area());

        let text = Text::from_iter(self.log.iter().map(|s| Line::raw(s)));
        let log = Paragraph::new(text)
                            .wrap(Wrap{trim: true});
        let log_height = log.line_count(log_area.width);
        frame.render_widget(log.scroll((log_height.saturating_sub(log_area.height as usize) as u16, 0)), log_area);
        frame.render_widget(self.input.value(), input_area);

        let scroll = self.input.visual_scroll(input_area.width as usize);
        let x = self.input.visual_cursor().max(scroll) - scroll;
        frame.set_cursor_position((input_area.x + x as u16, input_area.y));


    }

    fn enter(&mut self) -> bool {
        let cmd = self.input.value_and_reset();
        self.push(format!("> {}", cmd));
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
                    self.push("Wrong argument count");
                    return false;
                }
                let Ok(mut socks) = words[1].to_socket_addrs().or_else(|_| (words[1], 50051).to_socket_addrs()) else {
                    self.push("Unparseable location");
                    return false;
                };
                let Some(sock) = socks.next() else {
                    self.push("Not resolvable");
                    return false;
                };
                self.push(format!("{}", sock));
                let node = Node::from_sock(0, sock);
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.log(format!("{:?}", (node.ping(server).await)));
                    });
                } else {
                    self.push("Server is not available");
                }
            }

            "lookup_node" => {
                if words.len() != 2 {
                    self.push("Wrong argument count");
                    return false;
                }

                let Ok(key) = words[1].parse::<u32>() else {
                    self.push("Unparseable key");
                    return false;
                };
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.log(format!("Lookup result: {:?}", (server.lookup_node(key).await)));
                    });
                } else {
                    self.push("Server is not available");
                }
            }

            "lookup" => {
                if words.len() != 2 {
                    self.push("Wrong argument count");
                    return false;
                }

                let Ok(key) = words[1].parse::<u32>() else {
                    self.push("Unparseable key");
                    return false;
                };
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        match server.lookup_value(key).await {
                            Ok(StoreOrNodes::Store(store)) => {
                                server.log(format!("{}", store));
                            }
                            Ok(StoreOrNodes::Nodes(nodes)) => {
                                server.log(format!("{}",
                                        nodes.nodes.iter()
                                             .map(|n| n.to_string())
                                             .collect::<Vec<_>>()
                                             .join(" ")));
                            }
                            Err(e) => server.log(format!("{}", e))
                        }
                    });
                } else {
                    self.push("Server is not available");
                }
            }
            "local_store" => {
                if words.len() != 3 {
                    self.push("Wrong argument count");
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.push("Key not parsable");
                    return false;
                };
                let value = words[2].bytes().collect();
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.store.write().await.insert(key, value);
                        server.log("Stored".to_string());
                    });
                }
            }
            "store" => {
                if words.len() != 3 {
                    self.push("Wrong argument count");
                    return false;
                }

                let Ok(key) = words[1].parse() else {
                    self.push("Key not parsable");
                    return false;
                };
                let value = words[2].bytes().collect();
                if let Some(server) = self.server {
                    tokio::spawn(async move {
                        server.log(format!("{:?}", server.lookup_and_store(key, &value).await));
                    });
                }
            }
            "print" => {
                if words.len() != 2 {
                    self.push("Wrong argument count");
                    return false;
                }
                if let Some(server) = self.server {
                    match words[1] {
                        "id" => {
                            self.push(format!("id: {}", server.id));
                        }
                        "port" => {
                            self.push(format!("port: {}", server.port));
                        }
                        "buckets" => {
                            tokio::spawn( async move { server.log_buckets().await } );
                        }
                        "store" => {
                            tokio::spawn( async move { server.log_store().await } );
                        }
                        _ => {
                            self.push("Unknown thing to print");
                        }
                    }
                } else {
                    self.push("Server is not available");
                }
            }
            _ => {
                self.push("Unknown command");
            }

        }

        false
    }
}
