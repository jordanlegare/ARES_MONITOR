use axum::{extract::ws::{Message, WebSocket, WebSocketUpgrade}, routing::get, Router};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use iced::{Alignment, Border, Color, Element, Length, Shadow, Subscription, Task, Vector};
use iced::widget::{button, column, container, row, scrollable, text, text_input, Space, Id, operation};
use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebounceEventResult};
use tokio::time::{sleep, Duration};
use std::{fs::File, io::Read};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use zip::ZipArchive;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::Local;
use serde::{Deserialize, Serialize};

// Global for WebSocket URL
static WS_URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static ROUTER_IP: std::sync::OnceLock<String> = std::sync::OnceLock::new();

#[derive(Parser)]
#[command(name = "monitor")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server { path: String },
    Client { server: String, router: String },
}

#[derive(Serialize, Deserialize, Debug)]
struct NotePayload {
    text: String,
    version: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct UpdateResponse {
    new_version: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct MessagePayload {
    id: String,
    content: String,
}

async fn fetch_and_update_note(nuke: bool, router: String, id: String, new_text: String) -> Result<(), String> {
    let url = if let Some((project_id, subproject_id)) = id.split_once('_') {
        format!("http://{}/api/projects/{}/subprojects/{}/notes", router, project_id, subproject_id)
    } else {
        format!("http://{}/api/skills/{}/notes", router, id)
    };
    let client = reqwest::Client::new();
    println!("Fetching current note state from {}...", url);
    
    let current_note: NotePayload = client
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    let mut input_sysop = "".to_string();

    if !nuke{
        input_sysop = format!("{}\n\nSYSOP\n\n{}", current_note.text, new_text);
    }

    let update_payload = NotePayload {
        text: input_sysop,
        version: current_note.version, 
    };

    let response = client
        .post(&url)
        .json(&update_payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        println!("Success! Note updated.");
        Ok(())
    } else {
        Err(format!("Failed to update. Status: {}", response.status()))
    }
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server { path } => {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_server(path));
        }
        Commands::Client { server, router } => {
            run_client(server, router);
        }
    }
}

// --- SERVER LOGIC ---

async fn run_server(path: String) {
    let (tx, _) = broadcast::channel(100);
    let watcher_tx = tx.clone();
    let last_content_map = Arc::new(Mutex::new(HashMap::<std::path::PathBuf, String>::new()));
    let cache = last_content_map.clone();

    let mut debouncer = new_debouncer(Duration::from_millis(500), move |res: DebounceEventResult| {
        if let Ok(events) = res {
            for event in events {
                let p = event.path.canonicalize().unwrap_or(event.path);
                if !p.extension().map_or(false, |e| e == "zip") { continue; }

                let id = p.file_name()
                    .and_then(|os_str| os_str.to_str())
                    .and_then(|s| s.split('.').next())
                    .unwrap_or("unknown");

                if let Ok(f) = std::fs::File::open(&p) {
                    if let Ok(mut archive) = ZipArchive::new(f) {
                        if let Ok(mut member) = archive.by_index(0) {
                            let mut current_content = String::new();
                            if member.read_to_string(&mut current_content).is_ok() {
                                let mut map = cache.lock().unwrap();
                                
                                if let Some(old_content) = map.get(&p) {
                                    if old_content == &current_content { continue; }
                                }
                                
                                println!("Processing change for: {:?}", p);

                                let update_payload = MessagePayload {
                                    id: id.to_string(),
                                    content: current_content.clone(), 
                                };
                                
                                if let Ok(json_str) = serde_json::to_string(&update_payload) {
                                    map.insert(p.clone(), current_content.clone());
                                    let _ = watcher_tx.send(json_str); 
                                }
                            }
                        }
                    }
                }
            }
        }
    }).unwrap();

    debouncer.watcher().watch(std::path::Path::new(&path), RecursiveMode::NonRecursive).unwrap();
    let _ = Box::leak(Box::new(debouncer));

    let app = Router::new().route("/ws", get(move |ws: WebSocketUpgrade| async move {
        ws.on_upgrade(move |socket| handle_socket(socket, tx.subscribe()))
    }));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5656").await.unwrap();
    println!("Server WS listening on 0.0.0.0:5656");
    axum::serve(listener, app).await.unwrap();
}

async fn handle_socket(mut socket: WebSocket, mut rx: broadcast::Receiver<String>) {
    loop {
        // tokio::select! allows us to simultaneously poll the broadcast channel AND the incoming websocket
        tokio::select! {
            msg_res = rx.recv() => {
                if let Ok(msg) = msg_res {
                    if socket.send(Message::Text(msg.into())).await.is_err() {
                        break; // Client connection dropped
                    }
                } else {
                    break; // Broadcast channel closed or lagged
                }
            }
            socket_res = socket.recv() => {
                // We MUST poll recv() to process underlying Pings/Pongs correctly
                if socket_res.is_none() {
                    break; // Client manually disconnected
                }
            }
        }
    }
}

// --- CLIENT LOGIC ---

fn run_client(server: String, router: String) {
    WS_URL.set(server).unwrap();
    ROUTER_IP.set(router).unwrap();
    iced::application(Dashboard::new, Dashboard::update, Dashboard::view)
        .title("Monitor Dashboard")
        .subscription(|state| state.subscription())
        .run()
        .unwrap();
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub id: String, 
    pub text: String,
    pub is_mine: bool,
    pub timestamp: String,
}

struct Dashboard { 
    messages: Vec<ChatMessage>, 
    input_value: String,
    filter_query: String,
    server_ip: String,
    router_ip: String,
    selected_id: Option<String>, 
    latest_id: Option<String>,
    scroll_id: Id,
    is_at_bottom: bool,
}

#[derive(Debug, Clone)]
pub enum Msg {
    InputChanged(String),
    FilterChanged(String), 
    SendMessage,
    NukeMessages,          
    CopyToClipboard(String), 
    Received(String),
    PostResult(Result<(), String>),
    SelectMessage(String),
    ClearSelection,
    Scrolled(iced::widget::scrollable::Viewport),
}

fn ws_connect() -> impl iced::futures::Stream<Item = Msg> {
    let host = WS_URL.get().unwrap().clone();
    let ws_url = format!("ws://{}:5656/ws", host);
    
    iced::stream::channel(100, move |mut output: iced::futures::channel::mpsc::Sender<Msg>| async move {
        loop {
            println!("Attempting to connect to WS: {}", ws_url);
            
            match connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    println!("Successfully connected to server.");
                    let (_, mut read) = ws_stream.split();
                    
                    // The loop safely continues processing without breaking on Pings or Pongs
                    while let Some(result) = read.next().await {
                        match result {
                            Ok(WsMessage::Text(t)) => {
                                let _ = output.send(Msg::Received(t.to_string())).await;
                            }
                            Ok(WsMessage::Close(_)) => {
                                println!("Server closed the connection.");
                                break;
                            }
                            Err(e) => {
                                println!("WebSocket read error: {}", e);
                                break;
                            }
                            _ => {
                                // Explicitly ignore non-text frames (Binary, Ping, Pong)
                                // so the loop does not prematurely terminate.
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to connect to WS: {}. Retrying...", e);
                }
            }
            // If connection fails or drops, wait 2 seconds before attempting reconnection
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

impl Dashboard {
    fn new() -> (Self, Task<Msg>) { 
        (
            Self { 
                messages: vec![], 
                input_value: String::new(),
                filter_query: String::new(),
                server_ip: WS_URL.get().unwrap().clone(),
                router_ip: ROUTER_IP.get().unwrap().clone(),
                selected_id: None,
                latest_id: None,
                scroll_id: Id::unique(),
                is_at_bottom: true,
            }, 
            Task::none()
        ) 
    }

    fn active_target_id(&self) -> Option<String> {
        self.selected_id.clone().or(self.latest_id.clone())
    }

    fn update(&mut self, message: Msg) -> Task<Msg> {
        match message {
            Msg::InputChanged(val) => {
                self.input_value = val;
                Task::none()
            }
            Msg::FilterChanged(val) => {
                self.filter_query = val;
                Task::none()
            }
            Msg::SelectMessage(id) => {
                self.selected_id = Some(id);
                Task::none()
            }
            Msg::ClearSelection => {
                self.selected_id = None;
                Task::none()
            }
            Msg::NukeMessages => {
                let router = self.router_ip.clone();
                
                let mut unique_ids: Vec<String> = self.messages
                    .iter()
                    .map(|m| m.id.clone())
                    .collect();
                unique_ids.sort();
                unique_ids.dedup();

                self.messages.clear();

                let tasks = unique_ids.into_iter().map(|id| {
                    let r = router.clone();
                    Task::perform(
                        async move {
                            // 1. Perform the operation
                            let res = fetch_and_update_note(true, r, id, String::new()).await;
                            
                            // 2. Perform the sleep
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            
                            // 3. Explicitly return the result so Task::perform can map it to Msg::PostResult
                            res 
                        },
                        Msg::PostResult
                    )
                });

                Task::batch(tasks)
            }
            Msg::SendMessage => {
                if !self.input_value.trim().is_empty() {
                    let text_to_send = self.input_value.clone();
                    let current_time = Local::now().format("%Y-%m-%d %I:%M %p").to_string();
                    let target_id = self.active_target_id();

                    self.messages.push(ChatMessage {
                        id: target_id.clone().unwrap_or_else(|| "unknown".to_string()),
                        text: text_to_send.clone(),
                        is_mine: true,
                        timestamp: current_time,
                    });
                    
                    self.input_value.clear();

                    if let Some(id) = target_id {
                        let router = self.router_ip.clone();
                        self.selected_id = None; 
                        
                        // Combine the network task and the scroll task
                        Task::batch(vec![
                            Task::perform(
                                async move { fetch_and_update_note(false, router, id, text_to_send).await },
                                Msg::PostResult
                            ),
                            iced::widget::operation::snap_to_end(self.scroll_id.clone())
                        ])
                    } else {
                        println!("Cannot POST: No target ID available.");
                        Task::none()
                    }
                } else {
                    Task::none()
                }
            }
            Msg::PostResult(result) => {
                if let Err(e) = result {
                    println!("Failed background post task: {}", e);
                }
                Task::none()
            }
            Msg::CopyToClipboard(text) => {
                iced::clipboard::write(text) 
            }
            Msg::Received(json_str) => {
                match serde_json::from_str::<MessagePayload>(&json_str) {
                    Ok(payload) => {
                        self.latest_id = Some(payload.id.clone());
                        let current_time = Local::now().format("%Y-%m-%d %I:%M %p").to_string();
                        self.messages.push(ChatMessage {
                            id: payload.id, 
                            text: payload.content,
                            is_mine: false,
                            timestamp: current_time,
                        });
                    }
                    Err(_) => {
                        let current_time = Local::now().format("%Y-%m-%d %I:%M %p").to_string();
                        self.messages.push(ChatMessage {
                            id: "unknown".to_string(), 
                            text: json_str,
                            is_mine: false,
                            timestamp: current_time,
                        });
                    }
                }
                // Return the scroll task here
                if self.is_at_bottom {
                    // Updated line here:
                    iced::widget::operation::snap_to_end(self.scroll_id.clone())
                } else {
                    Task::none()
                }
            }
            Msg::Scrolled(viewport) => {
                // Check if the scroll is at the very end
                let relative_offset = viewport.relative_offset();
                self.is_at_bottom = relative_offset.y >= 0.99;
                Task::none()
            }
        }
    }

    fn subscription(&self) -> Subscription<Msg> {
        Subscription::run(ws_connect)
    }

    fn view(&self) -> Element<'_, Msg> {
        let active_id = self.active_target_id();
        
        let query = self.filter_query.to_lowercase();
        let filtered_messages: Vec<&ChatMessage> = self.messages
            .iter()
            .filter(|m| query.is_empty() || m.text.to_lowercase().contains(&query))
            .collect();

        let message_list = column(
            filtered_messages.into_iter().map(|msg| {
                let is_mine = msg.is_mine;
                let bg_color = if is_mine { Color::from_rgb8(217, 253, 211) } else { Color::from_rgb8(255, 255, 255) };
                
                let is_targeted = Some(&msg.id) == active_id.as_ref();

                let btn_style = |_theme: &iced::Theme, status: iced::widget::button::Status| {
                    let text_color = match status {
                        iced::widget::button::Status::Hovered => Color::from_rgb8(100, 100, 100),
                        _ => Color::from_rgb8(180, 180, 180), 
                    };
                    iced::widget::button::Style {
                        background: None,
                        border: Border::default(),
                        text_color,
                        ..Default::default()
                    }
                };

                let copy_button = button(text("⎘").size(14))
                    .on_press(Msg::CopyToClipboard(msg.text.clone()))
                    .padding(2)
                    .style(btn_style);

                let target_button = button(text("🎯").size(14))
                    .on_press(Msg::SelectMessage(msg.id.clone()))
                    .padding(2)
                    .style(btn_style);

                let footer = row![
                    text(&msg.timestamp).size(10).style(|_| iced::widget::text::Style { color: Some(Color::from_rgb8(150, 150, 150)) }),
                    Space::new().width(Length::Fixed(6.0)),
                    target_button,
                    Space::new().width(Length::Fixed(2.0)),
                    copy_button
                ].align_y(Alignment::Center);

                let bubble_content = column![
                    text(&msg.text).wrapping(iced::widget::text::Wrapping::Glyph).size(15),
                    footer
                ].spacing(4).align_x(Alignment::End); 

                let bubble = container(bubble_content)
                    .padding([8, 12])
                    .max_width(500.0)
                    .style(move |_theme| {
                        let border = if is_targeted {
                            Border { radius: 8.0.into(), width: 2.0, color: Color::from_rgb8(0, 120, 215) }
                        } else {
                            Border { radius: 8.0.into(), width: 0.0, color: Color::TRANSPARENT }
                        };

                        container::Style {
                            background: Some(bg_color.into()),
                            border,
                            shadow: Shadow {
                                color: Color::from_rgba8(0, 0, 0, 0.08),
                                offset: Vector::new(0.0, 1.0),
                                blur_radius: 2.0,
                            },
                            ..Default::default()
                        }
                    });

                let row_alignment = if is_mine {
                    row![Space::new().width(Length::Fill), bubble]
                } else {
                    row![bubble, Space::new().width(Length::Fill)]
                };

                row_alignment.into()
            })
        ).spacing(12).padding(20).width(Length::Fill);

        let scrollable_messages = scrollable(message_list).id(self.scroll_id.clone()).width(Length::Fill).height(Length::Fill).on_scroll(Msg::Scrolled);

        let input_row = row![
            text_input("Filter messages...", &self.filter_query)
                .on_input(Msg::FilterChanged)
                .padding(12)
                .width(Length::FillPortion(1)),
            text_input("Type a message", &self.input_value)
                .on_input(Msg::InputChanged)
                .on_submit(Msg::SendMessage)
                .padding(12)
                .width(Length::FillPortion(2)),
            button(text("Send").align_x(Alignment::Center))
                .on_press(Msg::SendMessage)
                .padding([12, 20]),
            button(text("Nuke").align_x(Alignment::Center))
                .on_press(Msg::NukeMessages)
                .style(|_theme, _status| iced::widget::button::Style {
                    background: Some(Color::from_rgb8(200, 50, 50).into()),
                    text_color: Color::WHITE,
                    border: Border::default(),
                    ..Default::default()
                })
                .padding([12, 20])
        ].spacing(10).align_y(Alignment::Center);

        let target_display: Element<'_, Msg> = match (&self.selected_id, &self.latest_id) {
            (Some(id), _) => row![
                text(format!("🔒 Locked Target: {}", id)).size(12).style(|_| iced::widget::text::Style { color: Some(Color::from_rgb8(0, 120, 215)) }),
                Space::new().width(Length::Fixed(8.0)),
                button(text("✖ Clear").size(10)).on_press(Msg::ClearSelection).padding([2, 6])
            ].align_y(Alignment::Center).into(),
            (None, Some(id)) => text(format!("🔄 Auto-Targeting Latest: {}", id))
                .size(12)
                .style(|_| iced::widget::text::Style { color: Some(Color::from_rgb8(100, 100, 100)) }).into(),
            (None, None) => text("Waiting for target ID...")
                .size(12)
                .style(|_| iced::widget::text::Style { color: Some(Color::from_rgb8(100, 100, 100)) }).into(),
        };

        let input_column = column![
            target_display,
            input_row
        ].spacing(4);

        let input_container = container(input_column)
            .padding(10)
            .width(Length::Fill)
            .style(|_theme| container::Style {
                background: Some(Color::from_rgb8(240, 240, 240).into()),
                ..Default::default()
            });

        container(column![scrollable_messages, input_container])
            .width(Length::Fill)
            .height(Length::Fill)
            .style(|_theme| container::Style {
                background: Some(Color::from_rgb8(239, 234, 226).into()),
                ..Default::default()
            }).into()
    }
}