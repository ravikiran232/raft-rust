use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::thread::JoinHandle;

use futures::channel::mpsc::UnboundedSender;
use rand::Rng;
use std::sync::Mutex;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: Arc<Mutex<u64>>,
    pub is_leader: Arc<Mutex<bool>>,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        *self.term.lock().unwrap()
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        *self.is_leader.lock().unwrap()
    }
}

// A single Raft peer.

pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Mutex<Box<dyn Persister>>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    log_entries: Mutex<Vec<(u64, Vec<u8>)>>, // (term, command)
                                             // Your data here (2A, 2B, 2C).
                                             // Look at the paper's Figure 2 for a description of what
                                             // state a Raft server must maintain.
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister: Mutex::new(persister),
            me,
            state: Arc::default(),
            log_entries: Mutex::new(vec![(0, vec![])]),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
        // crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res);
        });
        // let mut total_positive_votes = 0;
        // for _ in 0..server{
        //     let peer_response = rx.recv().expect("unable to recieve the messager from channel").unwrap();
        //     if peer_response.vote_value==1{
        //         total_positive_votes+=1;
        //     }
        // }
        return rx;
    }

    fn start<M>(self: Arc<Self>, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = self.log_entries.lock().unwrap().len() as u64;
        let term = *self.state.term.lock().unwrap();
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        self.log_entries.lock().unwrap().push((term, buf));
        // Your code here (2B).
        let self_clone = self.clone();
        for peer in self_clone.peers.iter() {
            let peer_clone = peer.clone();
            let self_clone_move = self_clone.clone();
            if *self_clone_move.state.is_leader.lock().unwrap() {
                // If this node is not the leader, we don't need to send the command.
                continue;
            }
            peer.spawn(async move {
                let mut res = false;
                let mut index = self_clone_move.log_entries.lock().unwrap().len() as u64 - 1;
                println!("lock checkpoint0");
                while !res {
                    let prev_term = self_clone_move.log_entries.lock().unwrap()[index as usize].0;
                    let args = AppendEntriesArgs {
                        term: *self_clone_move.state.term.lock().unwrap(),
                        prev_index: index,
                        prev_term,
                        entries: self_clone_move.create_entries(
                            self_clone_move.log_entries.lock().unwrap().len() - index as usize,
                        ),
                    };
                    let reply = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
                    if let Ok(reply) = reply {
                        if reply.success {
                            res = true;
                        } else {
                            index -= 1;
                        }
                    }
                }
            });
        }
        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }

    pub fn send_heartbeat_handler(&self, index: usize) -> Receiver<Result<AppendEntriesReply>> {
        // //println!("Sending heartbeat to all peers from {:?}", self.me);
        let args = AppendEntriesArgs {
            term: *self.state.term.lock().unwrap(),
            prev_index: 0,
            prev_term: 0,
            entries: vec![], // No entries for heartbeat
        };
        // for peer in self.peers.iter() {
        //     let _ = peer.append_entries(&args);
        // }
        let (tx, rx) = sync_channel::<Result<AppendEntriesReply>>(1);
        let peer = &self.peers[index];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.send(res);
        });
        rx
    }

    fn create_entries(&self, no_of_entries: usize) -> Vec<CommandEntry> {
        let len_log = self.log_entries.lock().unwrap().len();

        self.log_entries.lock().unwrap()[len_log - no_of_entries..len_log]
            .iter()
            .map(|entry| CommandEntry {
                term: entry.0,
                command: entry.1.iter().map(|&x| x.into()).collect(),
            })
            .collect()
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        // let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Raft>,
    timeout_handler: Arc<Mutex<Option<JoinHandle<()>>>>,
    recieved_heartbeat: Arc<Mutex<bool>>,
    kill_handlers: Arc<Mutex<bool>>,
    heartbeat_sender: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(raft),
            timeout_handler: Arc::new(Mutex::new(None)),
            recieved_heartbeat: Arc::new(Mutex::new(false)),
            kill_handlers: Arc::new(Mutex::new(false)),
            heartbeat_sender: Arc::new(Mutex::new(None)),
        };
        //println!("Node created with id: {}", node.raft.me);
        node.heartbeat_sender();
        node.timeout_handler();
        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        // If this node is the leader, start the command.
        let raft_clone = self.raft.clone();
        let res = raft_clone.start(command);
        res
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // //println!("trying to hold the lock");
        *self.raft.state.term.lock().unwrap()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        *self.raft.state.is_leader.lock().unwrap()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: Arc::new(Mutex::new(self.term())),
            is_leader: Arc::new(Mutex::new(self.is_leader())),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        *self.kill_handlers.lock().unwrap() = true;
        let th = self.timeout_handler.lock().unwrap().take();
        let hh = self.heartbeat_sender.lock().unwrap().take();
        if let Some(timeout) = th {
            timeout.join().unwrap();
        }
        if let Some(heartbeat) = hh {
            heartbeat.join().unwrap();
        }
        //println!("Node {} killed", self.raft.me);
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }

    fn timeout_handler(&self) {
        let raft_clone = self.raft.clone();
        let recieved_heartbeat_clone = self.recieved_heartbeat.clone();
        let kill_handlers_clone = self.kill_handlers.clone();
        let handler = std::thread::spawn(move || {
            let mut rnd = rand::thread_rng();
            loop {
                if *kill_handlers_clone.lock().unwrap() {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(rnd.gen_range(150, 301)));
                if *recieved_heartbeat_clone.lock().unwrap() {
                    *recieved_heartbeat_clone.lock().unwrap() = false;
                    continue;
                }
                if !*raft_clone.state.is_leader.lock().unwrap() {
                    *raft_clone.state.term.lock().unwrap() += 1;
                    let log_lock = raft_clone.log_entries.lock().unwrap();
                    let args = RequestVoteArgs {
                        current_term: *raft_clone.state.term.lock().unwrap(),
                        current_index: log_lock.len() as u32 - 1,
                        current_entry_hash: log_lock.last().unwrap_or(&(0, vec![])).0 as u32, // This should be the hash of the last log entry
                        requesting_peer: raft_clone.me as u32,
                    };
                    let mut vec_rx = vec![];
                    for i in 0..raft_clone.peers.len() {
                        if *kill_handlers_clone.lock().unwrap() {
                            return;
                        }
                        if i != raft_clone.me {
                            let rx = raft_clone.send_request_vote(i, args.clone());
                            vec_rx.push(rx);
                        }
                    }
                    let mut total_positive_votes = 1;
                    for rx in vec_rx {
                        if *kill_handlers_clone.lock().unwrap() {
                            return;
                        }
                        let rc_value = rx.recv_timeout(std::time::Duration::from_millis(150));
                        if let Ok(value) = rc_value {
                            if value.is_ok() {
                                total_positive_votes += value.unwrap().vote_value;
                            }
                        }
                    }
                    if total_positive_votes as usize > raft_clone.peers.len() / 2 {
                        if *kill_handlers_clone.lock().unwrap() {
                            return;
                        }
                        *raft_clone.state.is_leader.lock().unwrap() = true;
                        for i in 0..raft_clone.peers.len() {
                            if i != raft_clone.me {
                                let _ = raft_clone.send_heartbeat_handler(i);
                            }
                        }
                    }
                }
            }
        });
        self.timeout_handler.lock().unwrap().replace(handler);
    }

    fn heartbeat_sender(&self) {
        let raft_clone = self.raft.clone();
        let kill_handlers_clone = self.kill_handlers.clone();
        let handle = std::thread::spawn(move || loop {
            if *kill_handlers_clone.lock().unwrap() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
            if *raft_clone.state.is_leader.lock().unwrap() == false {
                continue;
            }
            let mut rx_vec = vec![];
            for i in 0..raft_clone.peers.len() {
                if *kill_handlers_clone.lock().unwrap() {
                    return;
                }
                if i != raft_clone.me {
                    let rx = raft_clone.send_heartbeat_handler(i);
                    rx_vec.push(rx);
                }
            }
            let mut rx_error_count = 0;
            for rx in &rx_vec {
                if *kill_handlers_clone.lock().unwrap() {
                    return;
                }
                let rc_value = rx.recv_timeout(std::time::Duration::from_millis(150));
                if let Ok(value) = rc_value {
                    if value.is_ok() && !value.clone().unwrap().success {
                        *raft_clone.state.is_leader.lock().unwrap() = false;
                        *raft_clone.state.term.lock().unwrap() = value.clone().unwrap().term;
                    } else if value.is_err() {
                        rx_error_count += 1;
                    }
                }
            }
            if rx_error_count == rx_vec.len() {
                *raft_clone.state.is_leader.lock().unwrap() = false;
            }
        });
        self.heartbeat_sender.lock().unwrap().replace(handle);
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut term_lock = self.raft.state.term.lock().unwrap();
        let last_term_inlog = self
            .raft
            .log_entries
            .lock()
            .unwrap()
            .last()
            .unwrap_or(&(0, vec![]))
            .0 as u32;
        if *term_lock >= args.current_term as u64 {
            return Ok(RequestVoteReply { vote_value: 0 });
        }
        if args.current_entry_hash < last_term_inlog {
            return Ok(RequestVoteReply { vote_value: 0 });
        }
        if args.current_entry_hash == last_term_inlog
            && args.current_index < self.raft.log_entries.lock().unwrap().len() as u32 - 1
        {
            return Ok(RequestVoteReply { vote_value: 0 });
        }
        *term_lock = args.current_term as u64;
        return Ok(RequestVoteReply { vote_value: 1 });
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        // Your code here (2A, 2B).
        // crate::your_code_here(args);
        println!("lock checkpoint1");
        let mut term_lock = self.raft.state.term.lock().unwrap();
        if args.term >= *term_lock {
            println!("lock checkpoint misc1");
            *self.recieved_heartbeat.lock().unwrap() = true;
            println!("lock checkpoint misc1.1");
            *term_lock = args.term;
            println!("lock checkpoint misc2");
            *self.raft.state.is_leader.lock().unwrap() = false;
            println!("lock checkpoint2");
            if !args.entries.is_empty() {
                let last_index = self.raft.log_entries.lock().unwrap().len() as u64 - 1;
                let term_to_compare = self
                    .raft
                    .log_entries
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap_or(&(0, vec![]))
                    .0;
                println!("lock checkpoint3");
                if args.prev_index > last_index || args.prev_term != term_to_compare {
                    return Ok(AppendEntriesReply {
                        success: false,
                        term: *term_lock,
                    });
                }
                println!("lock checkpoint4");
                // remove the entries that are after the prev_index
                self.raft
                    .log_entries
                    .lock()
                    .unwrap()
                    .truncate(args.prev_index as usize + 1);
                let mut log_lock = self.raft.log_entries.lock().unwrap();
                for entry in args.entries.iter() {
                    log_lock.push((entry.term, entry.command.iter().map(|&x| x as u8).collect()));
                }
                println!("lock checkpoint5");
            }
            return Ok(AppendEntriesReply {
                success: true,
                term: 0,
            });
        } else {
            return Ok(AppendEntriesReply {
                success: false,
                term: *term_lock,
            });
        }
    }
}
