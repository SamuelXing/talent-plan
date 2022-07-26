use std::sync::{Arc, Mutex};

use futures::channel::mpsc::UnboundedSender;

use lazy_static::lazy_static;
use rand::Rng;
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{
            unbounded_channel as unbounded, UnboundedReceiver as Receiver,
            UnboundedSender as Sender,
        },
        Notify,
    },
    time::{self, Duration, Instant},
};

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(500);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(650);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(950);

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::{errors::*, persister::*};
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

#[derive(Eq, PartialEq, Clone, Copy)]
enum Event {
    RecvAppendEntries,
    BroadcastHeartbeat,
    BroadcastEntries,
    SendEntries(usize),
}

/// LogEntry is a state machine transition along with some metadata needed for
/// Raft.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

impl From<crate::proto::raftpb::LogEntry> for LogEntry {
    fn from(entry: crate::proto::raftpb::LogEntry) -> Self {
        LogEntry {
            data: entry.data,
            index: entry.index,
            term: entry.term,
        }
    }
}

impl From<LogEntry> for crate::proto::raftpb::LogEntry {
    fn from(entry: LogEntry) -> Self {
        crate::proto::raftpb::LogEntry {
            index: entry.index,
            data: entry.data,
            term: entry.term,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct SnapshotMeta {
    pub last_included_term: u64,
    pub last_included_index: usize,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    // persistent state
    /// Current term.
    pub current_term: u64,
    /// Who the last vote was cast for.
    pub voted_for: Option<u64>,
    // TODO: start index
    /// entries this Replica is aware of.
    pub log: Vec<LogEntry>,

    // volatile state on all servers
    /// Index of the highest transition known to be committed.
    pub commit_index: usize,
    /// Index of the highest transition applied to the local state machine.
    pub last_applied: usize,

    // volatile state on leaders
    /// For each server, index of the next log entry to send to that server.
    /// Only present on leaders.
    pub next_index: Vec<usize>,
    /// For each server, index of highest log entry known to be replicated on
    /// that server. Only present on leaders.
    pub match_index: Vec<usize>,

    /// Snapshot related metadata
    pub snapshot: Option<SnapshotMeta>,
    /// The length of the log sequence that is represented by the snapshot.
    /// so calculate it as snapshot.last_included_index + 1.
    /// Since compacted entries aren't in the log anymore, access to the log
    /// should be done with log[log_index - index_offset].
    ///
    /// The following is always true:
    ///
    /// last_log_index = log.len() - 1 + index_offset.
    pub index_offset: usize,

    /// If no heartbeat message is received by the deadline, the Replica will
    /// start an election.
    pub next_election_deadline: Instant,

    /// Check if this node is killed
    pub is_killed: bool,
}

impl Default for State {
    fn default() -> State {
        State {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                index: 0,
                data: Vec::new(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            snapshot: None,
            index_offset: 0,
            next_election_deadline: Instant::now()
                + rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT),
            is_killed: false,
        }
    }
}

impl State {
    pub fn new(peers: usize) -> State {
        State {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                index: 0,
                data: Vec::new(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; peers],
            match_index: vec![0; peers],
            snapshot: None,
            index_offset: 0,
            next_election_deadline: Instant::now()
                + rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT),
            is_killed: false,
        }
    }
}
// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: State, /* Your data here (2A, 2B, 2C).
                   * Look at the paper's Figure 2 for a description of what
                   * state a Raft server must maintain. */
    role: Role,
    apply_ch: UnboundedSender<ApplyMsg>,
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
        let peers_len = peers.len();
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::new(peers_len),
            role: Role::default(),
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // info!("Node {:?} is starting...", me);
        debug!(
            "Node {:?} is starting with initial state: {:?}...",
            me, rf.state
        );
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let data = self.encode_state();
        self.persister.save_raft_state(data);
    }

    fn encode_state(&self) -> Vec<u8> {
        let mut data = vec![];
        let state2persist = PersistedState {
            current_term: self.state.current_term,
            voted_for: self
                .state
                .voted_for
                .map_or(String::from("null"), |id| id.to_string()),
            log: self.state.log.iter().cloned().map(Into::into).collect(),
        };
        labcodec::encode(&state2persist, &mut data).unwrap();
        data
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }

        match labcodec::decode::<PersistedState>(data) {
            Ok(state) => {
                self.state.current_term = state.current_term;
                self.state.voted_for = if state.voted_for.as_str() == "null" {
                    None
                } else {
                    Some(state.voted_for.parse().unwrap())
                };
                self.state.log = state.log.iter().cloned().map(Into::into).collect();
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
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
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            // If command received from client: append entry to local log,
            // respond after entry applied to state machine
            let term = self.state.current_term;
            let mut data = vec![];
            labcodec::encode(command, &mut data).map_err(Error::Encode)?;
            let index = (self.state.index_offset + self.state.log.len()) as u64;
            self.state.log.push(LogEntry { data, term, index });
            self.persist();
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
        if self.state.index_offset as u64 >= last_included_index
            || self.state.log[last_included_index as usize].term != last_included_term
        {
            return false;
        }
        self.snapshot(last_included_index, snapshot);

        true
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        let last_included_index = index as usize;
        // if current snapshot has larger index, return
        if self.state.index_offset >= last_included_index {
            return;
        }
        // else save the snapshot
        let new_snapshot = SnapshotMeta {
            last_included_index,
            last_included_term: self.state.log[last_included_index].term,
        };
        self.state.log = self.state.log[last_included_index + 1..].to_vec();
        self.state.snapshot = Some(new_snapshot);
        self.state.index_offset = last_included_index + 1;
        self.persister
            .save_state_and_snapshot(self.encode_state(), snapshot.to_vec());
    }
}
// Raft Shared State between threads
pub struct SharedRaft {
    raft: Mutex<Raft>,
    apply_log_notifier: Notify,
    events_notifier: Sender<Event>,
}

impl SharedRaft {
    fn new(raft: Raft, events_notifier: Sender<Event>) -> SharedRaft {
        SharedRaft {
            raft: Mutex::new(raft),
            apply_log_notifier: Notify::new(),
            events_notifier,
        }
    }

    /// The current term of this peer.
    fn get_current_term(&self) -> u64 {
        let term = self.raft.lock().unwrap().state.current_term;
        term
    }

    /// The current term of this peer.
    fn set_current_term(&self, term: u64) {
        let state = &mut self.raft.lock().unwrap().state;
        state.current_term = term;
    }

    /// Whether this peer believes it is the leader.
    fn is_leader(&self) -> bool {
        let role = self.raft.lock().unwrap().role;
        role == Role::Leader
    }

    /// Who the last vote was cast for.
    fn get_voted_for(&self) -> Option<u64> {
        let voted_for = self.raft.lock().unwrap().state.voted_for;
        voted_for
    }

    /// get the last log index
    fn get_last_log_index(&self) -> usize {
        if let Some(log) = self.raft.lock().unwrap().state.log.last() {
            log.index as usize
        } else {
            // if log is empty, then snapshot must not be empty, since log was started from 1
            self.get_index_offset() - 1
        }
    }

    /// get the last log term
    fn get_last_log_term(&self) -> u64 {
        let state = &self.raft.lock().unwrap().state;
        if let Some(entry) = state.log.last() {
            entry.term
        } else {
            state.snapshot.as_ref().unwrap().last_included_term
        }
    }

    /// get the last log len
    fn get_log_len(&self) -> usize {
        let len = self.raft.lock().unwrap().state.log.len();
        len
    }

    /// get the index_offset
    fn get_index_offset(&self) -> usize {
        let index_offset = self.raft.lock().unwrap().state.index_offset;
        index_offset
    }

    /// get the term of and entry at given index of the log
    fn get_term_at_index(&self, index: usize) -> Result<u64> {
        let state = &self.raft.lock().unwrap().state;
        match &state.snapshot {
            Some(snapshot) if index == snapshot.last_included_index => {
                Ok(snapshot.last_included_term)
            }
            Some(snapshot) if index > snapshot.last_included_index => {
                let localized_index = index - state.index_offset;
                return state
                    .log
                    .get(localized_index)
                    .map(|entry| entry.term)
                    .ok_or(Error::InvalidIndex);
            }
            Some(_) => Err(Error::LogCompacted),
            None => state
                .log
                .get(index)
                .map(|entry| entry.term)
                .ok_or(Error::InvalidIndex),
        }
    }

    // searching for the first index at given term, return None if term is not found
    fn find_first_index_at_term(&self, end: usize, term: u64) -> Option<usize> {
        let state = &self.raft.lock().unwrap().state;
        for index in state.index_offset + 1..=end {
            if state.log[index - state.index_offset].term == term {
                return Some(index);
            }
        }
        None
    }

    // searching for the last index at given term, return None if term is not found
    fn find_last_index_at_term(&self, end: usize, term: u64) -> Option<usize> {
        let state = &self.raft.lock().unwrap().state;
        for index in (state.index_offset..end).rev() {
            if state.log[index - state.index_offset].term == term {
                return Some(index);
            }
        }
        None
    }

    /// discard the entries follow the index in the log
    fn truncate_log(&self, index: usize) {
        let mut raft = self.raft.lock().unwrap();
        raft.state.log.truncate(index);
    }

    /// append a log entry to the log
    fn append_log_entry(&self, entry: LogEntry) {
        let mut raft = self.raft.lock().unwrap();
        raft.state.log.push(entry);
    }

    /// get the commit_index
    fn get_commit_index(&self) -> usize {
        let commit_index = self.raft.lock().unwrap().state.commit_index;
        commit_index
    }

    // get the index of last applied log entry
    fn get_last_applied(&self) -> usize {
        let last_applied = self.raft.lock().unwrap().state.last_applied;
        last_applied
    }

    // set the commit_index
    fn set_commit_index(&self, commit_index: usize) {
        let mut raft = self.raft.lock().unwrap();
        raft.state.commit_index = commit_index;
    }

    fn get_self_id(&self) -> u64 {
        let me = self.raft.lock().unwrap().me;
        me as u64
    }

    fn get_peers_len(&self) -> usize {
        let len = self.raft.lock().unwrap().peers.len();
        len
    }

    fn get_peer_by_id(&self, peer_id: usize) -> RaftClient {
        let peers = &self.raft.lock().unwrap().peers;
        peers[peer_id].clone()
    }

    fn is_killed(&self) -> bool {
        let is_killed = self.raft.lock().unwrap().state.is_killed;
        is_killed
    }

    fn get_role(&self) -> Role {
        let role = self.raft.lock().unwrap().role;
        role
    }

    fn update_role(&self, new_role: Role) -> bool {
        if self.get_role() == new_role {
            false
        } else {
            self.raft.lock().unwrap().role = new_role;
            true
        }
    }

    fn get_next_election_deadline(&self) -> Instant {
        let next_election_deadline = self.raft.lock().unwrap().state.next_election_deadline;
        next_election_deadline
    }

    fn update_next_election_deadline(&self) {
        let mut raft = self.raft.lock().unwrap();
        let raft = &mut *raft;
        raft.state.next_election_deadline = Instant::now()
            + rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT);
    }
}

#[derive(Clone)]
pub struct Node {
    pub shared: Arc<SharedRaft>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let (events_notifier, events_handler) = unbounded();
        let shared = Arc::new(SharedRaft::new(raft, events_notifier));

        RUNTIME.spawn(handle_current_state(shared.clone(), events_handler));
        RUNTIME.spawn(handle_apply_log(shared.clone()));

        Node { shared }
    }

    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut raft = self.shared.raft.lock().unwrap();
        let res = raft.start(command);
        if res.is_ok() {
            let _ = self.shared.events_notifier.send(Event::BroadcastEntries);
        }
        res
    }

    /// The current term of this peer.
    pub fn get_current_term(&self) -> u64 {
        self.shared.get_current_term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.shared.is_leader()
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
        let mut raft = self.shared.raft.lock().unwrap();
        raft.state.is_killed = true;
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
        let mut raft = self.shared.raft.lock().unwrap();
        raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        let mut raft = self.shared.raft.lock().unwrap();
        raft.snapshot(index, snapshot)
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let current_term = self.get_current_term();
        let voted_for = self.shared.get_voted_for();

        // Do not vote for Replicas that are behind.
        if args.term < current_term {
            return Ok(RequestVoteReply {
                msg_type: MessageType::RequestVoteResponse as i32,
                term: current_term,
                vote_granted: false,
            });
        }
        // If RPC request contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if args.term > current_term {
            self.shared.set_current_term(args.term);
        }

        // If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote
        let last_log_index = self.shared.get_last_log_index();
        let last_log_term = self.shared.get_last_log_term();

        if (voted_for == None || voted_for == Some(args.candidate_id))
      // candidate’s log is at least as up-to-date as receiver’s log
      && last_log_index as u64 <= args.last_log_index
      && last_log_term as u64 <= args.last_log_term
        {
            return Ok(RequestVoteReply {
                msg_type: MessageType::RequestVoteResponse as i32,
                term: current_term,
                vote_granted: true,
            });
        }

        Ok(RequestVoteReply {
            msg_type: MessageType::RequestVoteResponse as i32,
            term: current_term,
            vote_granted: false,
        })
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let shared = &self.shared;
        // notify backgroung task that an append_entries rpc arrived
        let _ = shared.events_notifier.send(Event::RecvAppendEntries);

        let current_term = shared.get_current_term();
        // Reply false if term < currentTerm
        if args.term < current_term {
            return Ok(AppendEntriesReply {
                msg_type: MessageType::AppendResponse as i32,
                term: current_term,
                success: false,
                conflict_index: 0,
                conflict_term: 0,
            });
        }
        // If RPC request contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if args.term > current_term {
            shared.set_current_term(args.term);
        }

        // In raft paper: Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm
        // more details see: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
        // If a follower does not have prevLogIndex in its log,
        // it should return with conflictIndex = len(log) and conflictTerm = None.
        if args.prev_log_index > shared.get_last_log_index() as u64 {
            return Ok(AppendEntriesReply {
                msg_type: MessageType::AppendResponse as i32,
                term: current_term,
                success: false,
                conflict_index: (shared.get_last_log_index() + 1) as u64,
                conflict_term: 0,
            });
        }

        // If a follower does have prevLogIndex in its log, but the term does not match,
        // it should return conflictTerm = log[prevLogIndex].Term,
        // and then search its log for the first index whose entry has term equal to conflictTerm.
        let prev_log_term = shared.get_term_at_index(args.prev_log_index as usize)?;
        if prev_log_term != args.prev_log_term {
            let conflict_term = prev_log_term;
            let conflict_index = shared
                .find_first_index_at_term(args.prev_log_index as usize, conflict_term)
                .unwrap() as u64;
            return Ok(AppendEntriesReply {
                msg_type: MessageType::AppendResponse as i32,
                term: current_term,
                success: false,
                conflict_index,
                conflict_term,
            });
        }

        // If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it
        for entry in args.entries {
            if entry.index <= shared.get_last_log_index() as u64
                && shared.get_term_at_index(entry.index as usize)? != entry.term
            {
                shared.truncate_log(entry.index as usize);
            }
            // Append any new entries not already in the log
            if entry.index == (shared.get_log_len() + shared.get_index_offset()) as u64 {
                shared.append_log_entry(LogEntry::from(entry))
            }
        }

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if args.leader_commit > shared.get_commit_index() as u64 {
            shared.set_commit_index(std::cmp::min(
                args.leader_commit as usize,
                shared.get_last_log_index(),
            ));
            // notify that commit_index updated, maybe it's time to apply log
            shared.apply_log_notifier.notify_one();
        }

        Ok(AppendEntriesReply {
            msg_type: MessageType::AppendResponse as i32,
            term: current_term,
            success: true,
            conflict_index: 0,
            conflict_term: 0,
        })
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        // notify backgroung task that an append_entries rpc arrived
        let _ = self.shared.events_notifier.send(Event::RecvAppendEntries);

        let mut raft = self.shared.raft.lock().unwrap();
        let raft = &mut *raft;
        let state = &mut raft.state;
        let current_term = state.current_term;
        let last_included_index = args.last_included_index as usize;
        // Reply immediately if leader's term < currentTerm
        if args.term < current_term {
            return Ok(InstallSnapshotReply {
                msg_type: MessageType::AppendResponse as i32,
                term: current_term,
            });
        }
        // If RPC request contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if args.term > current_term {
            state.current_term = args.term;
        }

        // local snapshot is longer than leader's, reply immediately
        if let Some(ref snapshot) = state.snapshot {
            if snapshot.last_included_index >= last_included_index {
                return Ok(InstallSnapshotReply {
                    msg_type: MessageType::AppendResponse as i32,
                    term: current_term,
                });
            }
        }
        // If existing log entry has same index and term as snapshot’s
        // last included entry, retain log entries following it and reply
        if let Some(log) = state
            .log
            .rsplit(|entry| {
                entry.index == last_included_index as u64 && entry.term == args.last_included_term
            })
            .next()
        {
            state.log = log.to_vec();
        } else {
            // Discard the entire log
            state.log = Vec::new();
        }

        state.snapshot = Some(SnapshotMeta {
            last_included_index,
            last_included_term: args.last_included_term,
        });
        state.index_offset = last_included_index + 1;
        state.commit_index = last_included_index;
        state.last_applied = last_included_index;
        raft.persister
            .save_state_and_snapshot(raft.encode_state(), args.data.to_vec());

        let _ = raft.apply_ch.unbounded_send(ApplyMsg::Snapshot {
            data: args.data.clone(),
            term: args.last_included_term,
            index: args.last_included_index,
        });

        return Ok(InstallSnapshotReply {
            msg_type: MessageType::AppendResponse as i32,
            term: current_term,
        });
    }
}

fn broadcast_request_vote(shared: Arc<SharedRaft>) -> Receiver<Result<RequestVoteReply>> {
    let (response_tx, response_rx) = unbounded();
    let me = shared.get_self_id();

    for peer_id in 0..shared.get_peers_len() {
        // skip self
        if peer_id as u64 == me {
            continue;
        }
        let peer = shared.get_peer_by_id(peer_id);
        let peer_clone = peer.clone();
        let args = RequestVoteArgs {
            msg_type: MessageType::RequestVote as i32,
            term: shared.get_current_term(),
            candidate_id: me,
            last_log_index: shared.get_last_log_index() as u64,
            last_log_term: shared.get_last_log_term(),
        };

        let response_tx_clone = response_tx.clone();
        // call request_vote rpc in async
        peer.spawn(async move {
            debug!(
                "Node {:?} send request_vote RPC to peer Node {:?} with args: {:?}",
                me, peer_id, args
            );
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = response_tx_clone.send(res);
        });
    }
    response_rx
}

fn handle_append_entries_response(
    shared: Arc<SharedRaft>,
    peer_id: usize,
    args: &AppendEntriesArgs,
    response: Result<AppendEntriesReply>,
) {
    if let Ok(response) = response {
        // If RPC response contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if response.term > shared.get_current_term() {
            shared.set_current_term(response.term);
            shared.update_role(Role::Follower);
            return;
        }
        if response.success {
            // update nextIndex and matchIndex for follower
            let mut raft = shared.raft.lock().unwrap();
            let raft = &mut *raft;
            let state = &mut raft.state;
            let last_log_index = args.prev_log_index as usize + args.entries.len();
            state.next_index[peer_id] = last_log_index + 1;
            state.match_index[peer_id] = last_log_index;

            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N.
            let mut n = state.log.len() - 1 + state.index_offset;
            while n > state.commit_index {
                let num_replications =
                    state.match_index.iter().fold(
                        0,
                        |acc, mtch_idx| {
                            if mtch_idx >= &n {
                                acc + 1
                            } else {
                                acc
                            }
                        },
                    );

                if num_replications * 2 >= raft.peers.len()
                    && state.log[n - state.index_offset].term == state.current_term
                {
                    state.commit_index = n;
                    break;
                }
                n -= 1;
            }

            // notify that commit_index updated, maybe it's time to apply log
            shared.apply_log_notifier.notify_one();
        } else {
            // Original log backtracking: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
            // Accelerated log backtracking: https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
            let state = &mut shared.raft.lock().unwrap().state;
            // Upon receiving a conflict response, the leader should first search its log for conflictTerm.
            // If it finds an entry in its log with that term,
            // it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
            if let Some(conflict_index) =
                shared.find_last_index_at_term(args.prev_log_index as usize, response.conflict_term)
            {
                state.next_index[peer_id as usize] = conflict_index;
            } else {
                // If it does not find an entry with that term, it should set nextIndex = conflictIndex.
                state.next_index[peer_id as usize] = response.conflict_index as usize;
            }

            // retry
            let _ = shared.events_notifier.send(Event::SendEntries(peer_id));
        }
    }
}

fn prepare_append_entries_args(
    shared: Arc<SharedRaft>,
    peer_id: usize,
    heartbeat: bool,
    prev_log_index: u64,
    prev_log_term: u64,
) -> AppendEntriesArgs {
    let raft = &shared.raft.lock().unwrap();
    let state = &raft.state;

    // heartbeat: AppendEntries RPCs that carry no log entries
    let (entries, msg_type) = if heartbeat {
        (Vec::new(), MessageType::Heartbeat as i32)
    } else {
        (
            state.log[state.next_index[peer_id as usize] - state.index_offset..state.log.len()]
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
            MessageType::Append as i32,
        )
    };

    AppendEntriesArgs {
        msg_type,
        term: state.current_term,
        leader_id: raft.me as u64,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit: state.commit_index as u64,
    }
}

fn prepare_install_snapshot_args(shared: Arc<SharedRaft>) -> InstallSnapshotArgs {
    let raft = &shared.raft.lock().unwrap();
    let state = &raft.state;
    let snapshot = state.snapshot.as_ref().unwrap();
    InstallSnapshotArgs {
        msg_type: MessageType::Snapshot as i32,
        term: state.current_term,
        leader_id: raft.me as u64,
        last_included_index: snapshot.last_included_index as u64,
        last_included_term: snapshot.last_included_term,
        // for this lab, just sending the entire snapshot
        offset: 0,
        data: raft.persister.snapshot(),
        done: true,
    }
}

fn send_append_entries(shared: Arc<SharedRaft>, peer_id: usize, args: AppendEntriesArgs) {
    let peer = shared.get_peer_by_id(peer_id);
    let peer_clone = peer.clone();

    // call append_entries rpc in async
    peer.spawn(async move {
        let response = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
        debug!("append_entries response: {:?}", response);
        handle_append_entries_response(shared, peer_id, &args, response);
    });
}

fn send_install_snapshot(shared: Arc<SharedRaft>, peer_id: usize, args: InstallSnapshotArgs) {
    let peer = shared.get_peer_by_id(peer_id);
    let peer_clone = peer.clone();

    // call install_snapshot rpc in async
    peer.spawn(async move {
        let response = peer_clone.install_snapshot(&args).await.map_err(Error::Rpc);
        if let Ok(response) = response {
            if response.term > shared.get_current_term() {
                shared.set_current_term(response.term);
                shared.update_role(Role::Follower);
            }
        }
    });
}

fn append_entries_to_single_peer(shared: Arc<SharedRaft>, peer_id: usize, heartbeat: bool) {
    let prev_log_index = {
        let state = &shared.raft.lock().unwrap().state;
        state.next_index[peer_id] - 1
    };

    match shared.get_term_at_index(prev_log_index) {
        Ok(prev_log_term) => {
            let args = prepare_append_entries_args(
                shared.clone(),
                peer_id,
                heartbeat,
                prev_log_index as u64,
                prev_log_term,
            );
            debug!(
                "Node {:?} send append_entries RPC to peer Node {:?} with args: {:?}",
                shared.get_self_id(),
                peer_id,
                args
            );
            send_append_entries(shared.clone(), peer_id, args);
        }
        Err(Error::LogCompacted) => {
            let args = prepare_install_snapshot_args(shared.clone());
            send_install_snapshot(shared.clone(), peer_id, args);
        }
        Err(_) => unreachable!(),
    }
}

fn broadcast_append_entries(shared: Arc<SharedRaft>, heartbeat: bool) {
    for peer_id in 0..shared.get_peers_len() {
        // skip self
        if peer_id as u64 == shared.get_self_id() {
            continue;
        }

        append_entries_to_single_peer(shared.clone(), peer_id, heartbeat);
    }
}

async fn handle_follower_state(shared: Arc<SharedRaft>, events_handler: &mut Receiver<Event>) {
    assert_eq!(shared.get_role(), Role::Follower);

    let next_election_deadline = shared.get_next_election_deadline();

    tokio::select! {
      // heartbeat timeout, start an election
      _ = time::sleep_until(next_election_deadline) => {
        info!("Follower receive heartbeat timeout, start a new election..");
        let _ = shared.update_role(Role::Candidate);
      },
      // received heartbeat, update deadline
      Some(event) = events_handler.recv() => {
        if event == Event::RecvAppendEntries {
            shared.update_next_election_deadline();
        }
      },
      else => (),
    }
}

async fn handle_candidate_state(shared: Arc<SharedRaft>, events_handler: &mut Receiver<Event>) {
    assert_eq!(shared.get_role(), Role::Candidate);
    {
        let mut raft = shared.raft.lock().unwrap();
        let raft = &mut *raft;
        // Increment currentTerm
        raft.state.current_term += 1;
        // Vote for self
        raft.state.voted_for = Some(raft.me as u64);
    }
    // Reset election timer
    shared.update_next_election_deadline();

    // Send RequestVote RPCs to all other servers
    let mut response_rx = broadcast_request_vote(shared.clone());

    let mut vote_count = 0;
    'candidate: loop {
        tokio::select! {
          Some(reply) = response_rx.recv() => {
            if reply.is_err() {
              continue;
            }
            let RequestVoteReply {msg_type: _, term, vote_granted} = reply.unwrap();
            // If RPC response contains term T > currentTerm:
            // set currentTerm = T, convert to follower
            if term > shared.get_current_term() {
              shared.set_current_term(term);
              shared.update_role(Role::Follower);
              break 'candidate;
            }
            if vote_granted {
              vote_count += 1;
            }
            debug!("Node {:?} current vote count: {:?}", shared.get_self_id(), vote_count);
            // If votes received from majority of servers: become leader
            if vote_count > shared.get_peers_len()/2 {
              shared.update_role(Role::Leader);
              break 'candidate;
            }
          },
          Some(event) = events_handler.recv() => {
            if event == Event::RecvAppendEntries {
                // If AppendEntries RPC received from new leader: convert to follower
                shared.update_role(Role::Follower);
                break 'candidate;
            }
          },
          _ = time::sleep_until(shared.get_next_election_deadline()) => {
            info!("Candidate election timeout, start a new election..");
            // If election timeout elapses: start new election
            // Start new election by just breaking the loop; Stay in Role::Candidate
            break 'candidate;
          },
          else => break 'candidate,
        }
    }
}

async fn handle_leader_state(shared: Arc<SharedRaft>, events_handler: &mut Receiver<Event>) {
    assert_eq!(shared.get_role(), Role::Leader);

    // Upon election: send initial empty AppendEntries RPCs
    // (heartbeat) to each server; repeat during idle periods to
    // prevent election timeouts
    broadcast_append_entries(shared.clone(), true);
    
    let mut heartbeat = time::interval(HEARTBEAT_TIMEOUT);
    while shared.is_leader() && !shared.is_killed() {
        let event = tokio::select! {
            _ = heartbeat.tick() => { Event::BroadcastHeartbeat }
            Some(event) = events_handler.recv() => { event },
            else => break,
        };

        match event {
            Event::BroadcastHeartbeat => broadcast_append_entries(shared.clone(), true),
            Event::BroadcastEntries => broadcast_append_entries(shared.clone(), false),
            Event::SendEntries(peer_id) => {
                append_entries_to_single_peer(shared.clone(), peer_id, false)
            }
            _ => (),
        }
    }
}

// background task to handle the current role
async fn handle_current_state(shared: Arc<SharedRaft>, mut events_handler: Receiver<Event>) {
    while !shared.is_killed() {
        let current_state = shared.get_role();
        info!(
            "Node {:?} becomes {:?}",
            shared.get_self_id(),
            current_state
        );
        match current_state {
            Role::Follower => handle_follower_state(shared.clone(), &mut events_handler).await,
            Role::Candidate => handle_candidate_state(shared.clone(), &mut events_handler).await,
            Role::Leader => handle_leader_state(shared.clone(), &mut events_handler).await,
        }
    }
}

// background task to apply a log entry to application layer if it's ready
async fn handle_apply_log(shared: Arc<SharedRaft>) {
    while !shared.is_killed() {
        if shared.get_commit_index() > shared.get_last_applied() {
            let mut raft = shared.raft.lock().unwrap();
            raft.state.last_applied += 1;
            let _ = raft.apply_ch.unbounded_send(ApplyMsg::Command {
                data: raft.state.log[raft.state.last_applied].data.clone(),
                index: raft.state.last_applied as u64,
            });
        } else {
            shared.apply_log_notifier.notified().await;
        }
    }
}
