use log::info;
use raft::eraftpb::Message;
use raft::prelude::EntryType;
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use std::cmp::max;
use std::sync::Mutex;

use crate::generated::{get_root_as_generic_request, GenericRequest};
use crate::local_storage::LocalStorage;
use crate::storage_node::{handler, LocalContext};
use crate::utils::is_write_request;
use flatbuffers::FlatBufferBuilder;
use futures::sync::oneshot;
use futures::sync::oneshot::Sender;
use futures::Future;
use std::collections::HashMap;

pub struct RaftManager<'a> {
    raft_node: Mutex<RawNode<MemStorage>>,
    pending_responses: Mutex<HashMap<u64, (FlatBufferBuilder<'a>, Sender<FlatBufferBuilder<'a>>)>>,
    node_id: u64,
    context: LocalContext,
}

impl<'a> RaftManager<'a> {
    pub fn new(context: LocalContext) -> RaftManager<'a> {
        // Unique ID of node within the cluster. Never 0.
        let node_id = 1;
        let raft_config = Config {
            id: node_id,
            peers: vec![node_id],
            learners: vec![],
            // TODO: set good value
            election_tick: 10,
            // TODO: set good value
            heartbeat_tick: 3,
            // TODO: need to restore this from storage
            applied: 0,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            tag: format!("peer_{}", node_id).to_string(),
            ..Default::default()
        };
        let raft_storage = MemStorage::new();
        let raft_node = RawNode::new(&raft_config, raft_storage, vec![]).unwrap();

        RaftManager {
            raft_node: Mutex::new(raft_node),
            pending_responses: Mutex::new(HashMap::new()),
            node_id,
            context,
        }
    }

    pub fn apply_messages(&self, messages: &[Message]) -> raft::Result<()> {
        let mut raft_node = self.raft_node.lock().unwrap();

        for message in messages {
            assert_eq!(message.to, self.node_id);
            raft_node.step(message.clone())?;
        }

        Ok(())
    }

    pub fn get_outgoing_raft_messages(&self) -> raft::Result<Vec<Message>> {
        let mut raft_node = self.raft_node.lock().unwrap();
        if !raft_node.has_ready() {
            return Ok(vec![]);
        }

        let mut ready = raft_node.ready();

        Ok(ready.messages.drain(..).collect())
    }

    // TODO: Add background thread to make sure this function is called frequently
    // Returns the last applied index
    fn process_raft_queue(&self) -> raft::Result<Option<u64>> {
        let mut raft_node = self.raft_node.lock().unwrap();

        if !raft_node.has_ready() {
            return Ok(None);
        }

        let mut ready = raft_node.ready();

        if !raft::is_empty_snap(ready.snapshot()) {
            raft_node
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())?;
        }

        if !ready.entries().is_empty() {
            raft_node.mut_store().wl().append(ready.entries())?;
        }

        if let Some(hard_state) = ready.hs() {
            raft_node.mut_store().wl().set_hardstate(hard_state.clone());
        }

        let mut applied_index = 0;
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                applied_index = max(applied_index, entry.index);

                if entry.data.is_empty() {
                    // New leaders send empty entries
                    continue;
                }

                assert_eq!(entry.entry_type, EntryType::EntryNormal);

                let mut pending_responses = self.pending_responses.lock().unwrap();

                let (mut builder, sender) = pending_responses.remove(&entry.index).unwrap();

                let local_storage = LocalStorage::new(self.context.clone());
                let request = get_root_as_generic_request(&entry.data);
                handler(request, &local_storage, &self.context, &mut builder);
                info!("Committed write index {}", entry.index);

                sender.send(builder).ok().unwrap();
            }
        }

        raft_node.advance(ready);

        if applied_index > 0 {
            return Ok(Some(applied_index));
        } else {
            return Ok(None);
        }
    }

    fn _propose(&self, data: Vec<u8>) -> raft::Result<u64> {
        let mut raft_node = self.raft_node.lock().unwrap();
        assert_eq!(self.node_id, raft_node.raft.leader_id);
        raft_node.propose(vec![], data)?;
        return Ok(raft_node.raft.raft_log.last_index());
    }

    pub fn initialize(&self) {
        for _ in 0..100 {
            let messages = self.get_outgoing_raft_messages().unwrap();
            self.apply_messages(&messages).unwrap();
            self.process_raft_queue().unwrap();
            {
                let mut raft_node = self.raft_node.lock().unwrap();
                // TODO: hack. remove this.
                raft_node.tick();
                if raft_node.raft.leader_id > 0 {
                    break;
                }
            }
            // Wait until there is a leader
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    fn wait_for_commit(&self, wait_index: u64) -> raft::Result<()> {
        for _ in 0..100 {
            let applied_index = self.process_raft_queue()?;
            if let Some(index) = applied_index {
                if index >= wait_index {
                    return Ok(());
                }
            }
            let messages = self.get_outgoing_raft_messages().unwrap();
            if !messages.is_empty() {
                self.apply_messages(&messages).unwrap();
            }

            {
                let mut raft_node = self.raft_node.lock().unwrap();
                raft_node.tick();
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        // TODO. For now just timeout after 10s
        Err(raft::Error::ViolatesContract("timeout".to_string()))
    }

    pub fn propose(
        &self,
        request: GenericRequest,
        builder: FlatBufferBuilder<'a>,
    ) -> impl Future<Item = FlatBufferBuilder<'a>, Error = ()> {
        assert!(is_write_request(request.request_type()));
        let index = self._propose(request._tab.buf.to_vec()).unwrap();

        // TODO: fix race. proposal could get accepted before this builder is inserted into response map
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(index, (builder, sender));
        }

        // Force immediate processing, since we know there's a proposal
        self.process_raft_queue().unwrap();

        return receiver.map_err(|_| ());
    }
}
