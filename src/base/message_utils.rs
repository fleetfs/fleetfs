pub struct RequestMetaInfo {
    pub raft_group: Option<u16>,
    pub inode: Option<u64>, // Some if the request accesses a single inode (i.e. None for Rename)
    pub lock_id: Option<u64>,
    pub access_type: AccessType, // Used to determine locks to acquire
    pub distribution_requirement: DistributionRequirement,
}

pub enum AccessType {
    ReadData,
    ReadMetadata,
    LockMetadata,
    WriteMetadata,
    WriteDataAndMetadata,
    NoAccess,
}

// Where this message can be processed
pub enum DistributionRequirement {
    Any,                    // Any node can process this message
    TransactionCoordinator, // Any node can process this message by acting as a transcation coordinator
    RaftGroup,              // Must be processed by a specific rgroup
    Node,                   // Must be processed by a specific node
}
