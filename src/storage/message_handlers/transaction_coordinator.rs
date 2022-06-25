use crate::base::{check_access, response_or_error};
use crate::base::{
    ArchivedRkyvGenericResponse, EntryMetadata, ErrorCode, FileKind, InodeUidPair,
    RkyvGenericResponse, RkyvRequest, UserContext,
};
use crate::client::RemoteRaftGroups;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use rand::Rng;
use rkyv::{AlignedVec, Deserialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

async fn propose(
    inode: u64,
    request: &RkyvRequest,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<AlignedVec, ErrorCode> {
    if raft.inode_stored_locally(inode) {
        let rkyv_response = raft.lookup_by_inode(inode).propose(request).await?;
        let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_response).unwrap();
        response_or_error(&rkyv_bytes)?;
        Ok(rkyv_bytes)
    } else {
        let response = remote_rafts
            .propose(inode, request)
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;
        response_or_error(&response)?;
        Ok(response)
    }
}

#[allow(clippy::too_many_arguments)]
async fn replace_link(
    parent: u64,
    name: &str,
    new_inode: u64,
    kind: FileKind,
    lock_id: u64,
    context: UserContext,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<u64, ErrorCode> {
    let request = RkyvRequest::ReplaceLink {
        parent,
        name: name.to_string(),
        new_inode,
        kind,
        lock_id: Some(lock_id),
        context,
    };

    let response_data = propose(parent, &request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    Ok(response.as_inode_response().unwrap())
}

async fn remove_link(
    parent: u64,
    name: &str,
    link_inode_and_uid: Option<(u64, u32)>,
    lock_id: Option<u64>,
    context: UserContext,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<(u64, bool), ErrorCode> {
    let request = RkyvRequest::RemoveLink {
        parent,
        name: name.to_string(),
        link_inode_and_uid: link_inode_and_uid.map(|(inode, uid)| InodeUidPair::new(inode, uid)),
        lock_id,
        context,
    };
    let response_data = propose(parent, &request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    if let ArchivedRkyvGenericResponse::RemovedInode { id, complete } = response {
        Ok((id.into(), *complete))
    } else {
        unreachable!();
    }
}

// TODO: should return some kind of guard object to prevent dropping the lock_id without unlocking it
async fn lock_inode(
    inode: u64,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<u64, ErrorCode> {
    let response_data = propose(inode, &RkyvRequest::Lock { inode }, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    if let ArchivedRkyvGenericResponse::Lock { lock_id } = response {
        Ok(lock_id.into())
    } else {
        unreachable!();
    }
}

async fn update_parent(
    inode: u64,
    new_parent: u64,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<(), ErrorCode> {
    let request = RkyvRequest::UpdateParent {
        inode,
        new_parent,
        lock_id,
    };

    let response_data = propose(inode, &request, raft, remote_rafts).await?;
    response_or_error(&response_data)?
        .as_empty_response()
        .expect("expected Empty");

    Ok(())
}

async fn update_metadata_changed_time(
    inode: u64,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<(), ErrorCode> {
    let request = RkyvRequest::UpdateMetadataChangedTime { inode, lock_id };

    let response_data = propose(inode, &request, raft, remote_rafts).await?;
    response_or_error(&response_data)?
        .as_empty_response()
        .expect("expected Empty");

    Ok(())
}

async fn unlock_inode(
    inode: u64,
    lock_id: u64,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<(), ErrorCode> {
    let response_data = propose(
        inode,
        &RkyvRequest::Unlock { inode, lock_id },
        raft,
        remote_rafts,
    )
    .await?;
    response_or_error(&response_data)?
        .as_empty_response()
        .expect("expected Empty");

    Ok(())
}

struct FileOrDirAttrs {
    inode: u64,
    kind: FileKind,
    // Permissions and special mode bits
    mode: u16,
    hardlinks: u32,
    uid: u32,
    gid: u32,
    directory_entries: u32,
}

impl FileOrDirAttrs {
    fn new(response: &EntryMetadata) -> FileOrDirAttrs {
        FileOrDirAttrs {
            inode: response.inode,
            kind: response.kind,
            mode: response.mode,
            hardlinks: response.hard_links,
            uid: response.user_id,
            gid: response.group_id,
            directory_entries: response.directory_entries.unwrap_or_default(),
        }
    }
}

// TODO: even these read-only RPCs add a significant performance cost. Maybe they can be optimized?
async fn getattrs(
    inode: u64,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<FileOrDirAttrs, ErrorCode> {
    if raft.inode_stored_locally(inode) {
        let rgroup = raft.lookup_by_inode(inode);
        let latest_commit = rgroup.get_latest_commit_from_leader().await?;
        rgroup.sync(latest_commit).await?;

        let response = raft.lookup_by_inode(inode).file_storage().getattr(inode)?;
        match response {
            RkyvGenericResponse::EntryMetadata(metadata) => Ok(FileOrDirAttrs::new(&metadata)),
            _ => Err(ErrorCode::BadResponse),
        }
    } else {
        let rkyv_request = RkyvRequest::GetAttr { inode };

        let response_data = remote_rafts
            .forward_request(&rkyv_request)
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;

        let response = response_or_error(&response_data)?;
        let metadata = response.as_attr_response().ok_or(ErrorCode::BadResponse)?;

        Ok(FileOrDirAttrs::new(&metadata))
    }
}

// TODO: even these read-only RPCs add a significant performance cost. Maybe they can be optimized?
async fn lookup(
    parent: u64,
    name: String,
    context: UserContext,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<u64, ErrorCode> {
    if raft.inode_stored_locally(parent) {
        let rgroup = raft.lookup_by_inode(parent);
        let latest_commit = rgroup.get_latest_commit_from_leader().await?;
        rgroup.sync(latest_commit).await?;

        let inode_response = raft
            .lookup_by_inode(parent)
            .file_storage()
            .lookup(parent, &name, context)?;

        match inode_response {
            RkyvGenericResponse::Inode { id } => Ok(id),
            _ => Err(ErrorCode::BadResponse),
        }
    } else {
        let response_data = remote_rafts
            .forward_request(&RkyvRequest::Lookup {
                parent,
                name,
                context,
            })
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;

        let response = response_or_error(&response_data)?;

        response.as_inode_response().ok_or(ErrorCode::BadResponse)
    }
}

async fn decrement_inode(
    inode: u64,
    count: u32,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) {
    let request = RkyvRequest::DecrementInode {
        inode,
        decrement_count: count,
        lock_id,
    };
    // TODO: if this fails the inode will leak ;(
    let response_data = propose(inode, &request, raft, remote_rafts)
        .await
        .expect("Leaked inode");
    response_or_error(&response_data)
        .expect("Leaked inode")
        .as_empty_response()
        .expect("expected Empty");
}

fn rename_check_access(
    parent_attrs: &FileOrDirAttrs,
    new_parent_attrs: &FileOrDirAttrs,
    inode_attrs: &FileOrDirAttrs,
    // Attributes and directory entry count if it's a directory
    existing_dest_inode_attrs: &Option<FileOrDirAttrs>,
    context: UserContext,
) -> Result<(), ErrorCode> {
    if !check_access(
        parent_attrs.uid,
        parent_attrs.gid,
        parent_attrs.mode,
        context.uid(),
        context.gid(),
        libc::W_OK,
    ) {
        return Err(ErrorCode::AccessDenied);
    }

    // "Sticky bit" handling
    if parent_attrs.mode & libc::S_ISVTX as u16 != 0
        && context.uid() != 0
        && context.uid() != parent_attrs.uid
        && context.uid() != inode_attrs.uid
    {
        return Err(ErrorCode::AccessDenied);
    }

    if !check_access(
        new_parent_attrs.uid,
        new_parent_attrs.gid,
        new_parent_attrs.mode,
        context.uid(),
        context.gid(),
        libc::W_OK,
    ) {
        return Err(ErrorCode::AccessDenied);
    }

    // "Sticky bit" handling in new_parent
    if new_parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
        if let Some(attrs) = existing_dest_inode_attrs {
            if context.uid() != 0
                && context.uid() != new_parent_attrs.uid
                && context.uid() != attrs.uid
            {
                return Err(ErrorCode::AccessDenied);
            }
        }
    }

    // Only overwrite an existing directory if it's empty
    if let Some(attrs) = existing_dest_inode_attrs {
        if attrs.kind == FileKind::Directory && attrs.directory_entries > 0 {
            return Err(ErrorCode::NotEmpty);
        }
    }

    // Only move an existing directory to a new parent, if we have write access to it,
    // because that will change the ".." link in it
    if inode_attrs.kind == FileKind::Directory
        && parent_attrs.inode != new_parent_attrs.inode
        && !check_access(
            inode_attrs.uid,
            inode_attrs.gid,
            inode_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        )
    {
        return Err(ErrorCode::AccessDenied);
    }

    Ok(())
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
#[allow(clippy::too_many_arguments)]
pub async fn rename_transaction<'a>(
    parent: u64,
    name: &str,
    new_parent: u64,
    new_name: &str,
    context: UserContext,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    let locks: Arc<Mutex<HashSet<(u64, u64)>>> = Arc::new(Mutex::new(HashSet::new()));
    let result = rename_transaction_lock_context(
        parent,
        name,
        new_parent,
        new_name,
        context,
        locks.clone(),
        raft.clone(),
        remote_rafts.clone(),
    )
    .await;
    let to_unlock: Vec<(u64, u64)> = locks.lock().unwrap().drain().collect();
    for (inode, lock_id) in to_unlock {
        unlock_inode(inode, lock_id, &raft, &remote_rafts)
            .await
            .expect("Unlock failed");
    }
    result
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::cognitive_complexity)]
async fn rename_transaction_lock_context<'a>(
    parent: u64,
    name: &str,
    new_parent: u64,
    new_name: &str,
    context: UserContext,
    // Pairs of (inode, lock_id)
    lock_guard: Arc<Mutex<HashSet<(u64, u64)>>>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    // TODO: since we acquire multiple locks in this function it could cause a deadlock.
    // We should acquire them in ascending order of inode
    let parent_lock_id = lock_inode(parent, &raft, &remote_rafts).await?;
    lock_guard.lock().unwrap().insert((parent, parent_lock_id));

    let new_parent_lock_id = if parent != new_parent {
        let lock_id = lock_inode(new_parent, &raft, &remote_rafts).await?;
        lock_guard.lock().unwrap().insert((new_parent, lock_id));
        lock_id
    } else {
        parent_lock_id
    };

    let inode = lookup(parent, name.to_string(), context, &raft, &remote_rafts).await?;
    let inode_lock_id = lock_inode(inode, &raft, &remote_rafts).await?;
    lock_guard.lock().unwrap().insert((inode, inode_lock_id));

    let existing_dest_inode = match lookup(
        new_parent,
        new_name.to_string(),
        context,
        &raft,
        &remote_rafts,
    )
    .await
    {
        Ok(inode) => Some(inode),
        Err(error_code) => {
            if error_code == ErrorCode::DoesNotExist {
                None
            } else {
                return Err(error_code);
            }
        }
    };
    let existing_inode_lock_id = if let Some(inode) = existing_dest_inode {
        let lock_id = lock_inode(inode, &raft, &remote_rafts).await?;
        lock_guard.lock().unwrap().insert((inode, lock_id));
        Some(lock_id)
    } else {
        None
    };

    // Perform access checks
    let parent_attrs = getattrs(parent, &raft, &remote_rafts).await?;
    let new_parent_attrs = getattrs(new_parent, &raft, &remote_rafts).await?;
    let inode_attrs = getattrs(inode, &raft, &remote_rafts).await?;
    let existing_inode_attrs = if let Some(ref inode) = existing_dest_inode {
        Some(getattrs(*inode, &raft, &remote_rafts).await?)
    } else {
        None
    };
    rename_check_access(
        &parent_attrs,
        &new_parent_attrs,
        &inode_attrs,
        &existing_inode_attrs,
        context,
    )?;

    if let Some(existing_inode) = existing_dest_inode {
        let old_inode = replace_link(
            new_parent,
            new_name,
            inode,
            inode_attrs.kind,
            new_parent_lock_id,
            context,
            &raft,
            &remote_rafts,
        )
        .await?;
        assert_eq!(old_inode, existing_inode);
        if existing_inode_attrs.as_ref().unwrap().kind == FileKind::Directory {
            assert_eq!(existing_inode_attrs.as_ref().unwrap().hardlinks, 2);
            decrement_inode(old_inode, 2, existing_inode_lock_id, &raft, &remote_rafts).await;
        } else {
            decrement_inode(old_inode, 1, existing_inode_lock_id, &raft, &remote_rafts).await;
        }
    } else {
        let create_link = RkyvRequest::CreateLink {
            parent: new_parent,
            name: new_name.to_string(),
            inode,
            kind: inode_attrs.kind,
            lock_id: Some(new_parent_lock_id),
            context,
        };

        let response_data = propose(new_parent, &create_link, &raft, &remote_rafts).await?;
        response_or_error(&response_data)?;
    }

    // TODO: this shouldn't be able to fail since we already performed access checks
    remove_link(
        parent,
        name,
        Some((inode, inode_attrs.uid)),
        Some(parent_lock_id),
        context,
        &raft,
        &remote_rafts,
    )
    .await?;

    if inode_attrs.kind == FileKind::Directory {
        update_parent(inode, new_parent, Some(inode_lock_id), &raft, &remote_rafts).await?;
    }
    update_metadata_changed_time(inode, Some(inode_lock_id), &raft, &remote_rafts).await?;

    Ok(RkyvGenericResponse::Empty)
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn rmdir_transaction<'a>(
    parent: u64,
    name: &str,
    context: UserContext,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    let mut inode = lookup(parent, name.to_string(), context, &raft, &remote_rafts).await?;
    let mut complete = false;
    while !complete {
        // If the link removal didn't complete successful or with an error, then we need to
        // lock the target inode and lookup its uid to allow processing of "sticky bit"
        let lock_id = lock_inode(inode, &raft, &remote_rafts).await?;
        match getattrs(inode, &raft, &remote_rafts).await {
            Ok(attrs) => {
                if attrs.directory_entries > 0 {
                    unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                    return Err(ErrorCode::NotEmpty);
                }
                match remove_link(
                    parent,
                    name,
                    Some((inode, attrs.uid)),
                    None,
                    context,
                    &raft,
                    &remote_rafts,
                )
                .await
                {
                    Ok((lookup_inode, is_complete)) => {
                        unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                        inode = lookup_inode;
                        complete = is_complete;
                    }
                    Err(error_code) => {
                        unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                        return Err(error_code);
                    }
                }
            }
            Err(error_code) => {
                unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                return Err(error_code);
            }
        }
    }

    // TODO: can remove this roundtrip. It's just for debuggability
    let hardlinks = getattrs(inode, &raft, &remote_rafts).await?.hardlinks;
    assert_eq!(hardlinks, 2);

    let decrement_link_count = RkyvRequest::DecrementInode {
        inode,
        decrement_count: 2,
        lock_id: None,
    };
    // TODO: if this fails the inode will leak ;(
    let response_data = propose(inode, &decrement_link_count, &raft, &remote_rafts).await?;
    response_or_error(&response_data)?;

    Ok(RkyvGenericResponse::Empty)
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn unlink_transaction<'a>(
    parent: u64,
    name: &str,
    context: UserContext,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    // Try to remove the link. The result of this might be indeterminate, since "sticky bit"
    // can require that we know the uid of the inode
    let (link_inode, complete) =
        remove_link(parent, name, None, None, context, &raft, &remote_rafts).await?;

    if complete {
        let decrement_link_count = RkyvRequest::DecrementInode {
            inode: link_inode,
            decrement_count: 1,
            lock_id: None,
        };
        // TODO: if this fails the inode will leak ;(
        let response_data =
            propose(link_inode, &decrement_link_count, &raft, &remote_rafts).await?;
        response_or_error(&response_data)?;
    } else {
        let mut inode = link_inode;
        let mut complete = complete;
        while !complete {
            // If the link removal didn't complete successful or with an error, then we need to
            // lock the target inode and lookup its uid to allow processing of "sticky bit"
            let lock_id = lock_inode(inode, &raft, &remote_rafts).await?;
            match getattrs(inode, &raft, &remote_rafts).await {
                Ok(attrs) => {
                    match remove_link(
                        parent,
                        name,
                        Some((inode, attrs.uid)),
                        None,
                        context,
                        &raft,
                        &remote_rafts,
                    )
                    .await
                    {
                        Ok((lookup_inode, is_complete)) => {
                            unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                            inode = lookup_inode;
                            complete = is_complete;
                        }
                        Err(error_code) => {
                            unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                            return Err(error_code);
                        }
                    }
                }
                Err(error_code) => {
                    unlock_inode(inode, lock_id, &raft, &remote_rafts).await?;
                    return Err(error_code);
                }
            }
        }
        let decrement_link_count = RkyvRequest::DecrementInode {
            inode,
            decrement_count: 1,
            lock_id: None,
        };
        // TODO: if this fails the inode will leak ;(
        let response_data = propose(inode, &decrement_link_count, &raft, &remote_rafts).await?;
        response_or_error(&response_data)?;
    }

    Ok(RkyvGenericResponse::Empty)
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
#[allow(clippy::too_many_arguments)]
pub async fn create_transaction<'a>(
    parent: u64,
    name: &str,
    uid: u32,
    gid: u32,
    mode: u16,
    kind: FileKind,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    // First create inode. This effectively begins the transaction.
    // TODO: actually load balance
    let raft_group = rand::thread_rng().gen_range(0..remote_rafts.get_total_raft_groups());
    let create_inode = RkyvRequest::CreateInode {
        raft_group,
        parent,
        uid,
        gid,
        mode,
        kind,
    };

    // This will be the response back to the client
    let create_response_data = remote_rafts
        .propose_to_specific_group(raft_group, &create_inode)
        .await
        .map_err(|_| ErrorCode::Uncategorized)?;
    let response = response_or_error(&create_response_data)?;
    let created_inode_response = response
        .as_attr_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap();

    let inode = created_inode_response.inode;
    let link_count = created_inode_response.hard_links;
    // TODO: optimize out deserialize
    let client_response = RkyvGenericResponse::EntryMetadata(created_inode_response);

    // Second create the link
    let create_link = RkyvRequest::CreateLink {
        parent,
        name: name.to_string(),
        inode,
        kind,
        lock_id: None,
        context: UserContext::new(uid, gid),
    };

    match propose(parent, &create_link, &raft, &remote_rafts).await {
        Ok(_) => {
            // This is the response back to the client
            Ok(client_response)
        }
        Err(error_code) => {
            // Rollback the transaction
            let rollback = RkyvRequest::DecrementInode {
                inode,
                decrement_count: link_count,
                lock_id: None,
            };
            // TODO: if this fails the inode will leak ;(
            let response_data = propose(inode, &rollback, &raft, &remote_rafts).await?;
            response_or_error(&response_data)?;
            Err(error_code)
        }
    }
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn hardlink_transaction(
    inode: u64,
    new_parent: u64,
    new_name: &str,
    context: UserContext,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<RkyvGenericResponse, ErrorCode> {
    // First increment the link count on the inode to ensure it can't be deleted
    // this effectively begins the transaction.
    let increment = RkyvRequest::HardlinkIncrement { inode };

    let response_data = propose(inode, &increment, &raft, &remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    let (rollback, attrs) = match response {
        ArchivedRkyvGenericResponse::HardlinkTransaction {
            rollback_last_modified,
            attrs,
        } => (rollback_last_modified, attrs),
        _ => return Err(ErrorCode::BadResponse),
    };

    // Second create the new link
    let create_link = RkyvRequest::CreateLink {
        parent: new_parent,
        name: new_name.to_string(),
        inode,
        kind: (&attrs.kind).into(),
        lock_id: None,
        context,
    };

    let rollback_request = RkyvRequest::HardlinkRollback {
        inode,
        last_modified_time: rollback.into(),
    };

    match propose(new_parent, &create_link, &raft, &remote_rafts).await {
        Ok(response_data) => {
            if response_or_error(&response_data).is_err() {
                // Rollback the transaction
                // TODO: if this fails the filesystem is corrupted ;( since the link count
                // may not have been decremented
                let response_data = propose(inode, &rollback_request, &raft, &remote_rafts).await?;
                response_or_error(&response_data)?;
            }

            // This is the response back to the client
            let entry: EntryMetadata = attrs.deserialize(&mut rkyv::Infallible).unwrap();
            Ok(RkyvGenericResponse::EntryMetadata(entry))
        }
        Err(error_code) => {
            // Rollback the transaction
            // TODO: if this fails the filesystem is corrupted ;( since the link count
            // may not have been decremented
            let response_data = propose(inode, &rollback_request, &raft, &remote_rafts).await?;
            response_or_error(&response_data)?;
            Err(error_code)
        }
    }
}
