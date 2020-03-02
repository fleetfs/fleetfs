use crate::generated::*;
use crate::storage::metadata_storage::InodeAttributes;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::utils::{
    check_access, empty_response, fileattr_response_to_inode_attributes,
    finalize_request_without_prefix, finalize_response, finalize_response_without_prefix,
    response_or_error, FlatBufferWithResponse,
};
use flatbuffers::{FlatBufferBuilder, SIZE_UOFFSET};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

fn hardlink_increment_request<'a>(
    inode: u64,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = HardlinkIncrementRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(
        builder,
        RequestType::HardlinkIncrementRequest,
        finish_offset,
    );

    get_root_as_generic_request(builder.finished_data())
}

fn hardlink_rollback_request<'a>(
    inode: u64,
    last_modified: Timestamp,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = HardlinkRollbackRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    request_builder.add_last_modified_time(&last_modified);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::HardlinkRollbackRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn create_inode_request<'a>(
    parent: Option<u64>,
    uid: u32,
    gid: u32,
    mode: u16,
    kind: FileKind,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = CreateInodeRequestBuilder::new(builder);
    request_builder.add_uid(uid);
    request_builder.add_gid(gid);
    request_builder.add_mode(mode);
    request_builder.add_kind(kind);
    request_builder.add_parent(parent.unwrap_or(0));
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::CreateInodeRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn create_link_request<'a>(
    parent: u64,
    name: &'_ str,
    inode: u64,
    kind: FileKind,
    lock_id: Option<u64>,
    context: UserContext,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let builder_name = builder.create_string(name);
    let mut request_builder = CreateLinkRequestBuilder::new(builder);
    request_builder.add_parent(parent);
    request_builder.add_name(builder_name);
    request_builder.add_inode(inode);
    request_builder.add_kind(kind);
    let builder_lock_id;
    if let Some(id) = lock_id {
        builder_lock_id = OptionalULong::new(id);
        request_builder.add_lock_id(&builder_lock_id);
    }
    request_builder.add_context(&context);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::CreateLinkRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

fn decrement_inode_request<'a>(
    inode: u64,
    decrement_count: u32,
    lock_id: Option<u64>,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = DecrementInodeRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    request_builder.add_decrement_count(decrement_count);
    let builder_lock_id;
    if let Some(id) = lock_id {
        builder_lock_id = OptionalULong::new(id);
        request_builder.add_lock_id(&builder_lock_id);
    }
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(builder, RequestType::DecrementInodeRequest, finish_offset);

    get_root_as_generic_request(builder.finished_data())
}

async fn replace_link(
    parent: u64,
    name: &str,
    new_inode: u64,
    kind: FileKind,
    lock_id: u64,
    context: UserContext,
    raft: &LocalRaftGroupManager,
) -> Result<u64, ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let builder_name = builder.create_string(name);
    let mut request_builder = ReplaceLinkRequestBuilder::new(&mut builder);
    request_builder.add_parent(parent);
    request_builder.add_name(builder_name);
    request_builder.add_new_inode(new_inode);
    request_builder.add_kind(kind);
    let builder_lock_id = OptionalULong::new(lock_id);
    request_builder.add_lock_id(&builder_lock_id);
    request_builder.add_context(&context);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::ReplaceLinkRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(parent)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::InodeResponse);

    let inode = response.response_as_inode_response().unwrap().inode();

    Ok(inode)
}

async fn remove_link(
    parent: u64,
    name: &str,
    link_inode_and_uid: Option<(u64, u32)>,
    lock_id: Option<u64>,
    context: UserContext,
    raft: &LocalRaftGroupManager,
) -> Result<(u64, bool), ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let builder_name = builder.create_string(name);
    let mut request_builder = RemoveLinkRequestBuilder::new(&mut builder);
    request_builder.add_parent(parent);
    request_builder.add_name(builder_name);
    let builder_inode;
    let builder_uid;
    if let Some((link_inode, link_uid)) = link_inode_and_uid {
        builder_inode = OptionalULong::new(link_inode);
        builder_uid = OptionalUInt::new(link_uid);
        request_builder.add_link_inode(&builder_inode);
        request_builder.add_link_uid(&builder_uid);
    }
    let builder_lock_id;
    if let Some(id) = lock_id {
        builder_lock_id = OptionalULong::new(id);
        request_builder.add_lock_id(&builder_lock_id);
    }
    request_builder.add_context(&context);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::RemoveLinkRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(parent)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::RemoveLinkResponse);

    let inode = response.response_as_remove_link_response().unwrap().inode();
    let complete = response
        .response_as_remove_link_response()
        .unwrap()
        .processing_complete();

    Ok((inode, complete))
}

// TODO: should return some kind of guard object to prevent dropping the lock_id without unlocking it
async fn lock_inode(inode: u64, raft: &LocalRaftGroupManager) -> Result<u64, ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = LockRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::LockRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(inode)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::LockResponse);

    let lock_id = response.response_as_lock_response().unwrap().lock_id();

    Ok(lock_id)
}

async fn update_parent(
    inode: u64,
    new_parent: u64,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
) -> Result<(), ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = UpdateParentRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    request_builder.add_new_parent(new_parent);
    let builder_lock_id;
    if let Some(id) = lock_id {
        builder_lock_id = OptionalULong::new(id);
        request_builder.add_lock_id(&builder_lock_id);
    }
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(
        &mut builder,
        RequestType::UpdateParentRequest,
        finish_offset,
    );
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(inode)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::EmptyResponse);

    Ok(())
}

async fn update_metadata_changed_time(
    inode: u64,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
) -> Result<(), ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = UpdateMetadataChangedTimeRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    let builder_lock_id;
    if let Some(id) = lock_id {
        builder_lock_id = OptionalULong::new(id);
        request_builder.add_lock_id(&builder_lock_id);
    }
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(
        &mut builder,
        RequestType::UpdateMetadataChangedTimeRequest,
        finish_offset,
    );
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(inode)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::EmptyResponse);

    Ok(())
}

async fn unlock_inode(
    inode: u64,
    lock_id: u64,
    raft: &LocalRaftGroupManager,
) -> Result<(), ErrorCode> {
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = UnlockRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    request_builder.add_lock_id(lock_id);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::UnlockRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(inode)
        .propose(request, FlatBufferBuilder::new())
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(response.response_type(), ResponseType::EmptyResponse);

    Ok(())
}

async fn getattrs(
    inode: u64,
    raft: &LocalRaftGroupManager,
) -> Result<(InodeAttributes, u32), ErrorCode> {
    // TODO: need to send this request via peer client, so that it goes to the right rgroup
    let rgroup = raft.lookup_by_inode(inode);
    let latest_commit = rgroup.get_latest_commit_from_leader().await?;
    rgroup.sync(latest_commit).await?;

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(inode)
        .file_storage()
        .getattr(inode, FlatBufferBuilder::new())?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    let metadata = response
        .response_as_file_metadata_response()
        .ok_or(ErrorCode::BadResponse)?;

    return Ok((
        fileattr_response_to_inode_attributes(&metadata),
        metadata.directory_entries(),
    ));
}

async fn lookup(
    parent: u64,
    name: String,
    context: UserContext,
    raft: &LocalRaftGroupManager,
) -> Result<u64, ErrorCode> {
    // TODO: need to send this request via peer client, so that it goes to the right rgroup
    let rgroup = raft.lookup_by_inode(parent);
    let latest_commit = rgroup.get_latest_commit_from_leader().await?;
    rgroup.sync(latest_commit).await?;

    let (mut builder, response_type, response_offset) = raft
        .lookup_by_inode(parent)
        .file_storage()
        .lookup(parent, &name, context, FlatBufferBuilder::new())?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    let inode = response
        .response_as_inode_response()
        .ok_or(ErrorCode::BadResponse)?
        .inode();

    return Ok(inode);
}

async fn decrement_inode(
    inode: u64,
    count: u32,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
) {
    let mut decrement_request_builder = FlatBufferBuilder::new();
    let decrement_link_count =
        decrement_inode_request(inode, count, lock_id, &mut decrement_request_builder);
    // TODO: if this fails the inode will leak ;(
    raft.lookup_by_inode(inode)
        .propose(decrement_link_count, FlatBufferBuilder::new())
        .await
        .expect("Leaked inode");
}

fn rename_check_access(
    parent_attrs: &InodeAttributes,
    new_parent_attrs: &InodeAttributes,
    inode_attrs: &InodeAttributes,
    // Attributes and directory entry count if it's a directory
    existing_dest_inode_attrs: &Option<(InodeAttributes, u32)>,
    context: UserContext,
) -> Result<(), ErrorCode> {
    if !check_access(
        parent_attrs.uid,
        parent_attrs.gid,
        parent_attrs.mode,
        context.uid(),
        context.gid(),
        libc::W_OK as u32,
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
        libc::W_OK as u32,
    ) {
        return Err(ErrorCode::AccessDenied);
    }

    // "Sticky bit" handling in new_parent
    if new_parent_attrs.mode & libc::S_ISVTX as u16 != 0 {
        if let Some((attrs, _)) = existing_dest_inode_attrs {
            if context.uid() != 0
                && context.uid() != new_parent_attrs.uid
                && context.uid() != attrs.uid
            {
                return Err(ErrorCode::AccessDenied);
            }
        }
    }

    // Only overwrite an existing directory if it's empty
    if let Some((attrs, entry_count)) = existing_dest_inode_attrs {
        if attrs.kind == FileKind::Directory && *entry_count > 0 {
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
            libc::W_OK as u32,
        )
    {
        return Err(ErrorCode::AccessDenied);
    }

    Ok(())
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn rename_transaction<'a>(
    parent: u64,
    name: String,
    new_parent: u64,
    new_name: String,
    context: UserContext,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let locks: Arc<Mutex<HashSet<(u64, u64)>>> = Arc::new(Mutex::new(HashSet::new()));
    let result = rename_transaction_lock_context(
        parent,
        name,
        new_parent,
        new_name,
        context,
        locks.clone(),
        builder,
        raft.clone(),
    )
    .await;
    let to_unlock: Vec<(u64, u64)> = locks.lock().unwrap().drain().collect();
    for (inode, lock_id) in to_unlock {
        unlock_inode(inode, lock_id, &raft)
            .await
            .expect("Unlock failed");
    }
    return result;
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::cognitive_complexity)]
async fn rename_transaction_lock_context<'a>(
    parent: u64,
    name: String,
    new_parent: u64,
    new_name: String,
    context: UserContext,
    // Pairs of (inode, lock_id)
    lock_guard: Arc<Mutex<HashSet<(u64, u64)>>>,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    // TODO: since we acquire multiple locks in this function it could cause a deadlock.
    // We should acquire them in ascending order of inode
    let parent_lock_id = lock_inode(parent, &raft).await?;
    lock_guard.lock().unwrap().insert((parent, parent_lock_id));

    let new_parent_lock_id = if parent != new_parent {
        let lock_id = lock_inode(new_parent, &raft).await?;
        lock_guard.lock().unwrap().insert((new_parent, lock_id));
        lock_id
    } else {
        parent_lock_id
    };

    let inode = lookup(parent, name.clone(), context, &raft).await?;
    let inode_lock_id = lock_inode(inode, &raft).await?;
    lock_guard.lock().unwrap().insert((inode, inode_lock_id));

    let existing_dest_inode = match lookup(new_parent, new_name.clone(), context, &raft).await {
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
        let lock_id = lock_inode(inode, &raft).await?;
        lock_guard.lock().unwrap().insert((inode, lock_id));
        Some(lock_id)
    } else {
        None
    };

    // Perform access checks
    let (parent_attrs, _) = getattrs(parent, &raft).await?;
    let (new_parent_attrs, _) = getattrs(new_parent, &raft).await?;
    let (inode_attrs, _) = getattrs(inode, &raft).await?;
    let existing_inode_attrs = if let Some(ref inode) = existing_dest_inode {
        Some(getattrs(*inode, &raft).await?)
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
            &new_name,
            inode,
            inode_attrs.kind,
            new_parent_lock_id,
            context,
            &raft,
        )
        .await?;
        assert_eq!(old_inode, existing_inode);
        if existing_inode_attrs.as_ref().unwrap().0.kind == FileKind::Directory {
            assert_eq!(existing_inode_attrs.as_ref().unwrap().0.hardlinks, 2);
            decrement_inode(old_inode, 2, existing_inode_lock_id, &raft).await;
        } else {
            decrement_inode(old_inode, 1, existing_inode_lock_id, &raft).await;
        }
    } else {
        let mut builder = FlatBufferBuilder::new();
        let create_link = create_link_request(
            new_parent,
            &new_name,
            inode,
            inode_attrs.kind,
            Some(new_parent_lock_id),
            context,
            &mut builder,
        );

        raft.lookup_by_inode(new_parent)
            .propose(create_link, FlatBufferBuilder::new())
            .await?;
    }

    // TODO: this shouldn't be able to fail since we already performed access checks
    remove_link(
        parent,
        &name,
        Some((inode, inode_attrs.uid)),
        Some(parent_lock_id),
        context,
        &raft,
    )
    .await?;

    if inode_attrs.kind == FileKind::Directory {
        update_parent(inode, new_parent, Some(inode_lock_id), &raft).await?;
    }
    update_metadata_changed_time(inode, Some(inode_lock_id), &raft).await?;

    let (mut builder, response_type, offset) = empty_response(builder)?;
    finalize_response(&mut builder, response_type, offset);
    return Ok(FlatBufferWithResponse::new(builder));
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn rmdir_transaction<'a>(
    parent: u64,
    name: String,
    context: UserContext,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let mut inode = lookup(parent, name.clone(), context, &raft).await?;
    let mut complete = false;
    while !complete {
        // If the link removal didn't complete successful or with an error, then we need to
        // lock the target inode and lookup its uid to allow processing of "sticky bit"
        let lock_id = lock_inode(inode, &raft).await?;
        match getattrs(inode, &raft).await {
            Ok((attrs, directory_entries)) => {
                if directory_entries > 0 {
                    unlock_inode(inode, lock_id, &raft).await?;
                    return Err(ErrorCode::NotEmpty);
                }
                match remove_link(
                    parent,
                    &name,
                    Some((inode, attrs.uid)),
                    None,
                    context,
                    &raft,
                )
                .await
                {
                    Ok((lookup_inode, is_complete)) => {
                        unlock_inode(inode, lock_id, &raft).await?;
                        inode = lookup_inode;
                        complete = is_complete;
                    }
                    Err(error_code) => {
                        unlock_inode(inode, lock_id, &raft).await?;
                        return Err(error_code);
                    }
                }
            }
            Err(error_code) => {
                unlock_inode(inode, lock_id, &raft).await?;
                return Err(error_code);
            }
        }
    }

    // TODO: can remove this roundtrip. It's just for debuggability
    let hardlinks = getattrs(inode, &raft).await?.0.hardlinks;
    assert_eq!(hardlinks, 2);

    let mut decrement_request_builder = FlatBufferBuilder::new();
    let decrement_link_count =
        decrement_inode_request(inode, 2, None, &mut decrement_request_builder);
    // TODO: if this fails the inode will leak ;(
    raft.lookup_by_inode(inode)
        .propose(decrement_link_count, FlatBufferBuilder::new())
        .await?;

    let (mut builder, response_type, offset) = empty_response(builder)?;
    finalize_response(&mut builder, response_type, offset);
    return Ok(FlatBufferWithResponse::new(builder));
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn unlink_transaction<'a>(
    parent: u64,
    name: String,
    context: UserContext,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    // Try to remove the link. The result of this might be indeterminate, since "sticky bit"
    // can require that we know the uid of the inode
    let (link_inode, complete) = remove_link(parent, &name, None, None, context, &raft).await?;

    if complete {
        let mut decrement_request_builder = FlatBufferBuilder::new();
        let decrement_link_count =
            decrement_inode_request(link_inode, 1, None, &mut decrement_request_builder);
        // TODO: if this fails the inode will leak ;(
        raft.lookup_by_inode(link_inode)
            .propose(decrement_link_count, FlatBufferBuilder::new())
            .await?;
    } else {
        let mut inode = link_inode;
        let mut complete = complete;
        while !complete {
            // If the link removal didn't complete successful or with an error, then we need to
            // lock the target inode and lookup its uid to allow processing of "sticky bit"
            let lock_id = lock_inode(inode, &raft).await?;
            match getattrs(inode, &raft).await {
                Ok((attrs, _)) => {
                    match remove_link(
                        parent,
                        &name,
                        Some((inode, attrs.uid)),
                        None,
                        context,
                        &raft,
                    )
                    .await
                    {
                        Ok((lookup_inode, is_complete)) => {
                            unlock_inode(inode, lock_id, &raft).await?;
                            inode = lookup_inode;
                            complete = is_complete;
                        }
                        Err(error_code) => {
                            unlock_inode(inode, lock_id, &raft).await?;
                            return Err(error_code);
                        }
                    }
                }
                Err(error_code) => {
                    unlock_inode(inode, lock_id, &raft).await?;
                    return Err(error_code);
                }
            }
        }
        let mut decrement_request_builder = FlatBufferBuilder::new();
        let decrement_link_count =
            decrement_inode_request(inode, 1, None, &mut decrement_request_builder);
        // TODO: if this fails the inode will leak ;(
        raft.lookup_by_inode(inode)
            .propose(decrement_link_count, FlatBufferBuilder::new())
            .await?;
    }

    let (mut builder, response_type, offset) = empty_response(builder)?;
    finalize_response(&mut builder, response_type, offset);
    return Ok(FlatBufferWithResponse::new(builder));
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
#[allow(clippy::too_many_arguments)]
pub async fn create_transaction<'a>(
    parent: u64,
    name: String,
    uid: u32,
    gid: u32,
    mode: u16,
    kind: FileKind,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let mut create_request_builder = FlatBufferBuilder::new();
    // First create inode. This effectively begins the transaction.
    let create_inode = create_inode_request(
        Some(parent),
        uid,
        gid,
        mode,
        kind,
        &mut create_request_builder,
    );

    // This will be the response back to the client, so use builder object
    let (mut builder, response_type, response_offset) = raft
        .least_loaded_group()
        .propose(create_inode, builder)
        .await?;
    finalize_response(&mut builder, response_type, response_offset);
    // Skip the first SIZE_UOFFSET bytes because that's the size prefix
    let created_inode_response = response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
    assert_eq!(
        created_inode_response.response_type(),
        ResponseType::FileMetadataResponse
    );

    let inode = created_inode_response
        .response_as_file_metadata_response()
        .unwrap()
        .inode();
    let link_count = created_inode_response
        .response_as_file_metadata_response()
        .unwrap()
        .hard_links();

    // Second create the link
    let mut link_request_builder = FlatBufferBuilder::new();
    let create_link = create_link_request(
        parent,
        &name,
        inode,
        kind,
        None,
        UserContext::new(uid, gid),
        &mut link_request_builder,
    );

    match raft
        .lookup_by_inode(parent)
        .propose(create_link, FlatBufferBuilder::new())
        .await
    {
        Ok((mut response_buffer, response_type, response_offset)) => {
            finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
            if response_or_error(response_buffer.finished_data()).is_err() {
                // Rollback the transaction
                let mut internal_request_builder = FlatBufferBuilder::new();
                let rollback =
                    decrement_inode_request(inode, link_count, None, &mut internal_request_builder);
                // TODO: if this fails the inode will leak ;(
                raft.lookup_by_inode(inode)
                    .propose(rollback, FlatBufferBuilder::new())
                    .await?;
            }

            // This is the response back to the client
            return Ok(FlatBufferWithResponse::new(builder));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback =
                decrement_inode_request(inode, link_count, None, &mut internal_request_builder);
            // TODO: if this fails the inode will leak ;(
            raft.lookup_by_inode(inode)
                .propose(rollback, FlatBufferBuilder::new())
                .await?;
            return Err(error_code);
        }
    }
}

// TODO: persist transaction state, so that it doesn't get lost if the coordinating machine dies
// in the middle
pub async fn hardlink_transaction<'a, 'b>(
    hardlink_request: HardlinkRequest<'a>,
    mut builder: FlatBufferBuilder<'b>,
    raft: Arc<LocalRaftGroupManager>,
) -> Result<FlatBufferWithResponse<'b>, ErrorCode> {
    let mut internal_request_builder = FlatBufferBuilder::new();

    // First increment the link count on the inode to ensure it can't be deleted
    // this effectively begins the transaction.
    let increment =
        hardlink_increment_request(hardlink_request.inode(), &mut internal_request_builder);

    let (mut response_buffer, response_type, response_offset) = raft
        .lookup_by_inode(hardlink_request.inode())
        .propose(increment, FlatBufferBuilder::new())
        .await?;
    finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
    let increment_response = response_or_error(response_buffer.finished_data())?;
    assert_eq!(
        increment_response.response_type(),
        ResponseType::HardlinkTransactionResponse
    );
    let transaction_response = increment_response
        .response_as_hardlink_transaction_response()
        .unwrap();
    let last_modified = *transaction_response.last_modified_time();
    let inode_kind = transaction_response.kind();

    // Second create the new link
    let mut internal_request_builder = FlatBufferBuilder::new();
    let create_link = create_link_request(
        hardlink_request.new_parent(),
        hardlink_request.new_name(),
        hardlink_request.inode(),
        inode_kind,
        None,
        *hardlink_request.context(),
        &mut internal_request_builder,
    );

    match raft
        .lookup_by_inode(hardlink_request.new_parent())
        .propose(create_link, FlatBufferBuilder::new())
        .await
    {
        Ok((mut response_buffer, response_type, response_offset)) => {
            finalize_response_without_prefix(&mut response_buffer, response_type, response_offset);
            if response_or_error(response_buffer.finished_data()).is_err() {
                // Rollback the transaction
                let mut internal_request_builder = FlatBufferBuilder::new();
                let rollback = hardlink_rollback_request(
                    hardlink_request.inode(),
                    last_modified,
                    &mut internal_request_builder,
                );
                // TODO: if this fails the filesystem is corrupted ;( since the link count
                // may not have been decremented
                raft.lookup_by_inode(hardlink_request.inode())
                    .propose(rollback, FlatBufferBuilder::new())
                    .await?;
            }

            // This is the response back to the client
            let mut response_builder = FileMetadataResponseBuilder::new(&mut builder);
            let attrs = transaction_response.attr_response();
            response_builder.add_inode(attrs.inode());
            response_builder.add_size_bytes(attrs.size_bytes());
            response_builder.add_size_blocks(attrs.size_blocks());
            response_builder.add_last_access_time(attrs.last_access_time());
            response_builder.add_last_modified_time(attrs.last_modified_time());
            response_builder.add_last_metadata_modified_time(attrs.last_metadata_modified_time());
            response_builder.add_kind(attrs.kind());
            response_builder.add_mode(attrs.mode());
            response_builder.add_hard_links(attrs.hard_links());
            response_builder.add_user_id(attrs.user_id());
            response_builder.add_group_id(attrs.group_id());
            response_builder.add_device_id(attrs.device_id());

            let offset = response_builder.finish().as_union_value();
            finalize_response(&mut builder, ResponseType::FileMetadataResponse, offset);
            return Ok(FlatBufferWithResponse::new(builder));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback = hardlink_rollback_request(
                hardlink_request.inode(),
                last_modified,
                &mut internal_request_builder,
            );
            // TODO: if this fails the filesystem is corrupted ;( since the link count
            // may not have been decremented
            raft.lookup_by_inode(hardlink_request.inode())
                .propose(rollback, FlatBufferBuilder::new())
                .await?;
            return Err(error_code);
        }
    }
}
