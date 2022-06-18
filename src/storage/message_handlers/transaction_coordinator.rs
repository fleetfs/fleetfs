use crate::base::message_types::{
    ArchivedRkyvGenericResponse, EntryMetadata, ErrorCode, RkyvGenericResponse, RkyvRequest,
    Timestamp,
};
use crate::base::{
    check_access, empty_response, finalize_request_without_prefix, finalize_response,
    response_or_error, u8_to_file_kind, FlatBufferWithResponse,
};
use crate::client::RemoteRaftGroups;
use crate::generated::*;
use crate::storage::message_handlers::router::rkyv_response_to_fb;
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use flatbuffers::{FlatBufferBuilder, SIZE_UOFFSET};
use rand::Rng;
use rkyv::{AlignedVec, Deserialize};
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
    let fb_timestamp = FlatbufferTimestamp::new(last_modified.seconds, last_modified.nanos);
    let mut request_builder = HardlinkRollbackRequestBuilder::new(builder);
    request_builder.add_inode(inode);
    request_builder.add_last_modified_time(&fb_timestamp);
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
    total_raft_groups: u16,
    builder: &'a mut FlatBufferBuilder,
) -> GenericRequest<'a> {
    let mut request_builder = CreateInodeRequestBuilder::new(builder);
    // TODO: actually load balance
    request_builder.add_raft_group(rand::thread_rng().gen_range(0..total_raft_groups));
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

async fn propose(
    inode: u64,
    request: GenericRequest<'_>,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) -> Result<Vec<u8>, ErrorCode> {
    if raft.inode_stored_locally(inode) {
        let rkyv_response = raft
            .lookup_by_inode(inode)
            .propose_flatbuffer(request)
            .await?;
        let (mut builder, response_type, response_offset) =
            rkyv_response_to_fb(FlatBufferBuilder::new(), rkyv_response)?;
        finalize_response(&mut builder, response_type, response_offset);
        // Skip the first SIZE_UOFFSET bytes because that's the size prefix
        response_or_error(&builder.finished_data()[SIZE_UOFFSET..])?;
        // TODO: optimize this to_vec() copy
        return Ok(builder.finished_data()[SIZE_UOFFSET..].to_vec());
    } else {
        let response = remote_rafts
            .propose(inode, &request)
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;
        // TODO: optimize this to_vec() copy
        response_or_error(response.bytes())?;
        return Ok(response.bytes().to_vec());
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

    let response_data = propose(parent, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    assert_eq!(response.response_type(), ResponseType::RkyvResponse);

    let rkyv_data = response.response_as_rkyv_response().unwrap().rkyv_data();
    let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
    rkyv_aligned.extend_from_slice(rkyv_data);
    let inode_response = rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();
    Ok(inode_response.as_inode_response().unwrap())
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

    let response_data = propose(parent, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    assert_eq!(response.response_type(), ResponseType::RkyvResponse);

    let rkyv_data = response.response_as_rkyv_response().unwrap().rkyv_data();
    let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
    rkyv_aligned.extend_from_slice(rkyv_data);
    let removed_response = rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();
    if let ArchivedRkyvGenericResponse::RemovedInode { id, complete } = removed_response {
        return Ok((id.into(), *complete));
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
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = LockRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::LockRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let response_data = propose(inode, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    assert_eq!(response.response_type(), ResponseType::RkyvResponse);

    let rkyv_data = response.response_as_rkyv_response().unwrap().rkyv_data();
    let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
    rkyv_aligned.extend_from_slice(rkyv_data);
    let lock_response = rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();
    if let ArchivedRkyvGenericResponse::Lock { lock_id } = lock_response {
        return Ok(lock_id.into());
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

    let response_data = propose(inode, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    let rkyv_data = response
        .response_as_rkyv_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap()
        .rkyv_data();
    rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
        .unwrap()
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

    let response_data = propose(inode, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    let rkyv_data = response
        .response_as_rkyv_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap()
        .rkyv_data();
    rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
        .unwrap()
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
    let mut builder = FlatBufferBuilder::new();
    let mut request_builder = UnlockRequestBuilder::new(&mut builder);
    request_builder.add_inode(inode);
    request_builder.add_lock_id(lock_id);
    let finish_offset = request_builder.finish().as_union_value();
    // Don't need length prefix, since we're not serializing over network
    finalize_request_without_prefix(&mut builder, RequestType::UnlockRequest, finish_offset);
    let request = get_root_as_generic_request(builder.finished_data());

    let response_data = propose(inode, request, raft, remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    let rkyv_data = response
        .response_as_rkyv_response()
        .ok_or(ErrorCode::BadResponse)?
        .rkyv_data();
    rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
        .unwrap()
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
            kind: u8_to_file_kind(response.kind),
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
        // TODO: this seems wasteful
        let rkyv_bytes = rkyv::to_bytes::<_, 64>(&rkyv_request).unwrap();
        let archived_request = rkyv::check_archived_root::<RkyvRequest>(&rkyv_bytes).unwrap();

        let response_data = remote_rafts
            .forward_request(archived_request)
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;

        let response = response_or_error(response_data.bytes())?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let attr_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();
        let metadata = attr_response
            .as_attr_response()
            .ok_or(ErrorCode::BadResponse)?;

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
        let mut builder = FlatBufferBuilder::new();
        let builder_name = builder.create_string(&name);
        let mut request_builder = LookupRequestBuilder::new(&mut builder);
        request_builder.add_parent(parent);
        request_builder.add_name(builder_name);
        request_builder.add_context(&context);
        let finish_offset = request_builder.finish().as_union_value();
        finalize_request_without_prefix(&mut builder, RequestType::LookupRequest, finish_offset);
        let request = get_root_as_generic_request(builder.finished_data());
        let response_data = remote_rafts
            .forward_flatbuffer_request(&request)
            .await
            .map_err(|_| ErrorCode::Uncategorized)?;

        let response = response_or_error(response_data.bytes())?;
        let rkyv_data = response
            .response_as_rkyv_response()
            .ok_or(ErrorCode::BadResponse)?
            .rkyv_data();
        let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
        rkyv_aligned.extend_from_slice(rkyv_data);
        let inode_response =
            rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned).unwrap();

        inode_response
            .as_inode_response()
            .ok_or(ErrorCode::BadResponse)
    }
}

async fn decrement_inode(
    inode: u64,
    count: u32,
    lock_id: Option<u64>,
    raft: &LocalRaftGroupManager,
    remote_rafts: &RemoteRaftGroups,
) {
    let mut decrement_request_builder = FlatBufferBuilder::new();
    let decrement_link_count =
        decrement_inode_request(inode, count, lock_id, &mut decrement_request_builder);
    // TODO: if this fails the inode will leak ;(
    let response_data = propose(inode, decrement_link_count, raft, remote_rafts)
        .await
        .expect("Leaked inode");
    let response = response_or_error(&response_data).expect("Leaked inode");
    let rkyv_data = response
        .response_as_rkyv_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap()
        .rkyv_data();
    rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data)
        .unwrap()
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
    name: String,
    new_parent: u64,
    new_name: String,
    context: UserContext,
    builder: FlatBufferBuilder<'static>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
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
        remote_rafts.clone(),
    )
    .await;
    let to_unlock: Vec<(u64, u64)> = locks.lock().unwrap().drain().collect();
    for (inode, lock_id) in to_unlock {
        unlock_inode(inode, lock_id, &raft, &remote_rafts)
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
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
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

    let inode = lookup(parent, name.clone(), context, &raft, &remote_rafts).await?;
    let inode_lock_id = lock_inode(inode, &raft, &remote_rafts).await?;
    lock_guard.lock().unwrap().insert((inode, inode_lock_id));

    let existing_dest_inode =
        match lookup(new_parent, new_name.clone(), context, &raft, &remote_rafts).await {
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
            &new_name,
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

        let response_data = propose(new_parent, create_link, &raft, &remote_rafts).await?;
        response_or_error(&response_data)?;
    }

    // TODO: this shouldn't be able to fail since we already performed access checks
    remove_link(
        parent,
        &name,
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
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let mut inode = lookup(parent, name.clone(), context, &raft, &remote_rafts).await?;
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
                    &name,
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

    let mut decrement_request_builder = FlatBufferBuilder::new();
    let decrement_link_count =
        decrement_inode_request(inode, 2, None, &mut decrement_request_builder);
    // TODO: if this fails the inode will leak ;(
    let response_data = propose(inode, decrement_link_count, &raft, &remote_rafts).await?;
    response_or_error(&response_data)?;

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
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    // Try to remove the link. The result of this might be indeterminate, since "sticky bit"
    // can require that we know the uid of the inode
    let (link_inode, complete) =
        remove_link(parent, &name, None, None, context, &raft, &remote_rafts).await?;

    if complete {
        let mut decrement_request_builder = FlatBufferBuilder::new();
        let decrement_link_count =
            decrement_inode_request(link_inode, 1, None, &mut decrement_request_builder);
        // TODO: if this fails the inode will leak ;(
        let response_data = propose(link_inode, decrement_link_count, &raft, &remote_rafts).await?;
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
                        &name,
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
        let mut decrement_request_builder = FlatBufferBuilder::new();
        let decrement_link_count =
            decrement_inode_request(inode, 1, None, &mut decrement_request_builder);
        // TODO: if this fails the inode will leak ;(
        let response_data = propose(inode, decrement_link_count, &raft, &remote_rafts).await?;
        response_or_error(&response_data)?;
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
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<FlatBufferWithResponse<'static>, ErrorCode> {
    let mut create_request_builder = FlatBufferBuilder::new();
    // First create inode. This effectively begins the transaction.
    let create_inode = create_inode_request(
        Some(parent),
        uid,
        gid,
        mode,
        kind,
        remote_rafts.get_total_raft_groups(),
        &mut create_request_builder,
    );

    // This will be the response back to the client
    let create_response_data = remote_rafts
        .propose_to_specific_group(
            create_inode
                .request_as_create_inode_request()
                .unwrap()
                .raft_group(),
            &create_inode,
        )
        .await
        .map_err(|_| ErrorCode::Uncategorized)?;
    let response = response_or_error(create_response_data.bytes())?;
    assert_eq!(response.response_type(), ResponseType::RkyvResponse);
    let rkyv_data = response.response_as_rkyv_response().unwrap().rkyv_data();
    let mut rkyv_aligned = AlignedVec::with_capacity(rkyv_data.len());
    rkyv_aligned.extend_from_slice(rkyv_data);
    let created_inode_response = rkyv::check_archived_root::<RkyvGenericResponse>(&rkyv_aligned)
        .unwrap()
        .as_attr_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap();

    let inode = created_inode_response.inode;
    let link_count = created_inode_response.hard_links;

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

    match propose(parent, create_link, &raft, &remote_rafts).await {
        Ok(_) => {
            // This is the response back to the client
            return Ok(FlatBufferWithResponse::with_separate_response(
                builder,
                create_response_data,
            ));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback =
                decrement_inode_request(inode, link_count, None, &mut internal_request_builder);
            // TODO: if this fails the inode will leak ;(
            let response_data = propose(inode, rollback, &raft, &remote_rafts).await?;
            response_or_error(&response_data)?;
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
    remote_rafts: Arc<RemoteRaftGroups>,
) -> Result<FlatBufferWithResponse<'b>, ErrorCode> {
    let mut internal_request_builder = FlatBufferBuilder::new();

    // First increment the link count on the inode to ensure it can't be deleted
    // this effectively begins the transaction.
    let increment =
        hardlink_increment_request(hardlink_request.inode(), &mut internal_request_builder);

    let response_data = propose(hardlink_request.inode(), increment, &raft, &remote_rafts).await?;
    let response = response_or_error(&response_data)?;
    assert_eq!(response.response_type(), ResponseType::RkyvResponse);
    let rkyv_data = response
        .response_as_rkyv_response()
        .ok_or(ErrorCode::BadResponse)
        .unwrap()
        .rkyv_data();
    let rkyv_response = rkyv::check_archived_root::<RkyvGenericResponse>(rkyv_data).unwrap();
    let (rollback, attrs) = match rkyv_response {
        ArchivedRkyvGenericResponse::HardlinkTransaction {
            rollback_last_modified,
            attrs,
        } => (rollback_last_modified, attrs),
        _ => return Err(ErrorCode::BadResponse),
    };
    let inode_kind = u8_to_file_kind(attrs.kind);

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

    match propose(
        hardlink_request.new_parent(),
        create_link,
        &raft,
        &remote_rafts,
    )
    .await
    {
        Ok(response_data) => {
            if response_or_error(&response_data).is_err() {
                // Rollback the transaction
                let mut internal_request_builder = FlatBufferBuilder::new();
                let rollback = hardlink_rollback_request(
                    hardlink_request.inode(),
                    rollback.into(),
                    &mut internal_request_builder,
                );
                // TODO: if this fails the filesystem is corrupted ;( since the link count
                // may not have been decremented
                let response_data =
                    propose(hardlink_request.inode(), rollback, &raft, &remote_rafts).await?;
                response_or_error(&response_data)?;
            }

            // This is the response back to the client
            let entry: EntryMetadata = attrs.deserialize(&mut rkyv::Infallible).unwrap();

            let rkyv_bytes =
                rkyv::to_bytes::<_, 64>(&RkyvGenericResponse::EntryMetadata(entry)).unwrap();
            let flatbuffer_offset = builder.create_vector_direct(&rkyv_bytes);
            let mut response_builder = RkyvResponseBuilder::new(&mut builder);
            response_builder.add_rkyv_data(flatbuffer_offset);
            let offset = response_builder.finish().as_union_value();
            finalize_response(&mut builder, ResponseType::RkyvResponse, offset);
            return Ok(FlatBufferWithResponse::new(builder));
        }
        Err(error_code) => {
            // Rollback the transaction
            let mut internal_request_builder = FlatBufferBuilder::new();
            let rollback = hardlink_rollback_request(
                hardlink_request.inode(),
                rollback.into(),
                &mut internal_request_builder,
            );
            // TODO: if this fails the filesystem is corrupted ;( since the link count
            // may not have been decremented
            let response_data =
                propose(hardlink_request.inode(), rollback, &raft, &remote_rafts).await?;
            response_or_error(&response_data)?;
            return Err(error_code);
        }
    }
}
