use std::cmp::max;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::base::check_access;
use crate::base::{ErrorCode, FileKind, Timestamp, UserContext};
use crate::storage::local::data_storage::BLOCK_SIZE;
use fuser::FUSE_ROOT_ID;
use redb::{Durability, ReadOnlyTable, ReadableTable, TableDefinition, TypeName, Value};
use std::time::SystemTime;

pub const ROOT_INODE: u64 = FUSE_ROOT_ID;
pub const MAX_NAME_LENGTH: u32 = 255;
pub const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

type Inode = u64;

// Stores mapping of directory inodes to their parent
const PARENTS_TABLE: TableDefinition<u64, u64> = TableDefinition::new("parents");

// Directory contents. This is considered metadata rather than data,
// because it's replicated to all nodes to allow better searching.
// Maybe we should revisit that design?
// Maps (directory inodes, entry name) to -> inode
const DIRECTORY_TABLE: TableDefinition<(Inode, &str), (Inode, FileKind)> =
    TableDefinition::new("directory");

// Maps the inode & xattr key to an xattr value
const XATTR_TABLE: TableDefinition<(Inode, &str), &[u8]> = TableDefinition::new("xattrs");

// Maps the inode & xattr key to an xattr value
const ATTR_TABLE: TableDefinition<Inode, InodeAttributes> = TableDefinition::new("attrs");

#[derive(Clone, Debug)]
pub struct InodeAttributes {
    pub inode: Inode,
    pub size: u64,
    pub last_accessed: Timestamp,
    pub last_modified: Timestamp,
    pub last_metadata_changed: Timestamp,
    pub kind: FileKind,
    // Permissions and special mode bits
    pub mode: u16,
    pub hardlinks: u32,
    pub uid: u32,
    pub gid: u32,
}

impl InodeAttributes {
    pub fn blocks(&self) -> u64 {
        // TODO: seems like this should be rounded up? Is that a bug?
        self.size / BLOCK_SIZE
    }
}

type InodeTuple = (
    Inode,
    u64,
    Timestamp,
    Timestamp,
    Timestamp,
    FileKind,
    u16,
    u32,
    u32,
    u32,
);

impl Value for InodeAttributes {
    type SelfType<'a>
        = InodeAttributes
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        InodeTuple::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let tmp = InodeTuple::from_bytes(data);
        Self {
            inode: tmp.0,
            size: tmp.1,
            last_accessed: tmp.2,
            last_modified: tmp.3,
            last_metadata_changed: tmp.4,
            kind: tmp.5,
            mode: tmp.6,
            hardlinks: tmp.7,
            uid: tmp.8,
            gid: tmp.9,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let tmp = (
            value.inode,
            value.size,
            value.last_accessed,
            value.last_modified,
            value.last_metadata_changed,
            value.kind,
            value.mode,
            value.hardlinks,
            value.uid,
            value.gid,
        );
        InodeTuple::as_bytes(&tmp)
    }

    fn type_name() -> TypeName {
        TypeName::new("fleetfs::InodeAttributes")
    }
}

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
enum XattrNamespace {
    SECURITY,
    SYSTEM,
    TRUSTED,
    USER,
}

fn parse_xattr_namespace(key: &str) -> Result<XattrNamespace, ErrorCode> {
    if let Some(namespace) = key.split('.').next() {
        match namespace {
            "security" => Ok(XattrNamespace::SECURITY),
            "system" => Ok(XattrNamespace::SYSTEM),
            "trusted" => Ok(XattrNamespace::TRUSTED),
            "user" => Ok(XattrNamespace::USER),
            _ => Err(ErrorCode::InvalidXattrNamespace),
        }
    } else {
        Err(ErrorCode::InvalidXattrNamespace)
    }
}

fn xattr_access_check(
    key: &str,
    access_mask: i32,
    inode_attrs: &InodeAttributes,
    context: &UserContext,
) -> Result<(), ErrorCode> {
    match parse_xattr_namespace(key)? {
        XattrNamespace::SECURITY => {
            if access_mask != libc::R_OK && context.uid() != 0 {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        XattrNamespace::TRUSTED => {
            if context.uid() != 0 {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        XattrNamespace::SYSTEM => match key {
            "system.posix_acl_access" => {
                if !check_access(
                    inode_attrs.uid,
                    inode_attrs.gid,
                    inode_attrs.mode,
                    context.uid(),
                    context.gid(),
                    access_mask,
                ) {
                    return Err(ErrorCode::OperationNotPermitted);
                }
            }
            _ => {
                if context.uid() != 0 {
                    return Err(ErrorCode::OperationNotPermitted);
                }
            }
        },
        XattrNamespace::USER => {
            if !check_access(
                inode_attrs.uid,
                inode_attrs.gid,
                inode_attrs.mode,
                context.uid(),
                context.gid(),
                access_mask,
            ) {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
    }

    Ok(())
}

// TODO: add persistence
// When acquiring locks on multiple fields, they must be in alphabetical order
pub struct MetadataStorage {
    storage: Mutex<redb::Database>,
    // TODO: remove this once redb is stable & reliable
    parent_verification: Mutex<HashMap<Inode, Inode>>,
    // Raft guarantees that operations are performed in the same order across all nodes
    // which means that all nodes have the same value for this counter
    next_inode: AtomicU64,
    durability_counter: AtomicU64,
    num_raft_groups: u64,
}

impl MetadataStorage {
    #[allow(clippy::new_without_default)]
    pub fn new(raft_group: u16, num_raft_groups: u16, metadata_dir: &Path) -> MetadataStorage {
        let db = redb::Database::create(metadata_dir.join("metadata.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(PARENTS_TABLE).unwrap();
            table.insert(&ROOT_INODE, &ROOT_INODE).unwrap();
            txn.open_table(DIRECTORY_TABLE).unwrap();
            txn.open_table(XATTR_TABLE).unwrap();
            let mut table = txn.open_table(ATTR_TABLE).unwrap();
            let attrs = InodeAttributes {
                inode: ROOT_INODE,
                size: 0,
                last_accessed: now(),
                last_modified: now(),
                last_metadata_changed: now(),
                kind: FileKind::Directory,
                mode: 0o777,
                hardlinks: 2,
                uid: 0,
                gid: 0,
            };
            table.insert(&ROOT_INODE, attrs).unwrap();
        }
        txn.commit().unwrap();

        let mut parents = HashMap::new();
        parents.insert(ROOT_INODE, ROOT_INODE);

        // Each raft group is responsible for inodes modulo num_raft_groups
        let mut start_inode = ROOT_INODE + 1;
        while start_inode % num_raft_groups as u64 != raft_group as u64 {
            start_inode += 1;
        }

        MetadataStorage {
            storage: Mutex::new(db),
            parent_verification: Mutex::new(parents),
            next_inode: AtomicU64::new(start_inode),
            durability_counter: AtomicU64::new(0),
            num_raft_groups: num_raft_groups as u64,
        }
    }

    fn durability_tick(&self) -> bool {
        let count = self.durability_counter.fetch_add(1, Ordering::AcqRel);
        count % 100 == 0
    }

    pub(super) fn non_directory_inodes(&self) -> Result<Vec<u64>, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(ATTR_TABLE).unwrap();
        let mut result = vec![];
        for item in table.iter().unwrap() {
            let (inode, attrs) = item.unwrap();
            if attrs.value().kind != FileKind::Directory {
                result.push(inode.value());
            }
        }

        Ok(result)
    }

    pub fn lookup(
        &self,
        parent: Inode,
        name: &str,
        context: UserContext,
    ) -> Result<Option<Inode>, ErrorCode> {
        if name.len() > MAX_NAME_LENGTH as usize {
            return Err(ErrorCode::NameTooLong);
        }

        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let parent_attrs = attr_table
            .get(&parent)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::X_OK,
        ) {
            return Err(ErrorCode::AccessDenied);
        }

        let table = txn.open_table(DIRECTORY_TABLE).unwrap();
        let maybe_inode = table.get((parent, name)).unwrap().map(|x| x.value().0);
        Ok(maybe_inode)
    }

    pub fn get_xattr(
        &self,
        inode: Inode,
        key: &str,
        context: UserContext,
    ) -> Result<Vec<u8>, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(ATTR_TABLE).unwrap();
        let inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?;
        xattr_access_check(key, libc::R_OK, &inode_attrs.value(), &context)?;

        let table = txn.open_table(XATTR_TABLE).unwrap();
        table
            .get((inode, key))
            .unwrap()
            .map(|x| x.value().to_vec())
            .ok_or(ErrorCode::MissingXattrKey)
    }

    pub fn list_xattrs(&self, inode: Inode) -> Result<Vec<String>, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(XATTR_TABLE).unwrap();
        let mut keys = vec![];
        for item in table.range((inode, "")..(inode + 1, "")).unwrap() {
            let (key, _) = item.unwrap();
            keys.push(key.value().1.to_owned());
        }
        Ok(keys)
    }

    pub fn set_xattr(
        &self,
        inode: Inode,
        key: &str,
        value: &[u8],
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
            let mut inode_attrs = attr_table
                .get(&inode)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
            xattr_access_check(key, libc::W_OK, &inode_attrs, &context)?;
            {
                let mut table = txn.open_table(XATTR_TABLE).unwrap();
                table.insert((inode, key), value).unwrap();
            }
            inode_attrs.last_metadata_changed = now();
            attr_table.insert(&inode, inode_attrs).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    pub fn remove_xattr(
        &self,
        inode: Inode,
        key: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
            let mut inode_attrs = attr_table
                .get(&inode)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
            xattr_access_check(key, libc::W_OK, &inode_attrs, &context)?;
            {
                let mut table = txn.open_table(XATTR_TABLE).unwrap();
                if table.remove((inode, key)).unwrap().is_none() {
                    // No need to commit the transaction, since nothing was modified
                    return Err(ErrorCode::MissingXattrKey);
                }
            }
            inode_attrs.last_metadata_changed = now();
            attr_table.insert(&inode, inode_attrs).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    pub fn readdir(&self, inode: Inode) -> Result<Vec<(Inode, String, FileKind)>, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let attr_table = txn.open_table(ATTR_TABLE).unwrap();
        if attr_table.get(&inode).unwrap().is_none() {
            return Err(ErrorCode::DoesNotExist);
        }
        let vdb = self
            .parent_verification
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;

        let directory_table = txn.open_table(DIRECTORY_TABLE).unwrap();
        let table: ReadOnlyTable<Inode, Inode> = txn.open_table(PARENTS_TABLE).unwrap();
        let parent_inode = if let Some(vparent_inode) = vdb.get(&inode) {
            let parent_inode = table.get(&inode).unwrap().unwrap().value();
            assert_eq!(*vparent_inode, parent_inode);
            parent_inode
        } else {
            assert!(table.get(&inode).unwrap().is_none());
            return Err(ErrorCode::InodeDoesNotExist);
        };

        let mut result: Vec<(Inode, String, FileKind)> = vec![];
        for r in directory_table.range((inode, "")..(inode + 1, "")).unwrap() {
            let (key, value) = r.unwrap();
            result.push((value.value().0, key.value().1.to_string(), value.value().1));
        }
        // TODO: kind of a hack
        result.insert(0, (parent_inode, "..".to_string(), FileKind::Directory));
        result.insert(0, (inode, ".".to_string(), FileKind::Directory));
        Ok(result)
    }

    pub fn utimens(
        &self,
        inode: Inode,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut table = txn.open_table(ATTR_TABLE).unwrap();

        let mut inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        // Non-owners are only allowed to change atime & mtime to current:
        // http://man7.org/linux/man-pages/man2/utimensat.2.html
        if inode_attrs.uid != context.uid()
            && context.uid() != 0
            && (atime.is_none_or(|x| x.nanos != libc::UTIME_NOW as i32)
                || mtime.is_none_or(|x| x.nanos != libc::UTIME_NOW as i32))
        {
            return Err(ErrorCode::OperationNotPermitted);
        }

        if inode_attrs.uid != context.uid()
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

        if let Some(atime) = atime {
            if atime.nanos == libc::UTIME_NOW as i32 {
                // TODO: this should be set during proposal. Currently each node set its own timestamp
                inode_attrs.last_accessed = now();
            } else {
                inode_attrs.last_accessed = atime;
            }
        }
        if let Some(mtime) = mtime {
            if mtime.nanos == libc::UTIME_NOW as i32 {
                // TODO: this should be set during proposal. Currently each node set its own timestamp
                inode_attrs.last_modified = now();
            } else {
                inode_attrs.last_modified = mtime;
            }
        }
        table.insert(&inode, inode_attrs).unwrap();
        drop(table);
        txn.commit().unwrap();

        Ok(())
    }

    // t_mode type is u16 on MacOS, but u32 on Linux
    #[allow(clippy::unnecessary_cast)]
    pub fn chmod(
        &self,
        inode: Inode,
        mut mode: u32,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut table = txn.open_table(ATTR_TABLE).unwrap();
        let mut inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        if context.uid() != 0 && inode_attrs.uid != context.uid() {
            return Err(ErrorCode::OperationNotPermitted);
        }

        // TODO: suid/sgid not supported
        mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        inode_attrs.mode = mode as u16;
        inode_attrs.last_metadata_changed = now();
        table.insert(&inode, inode_attrs).unwrap();
        drop(table);
        txn.commit().unwrap();

        Ok(())
    }

    pub fn chown(
        &self,
        inode: Inode,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut table = txn.open_table(ATTR_TABLE).unwrap();
        let mut inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();

        // Only root can change uid
        if let Some(uid) = uid {
            if context.uid() != 0
                // but no-op changes by the owner are not an error
                && !(uid == inode_attrs.uid && context.uid() == inode_attrs.uid)
            {
                return Err(ErrorCode::OperationNotPermitted);
            }
        }
        // Only owner may change the group
        if gid.is_some() && context.uid() != 0 && context.uid() != inode_attrs.uid {
            return Err(ErrorCode::OperationNotPermitted);
        }

        if let Some(uid) = uid {
            inode_attrs.uid = uid;
        }
        if let Some(gid) = gid {
            inode_attrs.gid = gid;
        }
        if uid.is_some() || gid.is_some() {
            inode_attrs.last_metadata_changed = now();
        }
        table.insert(&inode, inode_attrs).unwrap();
        drop(table);
        txn.commit().unwrap();

        Ok(())
    }

    pub fn hardlink_stage0_link_increment(&self, inode: Inode) -> Result<Timestamp, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut table = txn.open_table(ATTR_TABLE).unwrap();
        let mut inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        inode_attrs.hardlinks += 1;
        let old = inode_attrs.last_metadata_changed;
        inode_attrs.last_metadata_changed = now();
        table.insert(&inode, inode_attrs).unwrap();
        drop(table);
        txn.commit().unwrap();

        Ok(old)
    }

    pub fn create_link(
        &self,
        inode: Inode,
        parent: u64,
        name: &str,
        context: UserContext,
        inode_kind: FileKind,
    ) -> Result<(), ErrorCode> {
        if self.lookup(parent, name, context)?.is_some() {
            return Err(ErrorCode::AlreadyExists);
        }

        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
            let mut parent_attrs = attr_table
                .get(&parent)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
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
            parent_attrs.last_modified = now();
            parent_attrs.last_metadata_changed = now();
            attr_table.insert(&parent, parent_attrs).unwrap();

            let mut table = txn.open_table(DIRECTORY_TABLE).unwrap();
            table.insert((parent, name), (inode, inode_kind)).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    pub fn replace_link(
        &self,
        parent: u64,
        name: &str,
        new_inode: Inode,
        inode_kind: FileKind,
        context: UserContext,
    ) -> Result<u64, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let mut parent_attrs = attr_table
            .get(&parent)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
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
        parent_attrs.last_modified = now();
        parent_attrs.last_metadata_changed = now();
        attr_table.insert(&parent, parent_attrs).unwrap();

        let old_inode = {
            let mut table = txn.open_table(DIRECTORY_TABLE).unwrap();
            let (old_inode, _) = table
                .insert((parent, name), (new_inode, inode_kind))
                .unwrap()
                .unwrap()
                .value();
            old_inode
        };
        drop(attr_table);
        txn.commit().unwrap();

        Ok(old_inode)
    }

    pub fn hardlink_rollback(
        &self,
        inode: Inode,
        last_metadata_changed: Timestamp,
    ) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_table(ATTR_TABLE).unwrap();
            let mut inode_attrs = table
                .get(&inode)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
            inode_attrs.hardlinks -= 1;
            inode_attrs.last_metadata_changed = last_metadata_changed;
            table.insert(&inode, inode_attrs).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    pub fn update_parent(&self, inode: u64, new_parent: u64) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let mut vdb = self
            .parent_verification
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;
        assert_eq!(
            attr_table.get(&inode).unwrap().unwrap().value().kind,
            FileKind::Directory
        );
        drop(attr_table);
        {
            let mut table = txn.open_table(PARENTS_TABLE).unwrap();
            table.insert(&inode, &new_parent).unwrap();
        }
        txn.commit().unwrap();
        vdb.insert(inode, new_parent);

        Ok(())
    }

    pub fn update_metadata_changed_time(&self, inode: u64) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
            let mut inode_attrs = attr_table
                .get(&inode)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
            inode_attrs.last_metadata_changed = now();
            attr_table.insert(&inode, inode_attrs).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    pub fn truncate(
        &self,
        inode: Inode,
        new_length: u64,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        if new_length > MAX_FILE_SIZE {
            return Err(ErrorCode::FileTooLarge);
        }

        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_table(ATTR_TABLE).unwrap();
            let mut inode_attrs = table
                .get(&inode)
                .unwrap()
                .ok_or(ErrorCode::InodeDoesNotExist)?
                .value();
            if !check_access(
                inode_attrs.uid,
                inode_attrs.gid,
                inode_attrs.mode,
                context.uid(),
                context.gid(),
                libc::W_OK,
            ) {
                return Err(ErrorCode::AccessDenied);
            }

            inode_attrs.size = new_length;
            inode_attrs.last_metadata_changed = now();
            inode_attrs.last_modified = now();
            table.insert(&inode, inode_attrs).unwrap();
        }
        txn.commit().unwrap();

        Ok(())
    }

    // Remove a directory entry.
    // Returns the link inode, and a boolean indicating if processing is complete. If it is false
    // then it could not be determined if the link can be removed due to "stick bit".
    // In this case the inode's uid should be retrieved and this method retried, passing the
    // link_inode_and_uid argument.
    pub fn remove_link(
        &self,
        parent: u64,
        name: &str,
        // If provided, will preform "sticky bit" checks for the inode.
        link_inode_and_uid: Option<(u64, u32)>,
        context: UserContext,
    ) -> Result<(Inode, bool), ErrorCode> {
        let db = self.storage.lock().unwrap();
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let mut parent_attrs = attr_table
            .get(&parent)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        let mut dir_table = txn.open_table(DIRECTORY_TABLE).unwrap();
        let (inode, _) = dir_table
            .get((parent, name))
            .unwrap()
            .ok_or(ErrorCode::DoesNotExist)?
            .value();

        if let Some((retrieved_inode, _)) = link_inode_and_uid {
            if retrieved_inode != inode {
                // The inode that the client looked up is out of date (i.e. this link has been
                // deleted and recreated since then). Tell the client to look it up again.
                drop(attr_table);
                drop(dir_table);
                txn.abort().unwrap();
                return Ok((inode, false));
            }
        }

        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            context.uid(),
            context.gid(),
            libc::W_OK,
        ) {
            drop(attr_table);
            drop(dir_table);
            txn.abort().unwrap();
            return Err(ErrorCode::AccessDenied);
        }

        let uid = context.uid();
        // "Sticky bit" handling
        if parent_attrs.mode & libc::S_ISVTX as u16 != 0 && uid != parent_attrs.uid && uid != 0 {
            if let Some((_, inode_uid)) = link_inode_and_uid {
                if uid != inode_uid {
                    drop(attr_table);
                    drop(dir_table);
                    txn.abort().unwrap();
                    return Err(ErrorCode::AccessDenied);
                }
            } else {
                // Sticky bit is on, and we need to check the inode's uid. Tell the client to lock
                // it and look up the uid.
                drop(attr_table);
                drop(dir_table);
                txn.abort().unwrap();
                return Ok((inode, false));
            }
        }

        parent_attrs.last_metadata_changed = now();
        parent_attrs.last_modified = now();
        attr_table.insert(&parent, parent_attrs).unwrap();
        drop(attr_table);
        let (inode, _) = dir_table
            .remove((parent, name))
            .unwrap()
            .ok_or(ErrorCode::DoesNotExist)?
            .value();
        drop(dir_table);
        txn.commit().unwrap();

        Ok((inode, true))
    }

    pub fn write(&self, inode: Inode, offset: u64, length: u32) -> Result<(), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut table = txn.open_table(ATTR_TABLE).unwrap();
        let mut inode_attrs = table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();

        let current_length = inode_attrs.size;
        inode_attrs.size = max(current_length, u64::from(length) + offset);
        inode_attrs.last_metadata_changed = now();
        inode_attrs.last_modified = now();
        table.insert(&inode, inode_attrs).unwrap();
        drop(table);
        txn.commit().unwrap();

        Ok(())
    }

    pub fn create_inode(
        &self,
        parent: Inode,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<(Inode, InodeAttributes), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let mut vdb = self
            .parent_verification
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;

        let inode = self.allocate_inode();
        let size = if kind == FileKind::Directory {
            BLOCK_SIZE
        } else {
            0
        };
        // Directories start with a link count of 2, since they have a self link
        let hardlinks = if kind == FileKind::Directory { 2 } else { 1 };
        let inode_metadata = InodeAttributes {
            inode,
            size,
            last_accessed: now(),
            last_modified: now(),
            last_metadata_changed: now(),
            kind,
            // TODO: suid/sgid not supported
            mode: mode & !(libc::S_ISUID | libc::S_ISGID) as u16,
            hardlinks,
            uid,
            gid,
        };
        attr_table.insert(&inode, &inode_metadata).unwrap();

        if kind == FileKind::Directory {
            {
                let mut table = txn.open_table(PARENTS_TABLE).unwrap();
                table.insert(&inode, &parent).unwrap();
            }
            vdb.insert(inode, parent);
        }
        drop(attr_table);
        txn.commit().unwrap();
        Ok((inode, inode_metadata))
    }

    // Returns an inode, if that inode's data should be deleted
    pub fn decrement_inode_link_count(
        &self,
        inode: Inode,
        count: u32,
    ) -> Result<Option<Inode>, ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let mut txn = db.begin_write().unwrap();
        if !self.durability_tick() {
            txn.set_durability(Durability::None);
        }
        let mut attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let mut vdb = self
            .parent_verification
            .lock()
            .map_err(|_| ErrorCode::Corrupted)?;

        let mut inode_attrs = attr_table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        inode_attrs.hardlinks -= count;
        inode_attrs.last_metadata_changed = now();
        let deleted_inode = if inode_attrs.hardlinks == 0 {
            let is_directory = inode_attrs.kind == FileKind::Directory;
            attr_table.remove(&inode).unwrap();
            {
                let mut table = txn.open_table(XATTR_TABLE).unwrap();
                table
                    .retain_in((inode, "")..(inode + 1, ""), |_, _| false)
                    .unwrap();
            }
            if is_directory {
                {
                    let mut table = txn.open_table(PARENTS_TABLE).unwrap();
                    table.remove(&inode).unwrap();
                }
                vdb.remove(&inode).unwrap();

                {
                    let directory_table = txn.open_table(DIRECTORY_TABLE).unwrap();
                    let mut entries = directory_table.range((inode, "")..(inode + 1, "")).unwrap();
                    assert!(
                        entries.next().is_none(),
                        "Deleted a non-empty directory inode"
                    );
                }
                None
            } else {
                // Only delete file contents if this is not a directory (directories don't have data contents)
                Some(inode)
            }
        } else {
            attr_table.insert(&inode, &inode_attrs).unwrap();
            None
        };
        drop(attr_table);
        txn.commit().unwrap();

        Ok(deleted_inode)
    }

    pub fn get_attributes(
        &self,
        inode: Inode,
    ) -> Result<(InodeAttributes, Option<u32>), ErrorCode> {
        let db = self.storage.lock().map_err(|_| ErrorCode::Corrupted)?;
        let txn = db.begin_read().unwrap();
        let attr_table = txn.open_table(ATTR_TABLE).unwrap();
        let attributes = attr_table
            .get(&inode)
            .unwrap()
            .ok_or(ErrorCode::InodeDoesNotExist)?
            .value();
        let table = txn.open_table(DIRECTORY_TABLE).unwrap();
        let directory_size = if attributes.kind == FileKind::Directory {
            Some(table.range((inode, "")..(inode + 1, "")).unwrap().count() as u32)
        } else {
            None
        };

        Ok((attributes, directory_size))
    }

    fn allocate_inode(&self) -> u64 {
        self.next_inode
            .fetch_add(self.num_raft_groups, Ordering::SeqCst)
    }
}

fn now() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time before unix epoch");
    Timestamp::new(now.as_secs() as i64, now.subsec_nanos() as i32)
}
